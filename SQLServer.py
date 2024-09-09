import time
from tango import AttrQuality, AttrWriteType, DispLevel, DevState, Attr, CmdArgType, UserDefaultAttrProp
from tango.server import Device, attribute, command, DeviceMeta
from tango.server import class_property, device_property
from tango.server import run
import os
import json
from json import JSONDecodeError
import sys,time,datetime,traceback,os
import pymysql

class Mysql(Device, metaclass=DeviceMeta):

    host = device_property(dtype=str, default_value="127.0.0.1")
    port = device_property(dtype=int, default_value=3306)
    username = device_property(dtype=str, default_value="")
    password = device_property(dtype=str, default_value="")
    database = device_property(dtype=str, default_value="")
    init_dynamic_attributes = device_property(dtype=str, default_value="")
    dynamicAttributes = {}
    dynamicAttributeValueTypes = {}
    dynamicAttributeSqlLookup = {}
    last_connect = 0
    connection = 0
    cursor = 0
    
    def connect(self,rethrow=False):
        print("In %s.connect()"%self.get_name())
        self.last_connect = time.time()
        try:
            self.connection.close()
            print('Connection to %s.%s closed'%(self.host,self.database))
        except: pass

        try:
            self.connection = pymysql.connect(
                database=self.database,
                host=self.host,
                user=self.username,
                password=self.password,
                port=self.port,
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor)
            self.cursor = self.connection.cursor()
            print('%s connected to %s@%s'%(self.get_name(),self.database,self.host))
            return True

        except Exception as e:
            print('Error in %s.connect()'%self.get_name())
            print(traceback.format_exc())
            self.last_error = str(e)
            self.connection = self.cursor = None
            if rethrow: raise e
            return False

    def init_device(self):
        print("In ", self.get_name(), "::init_device()")
        self.set_state(DevState.INIT)

        #Reloading properties
        self.get_device_properties(self.get_device_class())
        self.last_connect,self.last_update,self.last_error = 0,0,''
        self.connect()
        if self.init_dynamic_attributes != "":
            try:
                attributes = json.loads(self.init_dynamic_attributes)
                for attributeData in attributes:
                    self.add_dynamic_attribute(attributeData["name"], 
                        attributeData.get("data_type", ""), attributeData.get("min_value", ""), attributeData.get("max_value", ""),
                        attributeData.get("unit", ""), attributeData.get("write_type", ""), attributeData.get("label", ""),
                        attributeData.get("modifier", ""))
            except JSONDecodeError as e:
                attributes = self.init_dynamic_attributes.split(",")
                for attribute in attributes:
                    self.info_stream("Init dynamic attribute: " + str(attribute.strip()))
                    self.add_dynamic_attribute(attribute.strip())
    
    @command(dtype_in=str)
    def add_dynamic_attribute(self, topic, 
            variable_type_name="DevString", min_value="", max_value="",
            unit="", write_type_name="", label="", modifier=""):
        if topic == "": return
        prop = UserDefaultAttrProp()
        variableType = self.stringValueToVarType(variable_type_name)
        writeType = self.stringValueToWriteType(write_type_name)
        self.dynamicAttributeValueTypes[topic] = variableType
        self.dynamicAttributeSqlLookup[topic] = modifier
        if(min_value != "" and min_value != max_value): 
            prop.set_min_value(min_value)
        if(max_value != "" and min_value != max_value): 
            prop.set_max_value(max_value)
        if(unit != ""): 
            prop.set_unit(unit)
        if(label != ""):
            prop.set_label(label)
        attr = Attr(topic, variableType, writeType)
        attr.set_default_properties(prop)
        self.add_attribute(attr, r_meth=self.read_dynamic_attr, w_meth=self.write_dynamic_attr)
        self.dynamicAttributes[topic] = ""

    def stringValueToVarType(self, variable_type_name) -> CmdArgType:
        if(variable_type_name == "DevBoolean"):
            return CmdArgType.DevBoolean
        if(variable_type_name == "DevLong"):
            return CmdArgType.DevLong
        if(variable_type_name == "DevDouble"):
            return CmdArgType.DevDouble
        if(variable_type_name == "DevFloat"):
            return CmdArgType.DevFloat
        if(variable_type_name == "DevString"):
            return CmdArgType.DevString
        if(variable_type_name == ""):
            return CmdArgType.DevString
        raise Exception("given variable_type '" + variable_type + "' unsupported, supported are: DevBoolean, DevLong, DevDouble, DevFloat, DevString")

    def stringValueToWriteType(self, write_type_name) -> AttrWriteType:
        if(write_type_name == "READ"):
            return AttrWriteType.READ
        if(write_type_name == "WRITE"):
            return AttrWriteType.WRITE
        if(write_type_name == "READ_WRITE"):
            return AttrWriteType.READ_WRITE
        if(write_type_name == "READ_WITH_WRITE"):
            return AttrWriteType.READ_WITH_WRITE
        if(write_type_name == ""):
            return AttrWriteType.READ_WRITE
        raise Exception("given write_type '" + write_type_name + "' unsupported, supported are: READ, WRITE, READ_WRITE, READ_WITH_WRITE")

    def stringValueToTypeValue(self, name, val):
        if(self.dynamicAttributeValueTypes[name] == CmdArgType.DevBoolean):
            if(str(val).lower() == "false"):
                return False
            if(str(val).lower() == "true"):
                return True
            return bool(int(float(val)))
        if(self.dynamicAttributeValueTypes[name] == CmdArgType.DevLong):
            return int(float(val))
        if(self.dynamicAttributeValueTypes[name] == CmdArgType.DevDouble):
            return float(val)
        if(self.dynamicAttributeValueTypes[name] == CmdArgType.DevFloat):
            return float(val)
        return val

    def read_dynamic_attr(self, attr):
        name = attr.get_name()
        self.dynamicAttributes[name] = self.sqlRead(name)
        value = self.dynamicAttributes[name]
        self.debug_stream("read value " + str(name) + ": " + str(value))
        attr.set_value(self.stringValueToTypeValue(name, value))

    def write_dynamic_attr(self, attr):
        value = str(attr.get_write_value())
        name = attr.get_name()
        self.dynamicAttributes[name] = value
        self.sqlWrite(name, self.dynamicAttributes[name])
    
    def sqlRead(self, name):
        select = "SELECT `:COL:` as field FROM `:TABLE:` WHERE :WHERE: LIMIT 1;"
        parts = self.dynamicAttributeSqlLookup[name].split(",")
        update = update.replace(":TABLE:", parts[0])
        update = update.replace(":COL:", parts[1])
        update = update.replace(":WHERE:", parts[2])
        self.cursor.execute(select)
        result = self.cursor.fetchone()
        return result.field
        
    def sqlWrite(self, name, value):
        update = "UPDATE `:TABLE:` SET `:COL:` = :VALUE: WHERE :WHERE: LIMIT 1;"
        parts = self.dynamicAttributeSqlLookup[name].split(",")
        update = update.replace(":TABLE:", parts[0])
        update = update.replace(":COL:", parts[1])
        update = update.replace(":WHERE:", parts[2])
        update = update.replace(":VALUE:", "%s")
        self.cursor.execute(update, (value))

if __name__ == "__main__":
    deviceServerName = os.getenv("DEVICE_SERVER_NAME")
    run({deviceServerName: Mysql})
