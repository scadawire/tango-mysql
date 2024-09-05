import setuptools

setuptools.setup(
    name="SQLServer",
    version="0.1.0",
    author="Sebastian Jennen",
    author_email="sj@imagearts.de",
    description="SQLServer device driver",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    install_requires=['requests'],
    scripts=['SQLServer.py']
)