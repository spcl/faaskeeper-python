
from setuptools import find_packages
from setuptools import setup

setup(
    name='faaskeeper-python',
    version='0.1.0-beta',
    install_requires=['boto3'],
    url='https://github.com/spcl/serverless-zookeeper-client',
    license='BSD-3',
    author='Marcin Copik',
    author_email='mcopik@gmail.com',
    description='FaaSKeeper Python client'
)

