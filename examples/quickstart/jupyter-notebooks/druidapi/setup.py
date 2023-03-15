from setuptools import setup, find_packages

setup(
    name='druidapi',
    version='0.1.0',
    description='Python API client for Apache Druid',
    url='https://github.com/apache/druid/tree/master/examples/quickstart/jupyter-notebooks/druidapi',
    author='Paul Rogers',
    author_email='dev@druid.apache.org',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=['requests'],

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ],
)
