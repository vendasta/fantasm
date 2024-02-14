from setuptools import setup, find_packages

setup(
    name="fantasm",
    version="2.0.0",
    packages=find_packages(where="src"),
    url="http://github.com/vendasta/fantasm",
    install_requires=[
        "appengine-python-standard==1.1.5",
        "PyYAML==6.0.1",
    ],
)