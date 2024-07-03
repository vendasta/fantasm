from setuptools import setup, find_packages

setup(
    name="fantasm",
    version="2.0.1",
    packages=find_packages(),
    url="http://github.com/vendasta/fantasm",
    install_requires=[
        "appengine-python-standard==1.1.5",
        "PyYAML==6.0.1",
    ],
    package_data={
        'fantasm': ['scrubber.yaml'],
    },
)