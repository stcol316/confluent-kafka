# setup.py
from setuptools import setup, find_packages

setup(
    name="pydemo",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
)
