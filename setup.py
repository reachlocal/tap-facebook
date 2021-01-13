#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-facebook",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["facebook"],
    install_requires=[
        "singer-python",
        "requests",
        "futures3==1.0.0"
    ],
    entry_points="""
    [console_scripts]
    tap-facebook=tap_facebook:main
    """,
    packages=["tap_facebook"],
    package_data = {
        "schemas": ["tap_facebook/schemas/*.json"]
    },
    include_package_data=True,
)
