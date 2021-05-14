from setuptools import setup

import os


version = "0.0.1"

long_description = "\n\n".join([open("README.rst").read(), open("CHANGES.rst").read()])

install_requires = [
    "pydantic",
    "pymongo",
    "pystalkd",
]

# emulate "--no-deps" on the readthedocs build (there is no way to specify this
# behaviour in the .readthedocs.yml)
if os.environ.get("READTHEDOCS") == "True":
    install_requires = []


tests_require = [
    "requests",
    "pytest",
    "mock",
    "pydantic",
    "pymongo",
    "pystalkd",
]

setup(
    name="Arend",
    version=version,
    description="A ",
    long_description=long_description,
    # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    keywords=["arend", "producer-consumer", "process-queue"],
    author="Jose Maria Vazquez Jimenez",
    author_email="josevazjim88@gmail.com",
    url="",
    license="MIT License",
    packages=["arend"],
    include_package_data=True,
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    python_requires=">=3.6",
    extras_require={"test": tests_require},
    entry_points={"console_scripts": []},
)
