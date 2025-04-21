from setuptools import setup, find_packages

setup(
    name="PolarsDB",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "polars>=0.19.12",
    ],
    author="devincii-io",
    author_email="",
    description="A lightweight CSV-based database system built on Polars",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/devincii-io/PolarsDB",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
) 