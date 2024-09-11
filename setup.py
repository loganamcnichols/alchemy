from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="alchemy",
    version="0.1.0",
    author="Logan McNichols",
    author_email="loganamcnichols@gmail.com",
    description="A python package for working with Alchemer survey data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/loganamcnichols/alchemy",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=[
      "pandas",
    ],
)