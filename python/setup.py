import os

from setuptools import setup


try:
    with open(os.path.join(os.path.dirname(__file__), "README.md"), encoding="utf-8") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ""


setup(
    name="labours",
    description="Python companion for github.com/src-d/hercules to visualize the results.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="10.4.0",
    license="Apache-2.0",
    author="source{d}",
    author_email="machine-learning@sourced.tech",
    url="https://github.com/src-d/hercules",
    download_url="https://github.com/src-d/hercules",
    packages=["labours"],
    keywords=["git", "mloncode", "mining software repositories", "hercules"],
    install_requires=[
        "clint>=0.5.1,<1.0",
        "matplotlib>=2.0,<4.0",
        "numpy>=1.12.0,<2.0",
        "pandas>=0.20.0,<1.0",
        "PyYAML>=3.0,<5.0",
        "scipy>=0.19.0,<1.2.2",
        "protobuf>=3.5.0,<4.0",
        "munch>=2.0,<3.0",
        "hdbscan>=0.8.0,<2.0",
        "seriate>=1.0,<2.0",
        "fastdtw>=0.3.2,<2.0",
        "python-dateutil>=2.6.0,<3.0",
        "lifelines>=0.20.0,<2.0",
    ],
    package_data={"labours": ["../LICENSE.md", "../README.md", "../requirements.txt"]},
    entry_points={
        "console_scripts": ["labours=labours.__main__:main"],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Environment :: Console",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
)
