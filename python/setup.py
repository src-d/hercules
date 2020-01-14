import os

from setuptools import setup


try:
    with open(
        os.path.join(os.path.dirname(__file__), "README.md"), encoding="utf-8"
    ) as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ""

with open(
    os.path.join(os.path.dirname(__file__), "requirements.in"), encoding="utf-8"
) as f:
    requirements = f.readlines()


setup(
    name="labours",
    description="Python companion for github.com/src-d/hercules to visualize the results.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    version="10.7.2",
    license="Apache-2.0",
    author="source{d}",
    author_email="machine-learning@sourced.tech",
    url="https://github.com/src-d/hercules",
    download_url="https://github.com/src-d/hercules",
    packages=["labours", "labours._vendor", "labours.modes"],
    keywords=["git", "mloncode", "mining software repositories", "hercules"],
    install_requires=requirements,
    package_data={"labours": ["../LICENSE.md", "../README.md", "../requirements.txt"]},
    entry_points={"console_scripts": ["labours=labours.__main__:main"]},
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
