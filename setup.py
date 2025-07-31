#!/usr/bin/env python3
"""
Setup script for the Rucio Workflow package.

This script provides installation and development setup for the package.
"""

from setuptools import setup, find_packages
import os

# Read the README file
def read_readme():
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements
def read_requirements():
    with open("requirements.txt", "r", encoding="utf-8") as fh:
        return [line.strip() for line in fh if line.strip() and not line.startswith("#")]

# Get version
def get_version():
    """Get version from the package __init__.py file."""
    version = "1.0.0"
    init_file = os.path.join("rucio_workflow", "__init__.py")
    if os.path.exists(init_file):
        with open(init_file, "r") as f:
            for line in f:
                if line.startswith("__version__"):
                    version = line.split("=")[1].strip().strip('"').strip("'")
                    break
    return version

setup(
    name="rucio-workflow",
    version=get_version(),
    author="Xin Zhao",
    author_email="xzhao@bnl.gov",
    description="A comprehensive Python package for managing Rucio workflows",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/xzhao87/swf-rucio-example",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering :: Physics",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8", 
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=6.0.0",
            "pytest-mock>=3.6.0",
            "mock>=4.0.0",
            "flake8>=3.8.0",
            "black>=21.0.0",
        ],
        "test": [
            "pytest>=6.0.0",
            "pytest-mock>=3.6.0", 
            "mock>=4.0.0",
            "coverage>=5.0.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "rucio-workflow=rucio_workflow.cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "rucio_workflow": ["*.py"],
        "": ["README.md", "requirements.txt"],
    },
    zip_safe=False,
    keywords="rucio, panda, atlas, grid, physics, data-management",
    project_urls={
        "Bug Reports": "https://github.com/PanDAWMS/panda-server/issues",
        "Source": "https://github.com/PanDAWMS/panda-server",
        "Documentation": "https://panda-wms.readthedocs.io/",
    },
)
