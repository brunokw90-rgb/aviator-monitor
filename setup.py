from setuptools import setup, find_packages

setup(
    name="random_audit",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[],
    entry_points={
        "console_scripts": [
            "random-audit=random_audit.cli:main",
        ]
    },
)