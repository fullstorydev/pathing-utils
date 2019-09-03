from setuptools import setup

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='pathutils',
    version="0.1.19",
    packages=["pathutils"],
    description="Collection of pathing analytics utilities for FullStory Hauser export data",
    long_description=open("README.md").read(),
    install_requires=requirements,
    license="MIT"
)