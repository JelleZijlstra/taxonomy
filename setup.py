"""

Install the taxonomy package.

"""
from setuptools import setup


setup(
    name="taxonomy",
    version="0.0",
    description="A tool for maintaining a taxonomic database.",
    keywords="taxonomy",
    author="Jelle Zijlstra",
    author_email="jelle.zijlstra@gmail.com",
    url="https://github.com/JelleZijlstra/taxoomy",
    license="MIT",
    packages=["taxonomy"],
    install_requires=[
        "peewee==3.13.3",
        "IPython<7",
        "prompt_toolkit<2",
        "PyMySQL",
        "requests",
        "unidecode",
        "python-levenshtein",
        "bs4",
        "mypy",
        "flake8",
        "pytest",
        "aiohttp",
        "aiohttp_graphql",
        "graphene",
        "typing_inspect",
        "types-requests",
    ],
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Environment :: Console",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.11",
    ],
)
