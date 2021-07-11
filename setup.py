from setuptools import setup
from setuptools_rust import Binding, RustExtension

extras = {}
extras["testing"] = ["pytest"]

setup(
    name="vulcan",
    version="0.1.0a1",
    description="A package for reconstructing Wikipedia database dumps",
    author="Colin Dablain",
    author_email="colin.dablain@r3th.ink",
    long_description=open("README.md", "r", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/DayOfThePenguin/vulcan",
    python_requires=">=3.8",
    rust_extensions=[RustExtension(
        "hello_rust.hello_rust", binding=Binding.PyO3)],
    classifiers=[
        "Development Status: : 3 - Alpha",
        "Programming Language:: Python:: 3.8",
        "License:: OSI Approved:: GNU Affero General Public License v3 or later(AGPLv3+)",
        "Operating System:: OS Independent"
    ],
    install_requires=[
        "blist==1.3.6",
        "mwparserfromhell==0.6.2",
        "pydantic==1.8.2",
        "SQLAlchemy==1.4.20",
        "Unidecode==1.2.0"
    ],
    package_dir={"": "py_src"},
    packages=[
        "vulcan",
        "vulcan.database",
        "vulcan.wikitools",
    ],
    # rust extensions are not zip safe, just like C-extensions.
    zip_safe=False,
)
