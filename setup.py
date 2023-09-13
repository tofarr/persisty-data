import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="persisty-data",
    author="Tim O'Farrell",
    author_email="tofarr@gmail.com",
    description="A binary data persistence layer built on top of persisty",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/tofarr/persisty-data",
    packages=setuptools.find_packages(exclude=("tests*",)),
    install_requires=[
        "persisty[all]",
    ],
    extras_require={
        "dev": [
            "black~=23.3",
            "pytest~=7.2",
            "pytest-cov~=4.0",
            "pytest-xdist~=3.2",
            "pylint~=2.17",
            "boto3~=1.26",
            "moto~=3.1",
        ],
        "img": [
            "Pillow~=10.0"
        ]
    },
    setup_requires=["setuptools-git-versioning"],
    setuptools_git_versioning={"enabled": True, "dirty_template": "{tag}"},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
