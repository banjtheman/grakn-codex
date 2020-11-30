"""
    Purpose:
        setup.py is executed to build the python package
"""

# Python Imports
import os
import re
from setuptools import setup, find_packages


###
# Helper Functions
###


def get_version_from_file(python_version_file="./VERSION"):
    """
    Purpose:
        Get python version from a specified requirements file.
    Args:
        python_version_file (String): Path to the version file (usually
            it is VERSION in the same directory as the setup.py)
    Return:
        requirements (List of Strings): The python requirements necessary to run
            the library
    """

    version = "development"
    if os.path.isfile(python_version_file):
        with open(python_version_file) as version_file:
            version = version_file.readline().strip().strip("\n")

    return version


def get_requirements_from_file(python_requirements_file="./requirements.txt"):
    """
    Purpose:
        Get python requirements from a specified requirements file.
    Args:
        python_requirements_file (String): Path to the requirements file (usually
            it is requirements.txt in the same directory as the setup.py)
    Return:
        requirements (List of Strings): The python requirements necessary to run
            the library
    """

    requirements = []
    with open(python_requirements_file) as requirements_file:
        requirement = requirements_file.readline()
        while requirement:
            if requirement.strip().startswith("#"):
                pass
            elif requirement.strip() == "":
                pass
            else:
                requirements.append(requirement.strip())
            requirement = requirements_file.readline()

    return requirements


def get_requirements_from_packages(packages):
    """
    Purpose:
        Get python requirements for each package. will get requirements file
        in each package's subdirectory
    Args:
        packages (String): Name of the packages
    Return:
        requirements (List of Strings): The python requirements necessary to run
            the library
    """

    requirements = []
    for package in packages:
        package_dir = package.replace(".", "/")
        requirement_files = get_requirements_files_in_package_dir(package_dir)

        for requirement_file in requirement_files:
            package_requirements = get_requirements_from_file(
                python_requirements_file=requirement_file
            )
            requirements = requirements + package_requirements

    return list(set(requirements))


def get_requirements_files_in_package_dir(package_dir):
    """
    Purpose:
        From a package dir, find all requirements files (Assuming form requirements.txt
        or requirements_x.txt)
    Args:
        package_dir (String): Directory of the package
    Return:
        requirement_files (List of Strings): Requirement Files
    """

    requirements_regex = r"^requirements[_\w]*.txt$"

    requirement_files = []
    for requirement_file in os.listdir(f"./{package_dir}"):
        if re.match(requirements_regex, requirement_file):
            requirement_files.append(f"./{package_dir}/{requirement_file}")

    return requirement_files


def get_readme(readme_file_location="./README.md"):
    """
    Purpose:
        Return the README details from the README.md for documentation
    Args:
        readme_file_location (String): Project README file
    Return:
        requirement_files (List of Strings): Requirement Files
    """

    readme_data = "Description Not Found"
    if os.path.isfile(readme_file_location):
        with open(readme_file_location, "r") as readme_file_object:
            readme_data = readme_file_object.read()

    return readme_data


###
# Main Functionality
###


def main():
    """
    Purpose:
        Main function for packaging and setting up packages
    Args:
        N/A
    Return:
        N/A
    """

    # Get Version and README
    version = get_version_from_file()
    readme = get_readme()

    # Get Packages
    packages = find_packages(exclude=("testing",))
    install_packages = [
        package for package in packages if not package.endswith(".tests")
    ]
    test_packages = [package for package in packages if package.endswith(".tests")]

    # Get Requirements and Requirments Installation Details
    install_requirements = get_requirements_from_packages(install_packages)
    test_requirements = get_requirements_from_packages(test_packages)
    setup_requirements = ["pytest-runner", "pytest", "pytest-cov", "pytest-html"]

    # Get Dependency Links For Each Requirement (As Necessary)
    dependency_links = []

    if not install_packages:
        raise Exception("No Packages Found To Install, Empty Project")

    setup(
        author="Banjo Obayomi",
        author_email="banjtheman@gmail.com",
        classifiers=[
            "Development Status :: 5 - Production/Stable",
            "Intended Audience :: Developers",
            "Natural Language :: English",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
        ],
        description=(
            "Codex is a Python package that provides a simple, yet powerful API designed to make working with the Grakn knowledge graph intuitive"
        ),
        include_package_data=True,
        install_requires=install_requirements,
        keywords=["python", "knowledge graph", "grakn", "nlp", "database"],
        long_description=readme,
        long_description_content_type="text/markdown",
        name="grakn_codex",
        packages=packages,
        project_urls={},
        python_requires=">3.6",
        # scripts=["topicblob/bin/"],
        setup_requires=setup_requirements,
        tests_require=test_requirements,
        url="https://github.com/banjtheman/grakn-codex",
        version=version,
    )


if __name__ == "__main__":
    main()
