import os
from setuptools import setup, find_packages


name = 'mysql-toolkit'


def get_version(package_name, version_file='_version.py'):
    """Retrieve the package version from a version file in the package root."""
    filename = os.path.join(os.path.dirname(__file__), package_name, 'toolkit', version_file)
    with open(filename, 'rb') as fp:
        return fp.read().decode('utf8').split('=')[1].strip(" \n'")


setup(
    name=name,
    version=get_version(name),
    packages=find_packages(),
    namespace_packages=['mysql'],
    install_requires=[
        'mysql-connector>=2.1.6',
        'tqdm',
        'differentiate',
    ],
    url='https://github.com/mrstephenneal/mysql-toolkit',
    license='MIT',
    author='Stephen Neal',
    author_email='stephen@stephenneal.net',
    description='Pure Python MySQL development toolkit.'
)
