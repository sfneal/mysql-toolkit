import os
from setuptools import setup, find_packages


name = 'mysql-toolkit'


def get_version(version_file='_version.py'):
    """Retrieve the package version from a version file in the package root."""
    filename = os.path.join(os.path.dirname(__file__), 'mysql', 'toolkit', version_file)
    with open(filename, 'rb') as fp:
        return fp.read().decode('utf8').split('=')[1].strip(" \n'")


setup(
    name=name,
    version=get_version(),
    packages=find_packages(),
    namespace_packages=['mysql'],
    install_requires=[
        'dirutility>=0.5.6',
        'mysql-connector>=2.2.9',
        'differentiate>=1.2.1',
        'sqlparse>=3.0.0',
        'looptools>=1.1.2',
        'tqdm',
    ],
    url='https://github.com/mrstephenneal/mysql-toolkit',
    license='MIT',
    author='Stephen Neal',
    author_email='stephen@stephenneal.net',
    description='Pure Python MySQL development toolkit.'
)
