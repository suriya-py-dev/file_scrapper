from setuptools import setup, find_packages

setup(
    name='file_scrapper',
    version='1.0',
    packages=find_packages(),
    author='Suriya',
    author_email='suriya.devpy@gmail.com',
    description='Upload the files to cloud',
    install_requires=[
        'boto3',
        'google-cloud-storage'
        'pytest',
        'unittest'
    ],
)
