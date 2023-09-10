from setuptools import setup, find_packages

setup(
    name='fake_table_data_generator',
    version="1.0.9",
    packages=find_packages(),
    install_requires=[
        'rstr==3.2.1',
        'pandas==1.2.4',
        "numpy==1.21.5",
        "scipy==1.7.3",
        "loguru==0.5.1",
        "pyspark==3.3.0",
        "sqlalchemy==1.4.15",
    ],
    author='Alexander Maksimovich',
    python_requires=">=3.7.1,<3.11",
    author_email='cahr2001@mail.com',
    description='Fake data generator',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
)
