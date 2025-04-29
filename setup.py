from setuptools import setup, find_packages

setup(
    name='GCP-Manager',
    version='1.1.2',
    author='Hsieh Yu-Hung',
    author_email='yuhunghsieh@accuinbio.com',
    description='Handle GCP tasks like file transfer, SQL query, etc.',
    long_description=open('README.md', encoding='utf-8').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/Hsieh-Yu-Hung/GCP_manager.git',
    packages=find_packages(),
    install_requires=[
        'pandas>=1.5.0',
        'numpy>=1.21.0',
        'google-cloud',
        'google-cloud-storage',
        'google-cloud-bigquery',
        'bigframes',
        'pandas-gbq',
        'openpyxl',
        'pandas_gbq',
        'sqlalchemy',
        'cloud-sql-python-connector',
        'pg8000'
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
)