from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='neodb',
    version='0.0.1',
    package_dir={'': 'src'},
    packages=find_packages(where='src'),
    py_modules=['neodb'],
    url='https://github.com/eyukselen/neodb',
    license='MIT',
    author='emre',
    author_email='',
    description='simple key value store/document db',
    long_description=long_description,
    long_description_content_type="text/markdown",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
    project_urls={
        'Homepage': 'https://github.com/eyukselen',
        'Source': 'https://github.com/eyukselen/neodb',
    }
)
