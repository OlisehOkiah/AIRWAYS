from setuptools import find_packages, setup

def parse_requirements(filename):
    """Parse a requirements file into a list of requirements."""
    with open(filename, 'r') as file:
        requirements = [line.strip() for line in file if line.strip() and not line.startswith('#')]
    return requirements

setup(
    name='British',  # Use a more Pythonic name; underscores instead of spaces
    version='0.0.1',
    author='Oliseh Okiah',
    author_email='okiaholiseh123@gmail.com',
    description='A brief description of the package',  # Add a short description
    long_description=open('README.md').read(),  # If you have a README.md file, this adds a long description
    long_description_content_type='text/markdown',  # Specify the format of your long description
    url='https://github.com/OlisehOkiah/AIRWAYS.git',  # URL to your project’s homepage or repository
    packages=find_packages(),  # Automatically find and include all packages in the directory
    install_requires=parse_requirements('requirements.txt'),  # Parse and include dependencies from requirements.txt
    classifiers=[
        'Development Status :: 3 - Alpha',  # Pick appropriate classifiers from https://pypi.org/classifiers/
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',  # Add specific Python versions you support
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    python_requires='>=3.6',  # Specify the minimum Python version required
)
