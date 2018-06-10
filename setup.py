from setuptools import setup, find_packages
import re, io

__version__ = re.search(r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]',
                        io.open('loadit/__init__.py', encoding='utf_8_sig').read()).group(1)

setup(
    name='loadit',
    version=__version__,
    description='A blazing fast database for FEM loads',
    url='https://github.com/alvarosanz/loadit',
    author='Alvaro Sanz Oriz',
    author_email='alvaro.sanz.oriz@gmail.com',
    packages=find_packages(),
    license='MIT',
    keywords='NASTRAN FEM engineering',
    long_description=open('README.rst').read(),
    install_requires=['numpy', 'pandas', 'pyarrow', 'pyjwt', 'cryptography'],
    python_requires='>=3.6',
)
