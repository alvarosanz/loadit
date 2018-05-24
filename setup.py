from setuptools import setup, find_packages


setup(
    name='loadit',
    version='0.1.0',
    description='A blazing fast database for FEM loads',
    url='https://github.com/alvarosanz/loadit',
    author='Álvaro Sanz Oriz',
    author_email='alvaro.sanz.oriz@gmail.com',
    packages=find_packages(),
    license='MIT',
    keywords='NASTRAN FEM engineering',
    long_description=open('README.rst').read(),
    install_requires=['numpy', 'pandas', 'pyarrow', 'pyjwt', 'cryptography'],
    python_requires='>=3.6',
)
