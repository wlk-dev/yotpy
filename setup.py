from setuptools import setup, find_packages

setup(
    name='yotpy',
    version='0.1',
    packages=find_packages(),
    install_requires=[
        "aiohttp",
        "requests",
    ],
    author='William Koelling',
    author_email='william.koelling@gmail.com',
    description="An easy-to-use Python wrapper for the Yotpo API, that was developed to fill the gap of existing packages that didn't meet my needs.",
    license='MIT',
    keywords='yotpo api wrapper python data transformation client integration review ecommerce',
)
