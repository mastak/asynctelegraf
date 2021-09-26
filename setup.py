from setuptools import find_packages, setup

about = {}
exec(open('asynctelegraf/version.py').read(), about)

with open("README.md", "r") as fh:
    long_description = fh.read()


setup(
    name='asynctelegraf',
    version=about['version'],
    python_requires=">=3.7",
    description='asyncio python client for telegraf / statsd / AWD cloudwatch',
    author='Ihor Liubymov',
    author_email='infunt@gmail.com',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/mastak/asynctelegraf',
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
)
