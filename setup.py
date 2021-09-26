from setuptools import find_packages, setup

about = {}
exec(open('asynctelegraf/version.py').read(), about)


setup(
    name='asynctelegraf',
    version=about['version'],
    python_requires=">=3.7",
    description='An async client for telegraf (statsd)',
    url='https://github.com/mastak/asynctelegraf',
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
)
