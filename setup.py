from setuptools import find_packages, setup

about = {}
exec(open('asynctelegraf/version.py').read(), about)


def read_requirements():
    with open("requirements.txt", "r") as f:
        return f.read().splitlines()


setup(
    name='asynctelegraf',
    version=about['version'],
    install_requires=read_requirements(),
    python_requires=">=3.7",
    description='An async client for telegraf (statsd)',
    url='git@github.com:mastak/asynctelegraf.git',
    packages=find_packages(),
    zip_safe=False,
    include_package_data=True,
)
