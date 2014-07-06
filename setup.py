import os
from setuptools import setup, find_packages

setup(
    name='grano-ql',
    version=os.environ.get('GRANO_RELEASE', '0.0.1'),
    description="An entity and social network tracking software for"
                + "news applications (query language extension)",
    long_description=open('README.md').read(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        ],
    keywords='sql graph sna networks journalism ddj entities',
    author='Friedrich Lindenberg',
    author_email='friedrich@pudo.org',
    url='http://docs.grano.cc',
    license='MIT',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=[],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'grano>=0.3.1',
    ],
    entry_points={
        'grano.startup': [
            'ql = grano.ql.interface:Installer'
        ]
    },
    test_suite="grano.ql.test",
    tests_require=[]
)
