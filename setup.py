from setuptools import setup

setup(
    name='django_hana',
    version='2.1',
    include_package_data=True,
    description='SAP HANA backend for Django 2.2',
    author='NoviSystems, Max Bothe, Kapil Ratnani',
    author_email='mejohn@novi.systems, mathebox@gmail.com, kapil.ratnani@iiitb.net',
    url='https://github.com/mejohn/django_hana',
    packages=['django_hana'],
    install_requires=[
        'django~=2.2.0',
    ],
)
