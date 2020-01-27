from setuptools import setup

setup(
   name='bnp_mails_spark',
   version='1.0',
   description='Test application for sorting mails over Spark',
   author='Adrien Brunelat',
   author_email='adrien.brunelat@novencia.com',
   packages=['bnp_mails_spark'],
   install_requires=['pyspark', 'scipy', 'matplotlib', ''],
)
