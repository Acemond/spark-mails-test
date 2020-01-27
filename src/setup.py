from setuptools import setup

setup(
   name='bnp_mails_spark',
   version='1.0',
   description='Test application for sorting mails over Spark',
   author='Adrien Brunelat',
   author_email='adrien.brunelat@novencia.com',
   packages=['bnp_mails_spark'],  #same as name
   install_requires=['pyspark', 'scipy', 'matplotlib', ''], #external packages as dependencies
)
