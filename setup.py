from setuptools import setup, find_packages
import sys, os

version = '1.0'

requires = [
    'rabbitpy'
]
test_requires = []

setup(name='maxcarrot',
      version=version,
      description="Library to wrap rabbitmq--max bindings and actions",
      long_description="""\
""",
      classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
      keywords='',
      author='Carles Bruguera',
      author_email='carles.bruguera@upcnet.es',
      url='https://github.com/UPCnet/maxcarrot',
      license='GPL',
      packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
      include_package_data=True,
      zip_safe=True,
      install_requires=requires,
      tests_require=requires + test_requires,
      test_suite="maxcarrot.tests",
      extras_require={
          'test': []
      },

      entry_points="""
      # -*- Entry points: -*-
      """,
      )