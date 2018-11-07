#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-logmeinrescue',
      version='0.0.8',
      description='Singer.io tap for extracting data from the LogMeIn Rescue API',
      author='Fishtown Analytics',
      url='http://fishtownanalytics.com',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_logmeinrescue'],
      install_requires=[
          'tap-framework==0.0.5',
      ],
      extras_require={
          'dev':[
              'ipython',
              'ipdb'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-logmeinrescue=tap_logmeinrescue:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_logmeinrescue': [
              'schemas/*.json'
          ]
      })
