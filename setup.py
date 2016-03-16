'''
Created on Jul 10, 2014

@author: jtaghiyar
'''

import codecs
import os
import re
from setuptools import setup

def read(*paths):
    here = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(here, *paths)) as f:
        return f.read()

def get_version():
    version_file = read("kronos", "kronos_version.py")
    version_match = re.search(r"^kronos_version = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")

long_description = read('README.md')

setup(name='kronos_pipeliner',
      version=get_version(),
      description='A workflow assembler for genome analytics and informatics',
      long_description=long_description,
      classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Topic :: Scientific/Engineering :: Bio-Informatics", 
        ],
      keywords='pipeline workflow bioinformatics kronos',
      author='M. Jafar Taghiyar',
      author_email='jafar.taghiyar@gmail.com',
      url='https://github.com/jtaghiyar/kronos',
      license='MIT',
      packages=['kronos', 'templates'],
      entry_points={'console_scripts':['kronos=kronos:main']},
      install_requires = ['pyyaml>=3.11', 'ruffus==2.4.1']
     )

