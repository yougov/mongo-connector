# Copyright 2012 10gen, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This file will be used with PyPi in order to package and distribute the final
# product.

classifiers = """\
Development Status :: 4 - Beta
Intended Audience :: Developers
License :: OSI Approved :: Apache Software License
Programming Language :: Python
Programming Language :: JavaScript
Topic :: Database
Topic :: Software Development :: Libraries :: Python Modules
Operating System :: Unix
"""

from distutils.core import setup
import sys

__doc__ = ""
doclines = __doc__.split("\n")

setup(name="mongo-connector",
      version="1.0.0",
      maintainer="10Gen",
      maintainer_email="leonardo.stedile@10gen.com",
      #url = "https://github.com/AayushU/mongo-connector",
      license="http://www.apache.org/licenses/LICENSE-2.0.html",
      platforms=["any"],
      description=doclines[0],
      classifiers=filter(None, classifiers.split("\n")),
      long_description="\n".join(doclines[2:]),
      #include_package_data=True,
      packages=['mongo-connector.doc_managers', 'mongo-connector'],
      #packages = find_packages('src'),  # include all packages under src
      #package_dir = {'':'src'},   # tell distutils packages are under src
      #      scripts=[],
      install_requires=['pymongo', 'pyes', 'pysolr ==2.1.0', 'simplejson'],

      package_data={
          '': ['*.xml'],
          'mongo-connector': ['README.md', 'config.txt']
          # If any package contains *.txt files, include them:
          # And include any *.dat files found in the 'data' subdirectory
          # of the 'mypkg' package, also:
      }
      )
