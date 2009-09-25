from glob import glob
from setuptools import setup, Extension

# Get text from README.txt
readme_text = file('README.txt', 'rb').read()

_rtree = Extension('rtree._rtree',
                  sources=['rtree/_rtreemodule.cc', 'rtree/wrapper.cc',
                           'rtree/gispyspatialindex.cc'] \
                         +glob('spatialindex/tools/*.cc') \
                         +glob('spatialindex/storagemanager/*.cc') \
                         +glob('spatialindex/spatialindex/*.cc') \
                         +glob('spatialindex/rtree/*.cc'),
                  include_dirs=['spatialindex/include']
                  )

setup(name          = 'Rtree',
      version       = '0.4.1',
      description   = 'R-Tree spatial index for Python GIS',
      license       = 'LGPL',
      keywords      = 'gis spatial index',
      author        = 'Sean Gillies',
      author_email  = 'sgillies@frii.com',
      maintainer    = 'Sean Gillies',
      maintainer_email  = 'sgillies@frii.com',
      url   = 'http://trac.gispython.org/projects/PCL/wiki/RTree',
      long_description = readme_text,
      packages      = ['rtree'],
      ext_modules   = [_rtree],
      install_requires = ['setuptools'],
      test_suite = 'tests.test_suite',
      classifiers   = [
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Operating System :: OS Independent',
        'Programming Language :: C',
        'Programming Language :: C++',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Database',
        ],
)
