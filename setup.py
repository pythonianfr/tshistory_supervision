from setuptools import setup


setup(name='tshistory_supervision',
      version='0.3.0',
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr, arnaud.campeas@pythonian.fr',
      url='https://bitbucket.org/pythonian/tshistory_supervision',
      description='Provide a supervision mechanism over `tshistory`',

      packages=['tshistory_supervision'],
      install_requires=[
          'tshistory'
      ],
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 2',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Scientific/Engineering',
          'Topic :: Software Development :: Version Control'
      ]
)
