from setuptools import setup


setup(name='tshistory_supervision',
      version='0.5.0',
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr, arnaud.campeas@pythonian.fr',
      url='https://bitbucket.org/pythonian/tshistory_supervision',
      description='Provide a supervision mechanism over `tshistory`',

      packages=['tshistory_supervision'],
      install_requires=[
          'tshistory'
      ],
      entry_points={'tshistory.subcommands': [
          'migrate-supervision-0.5-to-0.6=tshistory_supervision.cli:migrate_supervision_dot_5_to_dot_6',
          'list-supervised-series-mismatch=tshistory_supervision.cli:list_mismatch',
          'shell=tshistory_supervision.cli:shell'
      ]},
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Scientific/Engineering',
          'Topic :: Software Development :: Version Control'
      ]
)
