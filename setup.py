from setuptools import setup

from tshistory_supervision import __version__


setup(name='tshistory_supervision',
      version=__version__,
      author='Pythonian',
      author_email='aurelien.campeas@pythonian.fr, arnaud.campeas@pythonian.fr',
      url='https://hg.sr.ht/~pythonian/tshistory_supervision',
      description='Provide a supervision mechanism over `tshistory`',

      packages=['tshistory_supervision'],
      install_requires=[
          'tshistory >= 0.18.0'
      ],
      entry_points={
          'tshistory.subcommands': [
              'fix-supervision-status=tshistory_supervision.cli:fix_supervision_status',
              'list-supervised-series-mismatch=tshistory_supervision.cli:list_mismatch',
              'shell=tshistory_supervision.cli:shell'
          ],
          'tshistory.migrate.Migrator': [
              'migrator=tshistory_supervision.migrate:Migrator'
          ]
      },
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
