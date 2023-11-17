from setuptools import setup, find_packages


setup(
    name='slurm_pool',
    version='0.1',
    packages=find_packages(),
    description='A Python utility package for distributed execution of python functions on a SLURM-based multi-node '
                'cluster.',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Narayan Schuetz',
    author_email='narayan dot schuetz at stanford . edu',
    url='https://github.com/NarayanSchuetz/SlurmMultiNodePool',
    license='[License]',
    keywords='SLURM cluster multiprocessing python HPC',
    python_requires='>=3.7',
)
