from setuptools import find_packages, setup

setup(
    name='tmc_summarizer',
    packages=find_packages(),
    version='1.0.0',
    description='Process raw TMC data into more functional formats',
    author='Aaron Fraint, AICP; Mark Morley, AICP', 
    entry_points="""
        [console_scripts]
        tmc=tmc_summarizer.cli:main
    """,
)
