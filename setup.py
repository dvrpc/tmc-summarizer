from setuptools import find_packages, setup

setup(
    name='tmc_summarizer',
    packages=find_packages(),
    version='0.1.0',
    description='Process raw TMC data into more functional formats',
    author='Aaron Fraint, AICP',
    entry_points="""
        [console_scripts]
        tmc=tmc_summarizer.cli:main
    """,
)
