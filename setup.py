from setuptools import setup, find_packages

setup(
    name='esg_investment_returns',
    version='0.1',
    packages=find_packages(include=['financial_report_analyzer', 'index_replication']),
)
