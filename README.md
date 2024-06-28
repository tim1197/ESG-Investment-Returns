<div align="center">
  <h1>ESG Investment Returns</h1>
</div>

This repo intends to analyze the impact of ESG-related communication on company's and index returns.

The annual reports are fetched from the [SEC Edgar database](https://www.sec.gov/edgar). For the analysis, the following pre-trained LLMs are used which can be found on 🤗Hugging Face:

* [ESGBERT/EnvironmentalBERT-environmental](https://huggingface.co/ESGBERT/EnvironmentalBERT-environmental)
* [ESGBERT/SocialBERT-social](https://huggingface.co/ESGBERT/SocialBERT-social)
* [ESGBERT/GovernanceBERT-governance](https://huggingface.co/ESGBERT/GovernanceBERT-governance)

# Installation

### Python Setup

    make setup

#### Optional: Create a Jupyter Kernel

    make kernel

### Database Setup

    POSTGRES_PASSWORD=mysecretpassword make database

 Set the database password accordingly in the [database connector](financial_report_analyzer/database_connector.py) DB PATH constant:

 ```python
DB_PASSWORD = "mysecretpassword"
```