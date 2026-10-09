# Neobank Data Pipeline

An end-to-end ELT pipeline built on AWS using **synthetic** neobank data (customers, accounts, transactions).

> All data in this repository is randomly generated. It contains no real customer data and is not derived from any employer's systems.

## Architecture

```
Raw CSV files  ->  Amazon S3 (data lake)  ->  Amazon Redshift Serverless (warehouse)  ->  dbt (transformations)
```

| Layer | Tool | Purpose |
|---|---|---|
| Landing zone | Amazon S3 | Stores raw CSV files |
| Warehouse | Amazon Redshift Serverless | Holds raw tables loaded with `COPY` |
| Transformation | dbt Core | Staging -> intermediate -> mart models, with data-quality tests |

## Dataset

Generated with `generate_data.py` (fixed random seed, so output is reproducible):

- `customers` — 500 rows
- `accounts` — ~760 rows, each linked to a customer
- `transactions` — ~33,000 rows, each linked to an account, spanning 12 months

## Project status

- [x] Synthetic data generator
- [ ] S3 bucket and raw file upload
- [ ] Redshift Serverless setup
- [ ] Raw tables loaded with `COPY`
- [ ] dbt project: staging models
- [ ] dbt project: mart model (monthly transaction summary per customer segment)
- [ ] dbt tests (unique, not_null, relationships)
- [ ] Architecture diagram and sample output

## How to reproduce

_To be completed as the project progresses._

### 1. Generate the data

```bash
python3 -m venv venv
source venv/bin/activate
pip install faker
python generate_data.py
```

This creates three CSV files in a local `data/` folder.

## What I learned

_To be completed at the end of the project._
