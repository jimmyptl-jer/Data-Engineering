# Data Engineering

This repository holds a batch data lake for stock market data on AWS, plus a legacy pipeline and some IAM policy documents. The active project, [`data-engineering/datalake-pipeline`](data-engineering/datalake-pipeline/README.md), pulls data from the Alpha Vantage, Massive and Finnhub APIs into Amazon S3 and refines it through Bronze, Silver and Gold layers with PySpark. Its infrastructure is defined with AWS SAM.

## Repository layout

```text
Data-Engineering/
├── archive/
│   └── youtube_pipeline/          # Legacy YouTube Data API pipeline (not maintained)
├── data-engineering/
│   └── datalake-pipeline/         # Stock data lake (active project)
│       ├── ingestion/             # API → Bronze Lambdas; each folder is a self-contained SAM app
│       │   ├── alpha_vantage/
│       │   ├── finnhub/
│       │   └── massive/
│       ├── notes/                 # Design notes and dataset specifications
│       ├── src/stock_pipeline/    # Bronze → Silver → Gold PySpark transform job
│       ├── tests/                 # pytest suite
│       ├── template.yaml          # Root SAM stack: bucket, transform Lambda, workflow, alarms
│       ├── samconfig.toml         # Root stack deploy settings (stack name stock-pipeline)
│       ├── requirements.txt       # Transform job runtime dependencies
│       └── requirements-dev.txt   # Adds ingestion, test and lint dependencies
├── infra/
│   └── iam/                       # IAM policy documents for S3 replication
└── README.md
```

## Projects

| Path | Status | Description |
| --- | --- | --- |
| [`data-engineering/datalake-pipeline`](data-engineering/datalake-pipeline/README.md) | Active | Stock data lake. Three ingestion Lambdas (Alpha Vantage, Massive, Finnhub) write raw JSON to S3 Bronze, a PySpark job builds the Silver and Gold layers, and a Step Functions workflow runs ingestion and then the transform on a schedule. |
| [`archive/youtube_pipeline`](archive/youtube_pipeline/) | Legacy, not maintained | Earlier pipeline that fetched data from the YouTube Data API v3, stored the raw JSON in S3 with boto3 and transformed it with PySpark. No template in this repository deploys it. |
| [`infra/iam`](infra/iam/) | Reference | Trust and permissions policies for an IAM role that Amazon S3 assumes to replicate objects from the `graywolf--data--lake` bucket to `graywolf-data-lake-mumbai-region` and `graywolf-data-lake-london-region`. No template in this repository references these files. |

## Architecture

```text
EventBridge schedule (default cron(0 14 ? * MON-FRI *): 14:00 UTC, Monday to Friday)
  │
  ▼
Step Functions state machine stock-data-pipeline-<EnvironmentName>
  1. CreateRunId            run_id = batch_<execution name>
  2. IngestToBronze         three Lambdas in parallel, API → S3 Bronze (raw JSON)
       ingest-alpha-vantage-<EnvironmentName>   daily_time_series, company_overview
       ingest-massive                           exchanges, ticker_reference, aggregates,
                                                splits, dividends, stock_overview
       ingest-finnhub                           ticker_reference
     A branch whose Lambda returns statusCode 500 fails the execution.
  3. TransformBronzeToGold  Lambda stock-data-pipeline-<EnvironmentName>, payload {"run_id": ...}
                            S3 Bronze → Silver (CSV and Parquet) → Gold (CSV), with PySpark
```

Every ingestion Lambda writes to `stock/bronze/source=<source>/dataset=<dataset>/ingestion_date=YYYY-MM-DD/run_id=<run_id>/`, and the transform reads the `run_id` partition it is given, which ties one execution's ingestion and transform together. The ingestion Lambdas are separate, self-contained SAM apps under `ingestion/<source>/`; their `samconfig.toml` files disable their own schedules because the workflow invokes them. The root [`template.yaml`](data-engineering/datalake-pipeline/template.yaml) defines the data-lake bucket, the transform Lambda, the state machine, and CloudWatch alarms (failed executions, transform Lambda errors and duration) that notify an SNS topic.

## Status and known limitations

- **Enabled in `StockPipeline.run()`:** Alpha Vantage `company_overview` and Massive `stock_overview` from Bronze to Silver, then the Gold `company_dataset` CSV (the Massive stock overview left-joined with the Alpha Vantage company overview on `symbol`).
- **In the code but commented out of `run()` (work in progress):** the daily time series with its date watermark, Massive exchanges, Finnhub `ticker_reference`, and a fuller Gold build. Massive aggregates and dividends have only a placeholder step, and Massive splits and ticker reference data are not processed beyond Bronze.
- **PySpark on Lambda:** the transform Lambda packages PySpark, which needs a JVM that the standard Lambda Python runtime does not provide and exceeds the Lambda zip package size limit. Running the transform in AWS needs a container-image Lambda, AWS Glue or Amazon EMR; that choice has not been made yet.
- **All-or-nothing Massive ingestion:** the Massive Lambda returns `statusCode` 500 if any one of its six endpoints fails, which fails the whole execution.
- **Separate clocks:** the ingestion Lambdas and the transform each derive `ingestion_date` from their own clock, so an execution that crosses midnight UTC can miss the Bronze data it just ingested.

## Tech stack

| Area | Technology |
| --- | --- |
| Language | Python; Lambda runtime `python3.14` (Python 3.13 works for local development and tests) |
| Processing | PySpark 4.0.4, with `hadoop-aws` 3.4.1 for `s3a://` access |
| API and AWS clients | `requests` (Alpha Vantage, Finnhub), `massive` (Massive), `boto3` |
| Storage | Amazon S3: Bronze JSON, Silver CSV and Parquet, Gold CSV, watermark JSON |
| Compute and orchestration | AWS Lambda, AWS Step Functions, Amazon EventBridge schedule |
| Monitoring | Amazon CloudWatch alarms and log groups, Amazon SNS |
| Infrastructure as code | AWS SAM (AWS CloudFormation) |
| Testing and tooling | pytest, PyYAML, flake8, black |

## Getting started

The active project lives in `data-engineering/datalake-pipeline`. To set up a development environment and run the tests:

```bash
git clone https://github.com/jimmyptl-jer/Data-Engineering.git
cd Data-Engineering/data-engineering/datalake-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
python -m pytest                 # Spark tests need Java 17+ and are skipped when no Java runtime is found
```

Then continue with the [Quick start](data-engineering/datalake-pipeline/README.md#quick-start) in the pipeline README. Running the transform job locally needs AWS credentials with access to the data-lake bucket.

Deployment is one SAM stack per `ingestion/<source>` folder plus the root stack in `data-engineering/datalake-pipeline`. Deploy the ingestion stacks first, because the workflow invokes those Lambdas by name. The Massive and Finnhub stacks need their API keys as the `MassiveApiKey` and `FinnhubApiKey` parameters, which have no default.

## License

This project is for educational and portfolio purposes.

## Author

**Jimmy Patel** — [GitHub](https://github.com/jimmyptl-jer)
