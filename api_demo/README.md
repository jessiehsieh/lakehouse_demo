# Data Ingestion Pipeline

## Table of Contents
- [Project Overview](#project-overview)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Anonymization Process](#anonymization-process)
- [Testing](#testing)

## Project Overview

This project implements a data pipeline that ingests data from an external API, anonymizes sensitive information, stores the data in a database, and generates a report based on the stored data. The pipeline is designed to be modular, scalable, and easy to configure.

## Features

- **Data Ingestion**: Fetch data from FakerAPI with retry logic.
- **Data Anonymization**: Replace sensitive information with anonymized values.
- **Database Storage**: Store the anonymized data in a local SQLite database.
- **Report Generation**: Questions are addressed in single queries and displayed as logs.

## Installation

### Prerequisites

- Python 3.12
- Pipenv or virtualenv (recommended)

### Steps

1. **Clone the repository**:
    ```bash
    git clone https://github.com/jesssiehsieh/Lakehouse.git
    cd etl_demo
    ```

2. **Set up a virtual environment**:
    ```bash
    pipenv install --dev
    pipenv shell
    ```

3. **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Ingest Data

Run the data ingestion script:

```bash
python api_demo/etl.py
```


## Anonymization Process

The anonymization module uses techniques like and data masking and generalization to anonymize sensitive fields. 

Example:
Before: {"firstname": "John", "birthday":"1980-09-04", "email": "john.doe@example.com"}
After: {"firstname": "\*\*\*\*", "birthday":"\*\*\*\*", "age_group":"\(40-50\]", "email": "\*\*\*\*", "email_domain": "example.com"}

## Testing

Unit tests are provided to ensure the functionality of each component in the pipeline. Run the tests using:

```bash
cd api_demp && pytest tests
```