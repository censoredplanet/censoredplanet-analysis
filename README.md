# Censored Planet Data Analysis Pipeline

This pipeline takes raw data from the
[Censored Planet Observatory](https://censoredplanet.org/data/raw) and runs it
through a pipeline to create bigquery tables for easier data analysis.

Because of the size of the data involved (many TB) this project requires a
devoted Google Cloud project to run in. It is not recommended to run yourself,
but the code is made available for anyone who wants to understand how the data
pipeline works.

![Master Build Status](https://github.com/Jigsaw-Code/censoredplanet-analysis/workflows/build/badge.svg?branch=master)

## System Diagram

![System Diagram](system-diagram.svg)

To contribute to this pipeline see the [development documentation](docs/development.md).
