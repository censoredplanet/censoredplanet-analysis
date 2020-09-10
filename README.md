# censoredplanet-analysis

Pipeline for analysing CensoredPlanet data.

Take raw data from the
[Censored Planet Observatory](https://censoredplanet.org/data/raw) and run it
through a pipeline to create bigquery tables for easier data analysis.

Because of the size of the data involved (many TB) this project requires a
devoted Google Cloud project to run in. It is not reccomended to run yourself,
(please contact us if this is your use case) but the code is made available for
anyone who wants to understand how the data pipeline works.

There are two main top-level pieces

`transfer/scans.py`

which sets up a daily data transfer job from the Censored Planet cloud bucket to
an internal bucket.

`main.py`

Which does some additional data processing and runs an Apache Beam pipeline over
the data.
