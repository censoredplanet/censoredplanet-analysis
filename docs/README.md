# Data

This project processes the
[Censored Planet raw data](https://censoredplanet.org/data/raw) from a series of
json files into a set of bigquery tables for faster analysis.

Censored Planet produces many types of measurements. Currently this projects
integrates four of them.

-   Echo (using [Echo protocol](https://tools.ietf.org/html/rfc862) servers)
-   Discard (using [Discard protocol](https://tools.ietf.org/html/rfc863)
    servers)
-   HTTP (using existing HTTP webservers)
-   HTTPS (using HTTPS webservers)

Censored Planet measurements are relatively technical. For information in how a
particular measurement is carried out and what kinds of censorship it can find
please explore [the papers](https://censoredplanet.org/publications) on the
topic.

In particular:

-   [Quack: Scalable Remote Measurement of Application-Layer Censorship](https://censoredplanet.org/assets/VanderSloot2018.pdf)
    for `ECHO` and `DISCARD` measurements.
-   [Measuring the Deployment of Network Censorship Filters at Global Scale](https://censoredplanet.org/assets/filtermap.pdf)
    for `HTTP` and `HTTPS` measurements as part of the Hyperquack system.

For documentation on the generated tables see [tables](tables.md).

For examples on how to query and use the data in analysis see
[examples](./examples).
