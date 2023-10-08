# Airline_data_enrichment_and_analysis

## Problem Statement:

A daily file having data on airplane flights is forwarded by upstream team by uploading it to a S3 bucket. Data on airports between which these flights takes place is already present on the database. An automated system needs to be developed which enriches the flight data with data on airports, filters the records based on business logic and stores it in on an OLAP style database in a de-normalized form. Furthermore, the data needs to be analysed.

Business logic dictates that data on flights whose departure has been delayed by more than an hour are only needed to be stored.

## Design:

![airline_data_enrichment drawio (1)](https://github.com/DS-v/Airline_data_enrichment_and_analysis/assets/59478620/fdbba642-63f0-44b2-ba6e-357de2af22bb)
The execution graph for the Step function is as follows:
![stepfunctions_graph](https://github.com/DS-v/Airline_data_enrichment_and_analysis/assets/59478620/68b34317-c73d-4921-84ba-bb37fce48d5e)
Here choice 1 is based on whether crawler is running or run is successfull. Choice 2 is based on whether Glue job succeded or not.





