<p align="center">
  <a href="" rel="noopener">
 <img width=500px height=100px src="https://docs.delta.io/latest/_static/delta-lake-logo.png" alt="Project logo"></a>
</p>

<h3 align="center">Delta Lake is an open source project that enables building a Lakehouse architecture on top of data lakes. Delta Lake provides ACID transactions, scalable metadata handling, and unifies streaming and batch data processing on top of existing data lakes, such as S3, ADLS, GCS, and HDFS.</h3>

<div align="center">

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](/LICENSE)

</div>

---

# Introduction to Delta Upsert
This repository exemplifies a simple ELT process using delta to perform upsert and remove data files that aren't in the latest state of the transaction log for the table.

## 📝 Table of Contents

- [1.raw-zone-ingestion](https://github.com/anneglienke/upsert-delta/blob/main/1.raw-zone-ingestion.py) - first ingestion to raw-zone
- [2.raw-zone-incremental](https://github.com/anneglienke/upsert-delta/blob/main/2.raw-zone-incremental.py) - incremental ingestion (append) to raw-zone 
- [3.staging-zone-ingestion](https://github.com/anneglienke/upsert-delta/blob/main/3.staging-zone-ingestion.py) - snapshot of the current db and creation of staging-zone (delta)
- [4.staging-zone-incremental](https://github.com/anneglienke/upsert-delta/blob/main/4.staging-zone-incremental.py) - incremental snapshot ingestion (delta)
- Check scripts (check_raw-zone.py, check_staging-zone.py) - scripts to read and monitor tables being created
- CSV files (titanic.csv, titanic2.csv, titanic3.csv) - simulate changes on tables being ingested
- Directories (raw-zone, staging-zone) - store the data 

