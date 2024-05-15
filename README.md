# Used_Car_Market_Analysis

#### -- Project Status: [Completed]

## Project Intro/Objective
This project intends to use big data technologies to perform a thorough investigation of the trends in the used automobile industry in the United States. It also intends to look at the impact of the COVID-19 pandemic on this industry.


### Technologies
* Google Cloud Platform (Google Cloud Storage (GCS), Google Cloud Composer(GCC), Google BigQuery)
* Python pandas
* Apache Airflow
* Tableau
* etc. 

## Project Description
By leveraging big data technologies, this project provides valuable insights into the online behavior of consumers and the role of digital platforms in the pre-owned vehicle market. Two datasets are used for the analysis, where the data is cleaned and joined together. Once joined the dataset is split into 5 tables (1 fact and 4 dimension table) using Airflow and accessed from BigQuery. GigQuery is connected to Tableau to provide insightful visualizations. An interactive dashboard is created and is shown in a web interface.

* The dataset source is Kaggle
* DataSet 1: https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset
* Dataset 2: https://www.kaggle.com/datasets/austinreese/craigslist-carstrucks-data

### Files 
The files shared are codes for the data cleaning (Jupyter notebook file data_transformation), Airflow file (DAG file and 5 SQL files), and System design and Tableau Dashboard. 

