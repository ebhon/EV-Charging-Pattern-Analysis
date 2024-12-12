# Project M3: Data Pipeline for Electric Vehicle Charging Data

Welcome to **Project M3**, a robust data pipeline designed to fetch, clean, process, and analyze electric vehicle (EV) charging data. This project is aimed at providing insights into EV charging patterns, user behavior, and energy consumption, which is crucial for building more efficient and accessible electric vehicle charging infrastructure.

### Project Overview
This project leverages the power of **Apache Airflow** to automate the ETL (Extract, Transform, Load) process, processing data from **PostgreSQL** and posting it to **Elasticsearch** for real-time querying and analysis. We also perform data cleaning, validation, and export to CSV to ensure the data is ready for further analysis.

### Key Features:
- **ETL Pipeline**: Automates the data fetching, cleaning, and processing workflows.
- **Data Cleaning**: Handles missing values, duplicate records, and standardizes column names.
- **Data Validation**: Validates the dataset to ensure consistency and accuracy using **Great Expectations**.
- **Elasticsearch Integration**: Posts cleaned data to Elasticsearch for easy querying and analytics.
- **CSV Export**: Saves the cleaned dataset as a CSV file for offline analysis and reporting.

### Architecture

The project follows a modular architecture with each functionality encapsulated in different Python files within the `plugins` folder. The main components are:

1. **Airflow DAG (`dag.py`)**: Defines the workflow with tasks like data fetching, cleaning, export to CSV, and posting to Elasticsearch.
2. **Data Loading (`load_data.py`)**: Fetches raw data from a PostgreSQL database.
3. **Data Cleaning (`clean_data.py`)**: Cleans the dataset by handling missing values, removing duplicates, and standardizing columns.
4. **CSV Export (`export_to_csv.py`)**: Exports cleaned data to a CSV file.
5. **Elasticsearch Posting (`post_to_elastic.py`)**: Posts the cleaned data to an Elasticsearch index.

### Project Setup

To run this project locally, you'll need to have Docker installed to containerize the services. Below are the steps to set up the project:

#### Prerequisites
- Docker & Docker Compose
- Python 3.8+
- Airflow 2.x
- PostgreSQL
- Elasticsearch and Kibana (for querying and visualization)

#### Steps to Run:

1. Clone this repository:
   ```bash
   git clone https://github.com/your-username/project-m3.git
   cd project-m3
2. Build and start the services using Docker Compose:
   ```bash
   docker-compose up --build
3. Access the Airflow UI:
   * Go to http://localhost:8080 to monitor and trigger the Airflow DAG.
4. Check the Elasticsearch dashboard on http://localhost:9200 and Kibana on http://localhost:5601 for querying and visualizing the data.

## Folder Structure:
project-m3/  
├── dags/  
│   ├── dag.py               # Main Airflow DAG  
├── plugins/  
│   ├── clean_data.py        # Data cleaning functionality  
│   ├── export_to_csv.py     # CSV export functionality  
│   ├── load_data.py         # Data loading functionality  
│   ├── post_to_elastic.py   # Posting data to Elasticsearch  
├── airflow_ES.yaml          # Docker Compose configuration  
├── gx.ipynb                 # Great Expectations validation notebook  
├── ddl.txt                  # DDL and DML scripts for PostgreSQL  
└── .gitignore               # Git ignore file  

Data Validation with Great Expectations
This project integrates Great Expectations to validate the integrity of the dataset, ensuring that:
* The vehicle model is within a valid set of expected models (e.g., BMW i3, Tesla Model 3).
* The charging duration falls within reasonable limits (0 to 24 hours).
* The charging cost is non-negative and within a maximum value of $100.
These validations help ensure that only accurate and meaningful data is pushed through the pipeline.

## Contributing
We welcome contributions! Feel free to fork the repository, create an issue, or submit a pull request with your improvements.

To contribute:
1. Fork the repository.
2. Create a new branch for your feature or bugfix.
3. Make your changes.
4. Open a pull request to the main branch.

## License
This project is licensed under the MIT License - see the LICENSE file for details.


We hope this project helps you understand how to build a complete data pipeline from fetching and cleaning data to posting it for real-time analysis. Happy coding!