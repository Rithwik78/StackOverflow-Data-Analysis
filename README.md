# StackOverflow-Data-Analysis

# Project Overview
This project aims to analyze Stack Overflow data to uncover insights into user behaviors, popular technologies, and the challenges developers face. Utilizing Big Data frameworks like Hadoop and Spark, we extract, transform, and load data from the Stack Exchange archive, then analyze it to provide interactive dashboards with Tableau.

# Features
Data extraction from Stack Exchange Data Dump

Data preprocessing and transformation using PySpark and AWS Glue

Analysis of questions, answers, response times, and popular tags

Visualization of data trends over time using Tableau

# Technologies Used
AWS (EC2, S3, EMR, Glue)

Apache Hadoop and Spark

Hive for data warehousing

Tableau for data visualization

# Installation and Setup
AWS Account Setup: Ensure you have an AWS account with access to EC2, S3, EMR, and Glue services.

EC2 Instance: Set up an EC2 instance with Ubuntu and connect using SSH

S3 Bucket: Create an S3 bucket for data storage.

EMR Cluster: Set up an EMR cluster with Hadoop, Spark, and Hive.

Data Extraction: Use provided scripts to extract and preprocess data from Stack Exchange Data Dump.

Data Analysis: Analyze the data using PySpark on the EMR cluster.

Visualization: Use Tableau to visualize and share insights. Connect Tableau to Hive on EMR for direct data access.

# Contributing
We welcome contributions! If you're interested in improving the analysis or adding new features, please:

Fork the project

Create your feature branch (git checkout -b feature/AmazingFeature)

Commit your changes (git commit -m 'Add some AmazingFeature')

Push to the branch (git push origin feature/AmazingFeature)

Open a Pull Request

# Contact
For any queries or further discussion, feel free to reach out to me at : rithwik.maramraju@sjsu.edu

