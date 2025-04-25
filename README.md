#### Data engineer programming assignment

The assignment consists of two tasks: the Airflow and Databricks tasks. In this
assignment, all code must be written in Python. Furthermore, functional code is not
required in the interview; we expect the conceptual level code to show the ability to solve the
problem.
For this assignment, the following are the requirements for MySQL, AWS S3 bucket, and
Databricks. You can make your assumptions wherever necessary.
#### MySQL
Consider that the following table exists in the MySQL students_info
database.
STUDENTS(student_code, first_name, last_name, email, phone_no,
date_of_birth, honors_subject, percentage_of_marks);
Allowed attributes: student_code, honors_subject, percentage_of_marks
Blocked attributes: first_name, last_name, email, phone_no, date_of_birth
         
#### AWS S3 bucket
Bucket name: assignment-eu-central-1
Databricks
Catalog: assignment
Schema: students_info

#### 1- Airflow task
Write airflow DAG (Directed Acyclic Graph) to perform the following tasks.
-       Read the STUDENTS table from a database with allowed columns.
-       Assert that the data does not contain blocked columns.
-       Convert the data to a parquet file using snappy compression. The output file
name will be STUDENTS.parquet.
-       Upload STUDENTS.parquetfile to the S3 bucket.

#### 2- Databricks task
Consider that you are using Databricks asset bundles. A daily ETL job will read data
from the the S3 bucket and save it to the Databricks schema.
- Provide the configuration to read data from the S3 bucket (example-eu-central-1)
and save the data to the Databricks schema (students_info).
- Create a Python notebook to read the STUDENTS.parquet file and save it
to students_info schema in the student table. For this purpose, Databricks DLT
should be used.
