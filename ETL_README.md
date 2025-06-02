# Hello Fresh food difficulty

## Architechture

![image](https://github.com/kanikasodhi02/HelloFresh/assets/140825271/0e11fdd9-6f31-4366-9b1d-3e94124b67ae)


**Extract:** 
- Read data from input folder
- Perform schema validation according to template
- Pass data to transform layer

**Transform:** 
- Parse column types according to template
- Perform data quality operations like remove duplicates, remove incorrect data of prepare time and cooking time
- Invalidated data will go to rejected DQ folders(i.e. /data/dq_rejected_file)
- Convert cooking and prepare time into minutes format
- Pass data to persis layer

**Persist:** 
- Write data in partition(cooking and perpare time) that can futher use for different insights like finding reciepes difficulty
  
**KPI:** 
- Filter out data containing Beef ingridient
- Calculate difficulty of reciepes
- Perform average operation to find out the difficulty level in Easy, Medium, and Hard


## **Prerequisite:**

- Required docker installed in your system

     
## Instructions to run application

### Build the Docker Image
- Open a terminal or command prompt
- Go to project root directory
- Run the following command to build the Docker image
  
`docker-compose build`


Please note,
- During docker build process unit test cases will automatically run. You don't need to run the tests explicitly.
- If you are using M1 Mac, please run this command before running build command `export DOCKER_DEFAULT_PLATFORM=linux/amd64`
  

### Run Application

After building the Docker image, you can run the application using the following command.
	  
      `docker-compose run hellofresh`

This command will start the Docker container based on the "hellofresh" service defined in the docker-compose.yml file.

### Check output

You can see the difficulty level data in data/output/ folder of the project

   
## Troubleshoot

You can find application logs inside ./logs/etl_pipeline.log file. This file also contains error logs that you can use for debugging purpose.


## Assumptions/considerations
- If Cooking and Perpare time is null, we don't need to consider them as a valid input so removed that from processing. This will help to get precise output and reduced unnecessary processing
- I assumed cooking and prepare data will be in Hours and Minutes only. Based on that assumption I did partitioning on cooking and prepare time because if we look for distinct probabilities considering hours and minutes (ignoring seconds), the number of possibilities is just 1,440 (24 hours * 60 minutes) since we are not counting individual seconds. Each of these 1,440 possibilities represents a unique time in a 24-hour format.


     
## Other points

### Extensibility

Used the Template Design Pattern which can enhance the extensibility of the code and make it more flexible for future additions and modifications. By following this pattern, I have created a skeleton or structure of the algorithm in a base class called ETL_Abstract and provided abstract methods that can be used to create pipeline like food_difficulty_pipeline.py 
   
### How to scale application

- Dynamic Resource Allocation: Enable dynamic resource allocation in Spark to allow executors to be added or removed automatically based on workload. This helps in optimizing resource utilization and improving application scalability.
-  Data Partitioning and Replication: Utilize data partitioning techniques such as repartitioning, bucketing, or range partitioning to evenly distribute data across nodes. This ensures that work is distributed efficiently among executors.
-  Caching and Persistence:Use Spark's caching and persistence capabilities to store intermediate data in memory or on disk. This helps avoid unnecessary recomputation and improves performance, especially in iterative algorithms.
- Broadcasting Variables: Use Spark's broadcasting feature to efficiently distribute read-only data to all nodes. Broadcasting helps reduce data transfer overhead, which can be critical for large-scale applications.
- Data Compression: Utilize data compression techniques like Snappy, Gzip, or Parquet to reduce storage and network overhead when dealing with large datasets.
- Pipeline Optimization: Optimize data processing pipelines by reordering and combining operations, reducing unnecessary transformations, and minimizing data movement.


## CI/CD

   - On PR creation:

     - Perform code quality check like using Pylint
     - Run unit test cases and check code coverage
     - Build application and create image
     - Push docker image to HelloFresh docker hub
   - On Deployment:
     - We can use docker compose to manage/run the application
       CMD: docker-compose run <Image name>
	 
### Scheduling of job
To schedule a PySpark ETL application to run periodically, you can use a job scheduling system or a workflow management tool. There are several options available, and the choice depends on your infrastructure and requirements. 

- Apache Airflow: Apache Airflow is an open-source workflow management platform. It allows you to define your ETL pipelines as Directed Acyclic Graphs (DAGs) and schedule them to run at specific intervals or on a cron-like schedule. Airflow provides a rich set of features, including monitoring, alerting, and dependency management, making it an excellent choice for complex ETL workflows.
- AWS Data Pipeline: If you are using Amazon Web Services (AWS), AWS Data Pipeline provides an option to schedule and automate the execution of ETL jobs.You can define a pipeline with a schedule that specifies when your PySpark ETL job should run.
