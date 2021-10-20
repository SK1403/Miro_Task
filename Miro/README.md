# Miro Data Engineering Assingment Task



### TASK 1 - Parse Mode

---------------------------------------------------------------------------------------------------------------------------------------

We need to parse mode only selects the app_loaded and registered events, and saves them on a disk in the parquet format

---------------------------------------------------------------------------------------------------------------------------------------

# Getting Started

This repository contains PySpark based solution of Miro Data Engineers Test task, which is part of recruitment process at Miro and is designed to assess key competencies required for working as data engineer at Miro.

# Project Structure

The basic project structure is as follows:

    Miro/  
     |   conf/
     |   |   |-- log4j.properties
     |   config/
     |   |   |-- config.ini
     |   project_utils/
     |   |   |-- __init__.py
     |   |   |-- log4j_root_logger.py
     |   |   |-- utils.py
     |   jobs/
     |   |   |-- __init__.py
     |   |   |-- miro_elt_entrypoint.py
     |   |   |-- miro_elt_Jobs.py
     |   |   |-- miro_elt_main.py
     |   tests/
     |   |   |-- __init__.py
     |   |   |-- test_dataset.json
     |   |   |-- test_miro_elt_Jobs.py
     |   |   |-- test_miro_utils.py
     |   target/
     |   |   |-- miro_elt_entrypoint.py
     |   |   |-- miro_elt_Jobs.py
     |   |   |-- miro_elt_main.py
     |   |   |-- project.zip
     |   logs/
     |   |   |-- miro_elt_Job_logger.log
     |   input/
     |   |   |-- dataset.json
     |   output/
     |   |   |   app_loader/
     |   |   |   |-- part-00000-*.parquet
     |   |   |   registered/
     |   |   |   |-- part-00000-*..parquet
     |   AirflowDAG.py
     |   Dockerfile
     |   Makefile
     |   README.md
     |   Requirements.txt

The main Python module containing the ELT job (which will be sent to the Spark cluster), is Miro/jobs/miro_elt_*.py. Any external configuration parameters required by miro_elt_*.py are stored in configs/spark_config.ini. Additional modules that support this job can be kept in the project_utils folder. In the project Unit test modules are kept in the tests folder. Sample input and output data are kept in respective input and output folder. There exists logs folder for capturing Log4j loggers log and conf folder for specifying configuration properties for lo4j logger. There is a target folder to package and place project.zip, All remaining files required for job Orchestration & execution are placed at root folder level along with README.md

# How to run the script ?

We can execute and run the job locally or on cluster. In either of the scenario before running the application we need to set up your own virtual environment with packages mentioned in requirements.txt file which required for running the our application

# Setting up the virtualenv:-
    Load up virtual environment before running the script by loading the activate file in the bin directory as shown below.
    
    1> python3 -m pip install --user virtualenv
    2> python3 -m virtualenv ~/miro_venv
    3> source ~/miro_venv/bin/activate
    4> pip install -r requirements.txt

# Running Locally

There are two options available for locally running and testing the application:-

    1> Simplest and easiest way to execute the code locally is, import project locally and run in IDE i.e. PyCharm, VisualStudioCode, etc.
    2> SSH on to python enabled local server or terminal, then Import project and switch to project root folder then 
    
    Assuming that the $SPARK_HOME environment variable points to your production Spark folder, 
    the ELT job can be triggered from the project's root directory using the following command from the terminal.
    
    $SPARK_HOME/bin/spark-submit \
    --master local[*] \
    --deploy-mode local \
    --py-files jobs.zip \
    jobs/miro_elt_entrypoint.py \
    --job miro_elt_Job
    
    NOTE : In case what to RUN the job in background mode and monitor the flow then prefix 'nohup' & suffix '&' in the above command
    
    # Alternative Approach: 
    
    1> We can create a Makefile to create pakage.zip and place it under target folder, configure steps to setup virtual environment, run the job, clean the cache, perform flake liniting check following PEP8 conventions, activate & destroy virtual environment. Attached is simple Makefile for packaging the project with dependencies 
    Incase of using Makefile modify the above command by : 1> make build  
                                                           2> cd target
                                                           3> $SPARK_HOME/bin/spark-submit \
                                                               --py-files jobs.zip \
                                                               jobs/miro_elt_entrypoint.py \
                                                               --job miro_elt_Job
    2> We can create docker image with pyspark 3.0 and hadoop 3.2 attached is sample Dockerfile.
    Incase of using Dockerfile follow below listed steps : 1> docker build -t .(build docker image using Dockerfile)
                                                           2> docker images (copy image name)
                                                           3> docker container create image_name desired_container_name (create container from docker image)
                                                           4> docker run --rm -it --link master:master --volumes-from spark-datastore $SPARK_HOME/bin spark-submit --master spark://192.168.0.0:4044 jobs/miro_elt_entrypoint.py



# Running On CLuster
SSH to the one of the cluster instances and clone the repository to your desired location. Then follow the below mentioned steps
    1> Switch to project root folder and create own virtual environment following step-2 described above for running locally 
    2> Assuming that the $SPARK_HOME environment variable points to your production Spark folder, then the ELT job can be run from the project's root directory using the following command from the terminal.
    
    $SPARK_HOME/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --py-files jobs.zip \
    jobs/miro_elt_entrypoint.py \
    --job miro_elt_Job

    NOTE : In case what to RUN the job in background mode and monitor the flow then prefix 'nohup' & suffix '&' in the above command
    
    # Alternative Approach: 
    
    1> We can create a Makefile to create pakage.zip and place it under target folder, configure steps to setup virtual environment, run the job, clean the cache, perform flake limiting check following PEP8 conventions, activate & destroy virtual environment. Attached is simple Makefile for packaging the project with dependencies 
    Incase of using Makefile modify the above command by : a> make build 
                                                           b> cd target
                                                           c> $SPARK_HOME/bin/spark-submit \
                                                               --py-files jobs.zip \
                                                              jobs/miro_elt_entrypoint.py \
                                                               --job miro_elt_Job
    2> We can create docker image with pyspark 3.0 and hadoop 3.2, attached is simple Dockerfile for packing project and its dependencies.
    Incase of using Dockerfile follow below listed steps : a> docker build -t .(build docker image using Dockerfile)
                                                           b> docker images (copy image name)
                                                           c> docker container create image_name desired_container_name (create container from docker image)
                                                           d> docker run --rm -it --link master:master --volumes-from spark-datastore $SPARK_HOME/bin/spark-submit --master spark://192.168.0.0:4044 jobs/miro_elt_entrypoint.py
   
# How would you implement CI/CD for this application?

  Continous Delivery:
       Releasing the source code to the artifact
       Uploading the artifact to the particular component
  Automated Deployement:
        Automatic release to the repository, here the code is always in the deployable format
        
---------------------------------------------------------------------------------------------------------------------------------------
  Pushing the code from the staging environment to the production environment is called continuous delivery with human intervention.
  The above process will done without any human intervention is called continuous deployment.
---------------------------------------------------------------------------------------------------------------------------------------

    # Steps involved in configuring CI/CD pipeline via Jenkins:
    STEP 1-----------install jenkins
    STEP 2-----------install docker
    STEP 3-----------configure the jenkins with dockr machine as by creating user in docker after that provide the credentials inside the jenkins server in order to authenticate the machine. This user has the admin access now communicate the 2 servers then u need to open up the ssh port.
    STEP 4-----------after generating the artifact(.war) from the Jenkins dashboard go to the jenkins server and copy the artifact path and paste it inside the linux machine of jenkins, switch to that path 'cd/var/lib/jenkins/workspace/firstjob/webapp/target' to check the artifact(webapp.war) is present.
    STEP 5-----------this is .war file need to copy into the docker machine. To communicate the jenkins server to docker machine we need to install the plugin called PUBLISH OVER SSH through this (.war file will copy to the docker machine), now add the docker-host name to the jenkins configer tool then provide the IP address of our docker machine along with username.
    STEP 6-----------now configer the job ---> build ----> Post build actions---->select---->send build artifact over SSH.
    STEP 7-----------now to fill the details we need to copy the sorce file which is present in the jenkins server 
    STEP 8-----------now to provide the URL of the .war file present in the path we need to mention the path inside the configeration of jenkins job (path : var/lib/jenkins/workspace/firstjob/webapp/target)
    STEP 9-----------now provide the source and the destination (in case of remote directory we need give he . ), such that the artifact has been copied from the jenkins server directory to the docker machine directory
    STEP10-----------finally Save and Apply
    STEP10-----------trigger jenkins build and montior console output
    STEP11-----------once build completes successfully will observe that jenkins has copied this artifactory inside docker home directory                     
    STEP12-----------go to the docker machine and home dirctory and check that file had been copied. Now this file need to be copied inside the docker container to test the application 
    STEP13-----------write the docker file referring the one in the project
    STEP14-----------save the file and build docker image out of it
                     ---docker build -t dockerimage .
                     now image has been succesfully build
    STEP15-----------now create and launch a container from the image.
                     ---docker run -d --name dockercontainer -p 8080:8080 dockerimage
                     container has been succesfully started
    
# How would you schedule this pipeline to run periodically?

There are multiple option i.e. cron(shell scripting), jenkins, celery, oozie (CDH i.e. onprem) through which I can schedule the pipeline to trigger and 
RUN the ELT job, out of all my favourite is Airflow. Here are the steps to schedule ELT data pipeline to RUN periodically.
    1> Write an airflow DAG, with desired job run configuration i.e. start_date, end_date, schedule_interval, retries, retry_delay, etc. and create appropriate task leveraging inbuilt Airflow Operator i.e. Bash, Python, Docker, etc. to perform the task i.e. set up, start, stop, backfill, etc. as done in the Miro/AirflowDAG.py 
    2> Place the airflow DAG i.e. Miro/AirflowDAG.py, into the $AIRFLOW_HOM/dags either manually or by configuring CI/CD pipeline (using jenkins, buildkite, etc.) to pick the latest GIT changes through pull request, deploy and trigger the project build 
    3> Now switch to airflow UI in the browser, you will observe that a DAG with 'miro_elt_Job_scheduler' will start appearing in case not just search for it, once DAG is located select switch its status from OFF to ON, then start the job and monitor DAG tree view, graph view, see runs details and logs per tasks, etc. 


### TASK 2 - Statistics Mode
---------------------------------------------------------------------------------------------------------------------------------------

The statistics mode computes a metric based on the app_loaded and registered events produced by the parse mode. The metric represents 
the percentage of all users who loaded the application in the calendar week immediately after their registration week.  

---------------------------------------------------------------------------------------------------------------------------------------

Above designed solution is capable enough of computing metric and printing end result on the console.


  