# Spark on Kubernetes, POC developed with Spark local cluster

This is development repo for Spark deployments on Kubernetes. 

First part of project is setting up Spark cluster locally. 

## Solution Overview  

Entire solution has been containarized using dockers, docker-compose for spinning up containers, defining dependencies and setting up a Spark cluster locally with one Master node and 2 worker nodes. 

Solution is developed in 2 phases: 


###  1st phase will be setting up a Spark local cluster and running ETL pipelines in local cluster.

```sh build.sh``` command will created images for development environment creating required Images for master and workers in the cluster. 
Once images are build locally, ```docker-compose.yml``` will launch development environments, defining dependencies as separate containers. 



###  2nd phase will be deploying to a Kubernetes cluster. (Work In Progress)

For development purpose, we will be working with Mini kube deployments. In production env, it can be EKS or GKE cluster. 
For kubernetes deployments, will be setting up with spark-submit operator and Spark Operator, from Google 



### Steps to setup up the environment locally : 
Git clone the repo to setup the environment and run data processing pipeline in Airflow 

```
   git clone https://github.com/shahin43/airflow_etl_pipeline.git  

```

Build the environment 
``` 
 sh build.sh 
``` 

And then, docker-compose for spinning up containers 
``` 
  docker-compose up -d
``` 


Once containers are up Airflow, Flower and Spark cluster can be accessed as below from host machine as :   

  - Spark cluster UI, access using - http://localhost:8080/
