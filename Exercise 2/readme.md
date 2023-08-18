#These are the details for the following file:

##requirements.txt: This file contains the Python dependencies required for the project.
##env_variables.env: This file should contain your AWS access key and secret access key, required for interacting with Amazon S3.
##Dockerfile: The Dockerfile for building the Docker image.

#How to execute the docker file using command line:

Use the follwing steps to build and run the docker file:
docker build -t file_name . docker build -t file_name . 
docker run --env-file env_variables.env file_name python main.py --year year --city city
