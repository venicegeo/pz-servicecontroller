# pz-servicecontroller

The Service Controller is a Spring Boot application that handles the registration and execution of user services and algorithms. The application can be run directly from the command line and acts as a broker to external services that allows users (developers) to host their own algorithmic or spatial services directly from within Piazza. Other external users can then run these algorithms with their own data. In this way, Piazza acts as a federated search for algorithms, geocoding, or other various microservices (spatial or not) to run within a common environment. Using the Piazza Workflow component, users can create workflows that will allow them to chain events together (such as listening for when new data is loaded into Piazza) in order to create complex, automated workflows. This satisfies one of the primary goals of Piazza: Allowing users across the GEOINT Services platform to share their data and algorithms amongst the community.

***
## Requirements
Before building and running the pz-gateway project, please ensure that the following components are available and/or installed, as necessary:
- [Java](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (JDK for building/developing, otherwise JRE is fine)
- [Maven (v3 or later)](https://maven.apache.org/install.html)
- [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) (for checking out repository source)
- [RabbitMQ](https://www.rabbitmq.com/download.html)
- [PostgreSQL](https://www.postgresql.org/download)
- Access to Nexus is required to build

Ensure that the nexus url environment variable `ARTIFACT_STORAGE_URL` is set:

	$ export ARTIFACT_STORAGE_URL={Artifact Storage URL}

For additional details on prerequisites, please refer to the Piazza Developer's Guide [Core Overview](http://pz-docs.int.dev.east.paas.geointservices.io/devguide/02-pz-core/) or [Piazza Service Controller](http://pz-docs.int.dev.east.paas.geointservices.io/devguide/14-pz-servicecontroller/) sections. Also refer to the [prerequisites for using Piazza](http://pz-docs.int.dev.east.paas.geointservices.io/devguide/03-jobs/) section for additional details.

***
## Setup, Configuring & Running

### Setup

Create the directory the repository must live in, and clone the git repository:

    $ mkdir -p {PROJECT_DIR}/src/github.com/venicegeo
	$ cd {PROJECT_DIR}/src/github.com/venicegeo
    $ git clone git@github.com:venicegeo/pz-servicecontroller.git
    $ cd pz-servicecontroller

>__Note:__ In the above commands, replace {PROJECT_DIR} with the local directory path for where the project source is to be installed.

### Configuring
As noted in the Requirements section, to build and run this project, RabbitMQ and PostgreSQL are required. The ServiceController uses an **application.properties** file - within (`src/main/resources directory`) which contains port, hostname, database name and other information used by the ServiceController.

To edit the port that the service is running on, edit the `server.port` property.

### Building & Running locally

To run the ServiceController from the main directory, run the following command:

	$ mvn clean install -U spring-boot:run

This will run the ServiceController, after initializing, the following message will be displayed:

    o.v.p.servicecontroller.Application : Started Application in 8.994 seconds (JVM running for 9.658)
    
### Running Unit Tests

To run the ServiceController unit tests from the main directory, run the following command:

	$ mvn test

