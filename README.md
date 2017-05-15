The ServiceController is a Spring Boot application and can be run directly from the command line. It uses an _**application.properties**_ file which contains port, hostname, database name and other information used by the ServiceController.  When the Piazza Discover Service is not available, the Service Controller utilizes information in the application.properties file to connect to supporting services.

_**application.properties**_ files are located in the mainServiceController/conf directory.  To run the ServiceController, locally, you have to choose _**application-local.properties**_ and place it in the location that the ServiceController is started from, in the _src/main/resources_ directory or in the classpath used by the ServiceController.

When the pz-discover service is running and the location/api is specified in the application.properties file, the ServiceController uses the values registered in the Discover service to communicate with things such as Kafka, MongoDB and other Piazza Core Services.


To run the ServiceController from the main directory, run the following command:

    > mvn clean install -U spring-boot:run

This will run the ServiceController, after initializing, the following message will be displayed:

    o.v.p.servicecontroller.Application : Started Application in 8.994 seconds (JVM running for 9.658)
