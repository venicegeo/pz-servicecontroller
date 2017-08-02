The ServiceController is a Spring Boot application and can be run directly from the command line. It uses an _**application.properties**_ file which contains port, hostname, database name and other information used by the ServiceController.

To run the ServiceController from the main directory, run the following command:
`$ mvn clean install -U spring-boot:run`

To build and run this project, software such as Kafka and PostgreSQL is required.  For details on these prerequisites, refer to the
[Piazza Developer's Guide](https://pz-docs.geointservices.io/devguide/index.html#_piazza_core_overview).

This will run the ServiceController, after initializing, the following message will be displayed:

    o.v.p.servicecontroller.Application : Started Application in 8.994 seconds (JVM running for 9.658)
