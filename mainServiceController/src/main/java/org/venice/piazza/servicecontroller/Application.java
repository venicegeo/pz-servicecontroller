package org.venice.piazza.servicecontroller;
// TODO add license
import java.util.Arrays;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
/**
 * Main class for the pz-servicecontroller.  Launches the application
 * @author mlynum
 * @since 1.0
 */

@SpringBootApplication
@EnableConfigurationProperties
@EnableMongoRepositories("org.venice.piazza.serviceregistry.data.mongodb.repository")
/* Enable Boot application and MongoRepositories */
public class Application extends SpringBootServletInitializer {


	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		
		return builder.sources(Application.class);
	}

	public static void main(String[] args) {

		ApplicationContext ctx = SpringApplication.run(Application.class, args);
		
		// now check to see if the first parameter is true, if so then test the health of the
		// Spring environment
		if (args.length == 2) {
			
			
			String regServiceHost = args[0];
			//TODO Call this service to obtain information on the location of
			//TODO Zookeeper, Kafka, MongoDB, UUID Generation, etc.
			Boolean inspectBool = Boolean.valueOf(args[1]);
			if (inspectBool.booleanValue() == true) {
				inspectSprintEnv(ctx);
			}
		}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               

	
	}
	
	/**
	 * Determines if the appropriate bean defintions are available
	 */
	public static void inspectSprintEnv(ApplicationContext ctx) {
		
		System.out.println("Spring Boot Beans");
		System.out.println("-----------------");

        String[] beanNames = ctx.getBeanDefinitionNames();
        Arrays.sort(beanNames);
        for (String beanName : beanNames) {
            System.out.println(beanName);
        }
		
	}
}


