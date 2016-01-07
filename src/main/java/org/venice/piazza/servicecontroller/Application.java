package org.venice.piazza.servicecontroller;

import java.util.Arrays;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.web.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.venice.piazza.servicecontroller.data.mongodb.repository.ServiceRepository;

/* Enable Boot application and MongoRepositories */

@SpringBootApplication
@EnableMongoRepositories("org.venice.piazza.serviceregistry.data.mongodb.repository")
public class Application extends SpringBootServletInitializer {


	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		
		return builder.sources(Application.class);
	}

	public static void main(String[] args) {

		ApplicationContext ctx = SpringApplication.run(Application.class, args);
		
		// now check to see if the first parameter is true, if so then test the health of the
		// Spring environment
		if (args.length == 1) {
			Boolean inspectBool = Boolean.valueOf(args[0]);
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


