/*******************************************************************************
 * Copyright 2016, RadiantBlue Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package org.venice.piazza.servicecontroller;

import java.util.Arrays;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import org.springframework.web.client.RestTemplate;

/**
 * Main class for the pz-servicecontroller. Launches the application
 * 
 * @author mlynum
 * @since 1.0
 */
@SpringBootApplication
@EnableConfigurationProperties
@EnableAsync
@Configuration
@EnableAutoConfiguration
@EnableScheduling
@EnableTransactionManagement
@EnableJpaRepositories(basePackages = { "org.venice.piazza.common.hibernate" })
@EntityScan(basePackages = { "org.venice.piazza.common.hibernate" })
@ComponentScan(basePackages = { "org.venice.piazza.servicecontroller", "util", "org.venice.piazza" })
public class Application extends SpringBootServletInitializer {
	@Value("${http.max.total}")
	private int httpMaxTotal;
	@Value("${http.max.route}")
	private int httpMaxRoute;
	@Value("${http.request.timeout}")
	private int httpRequestTimeout;

	private static final Logger LOG = LoggerFactory.getLogger(Application.class);

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {

		return builder.sources(Application.class);
	}

	@Bean
	public RestTemplate restTemplate() {
		RestTemplate restTemplate = new RestTemplate();
		HttpClient httpClient = HttpClientBuilder.create().setMaxConnTotal(httpMaxTotal).setMaxConnPerRoute(httpMaxRoute).build();
		HttpComponentsClientHttpRequestFactory factory = new HttpComponentsClientHttpRequestFactory(httpClient);
		factory.setReadTimeout(httpRequestTimeout * 1000);
		factory.setConnectTimeout(httpRequestTimeout * 1000);
		restTemplate.setRequestFactory(factory);

		return restTemplate;
	}

	public static void main(String[] args) {
		ApplicationContext ctx = SpringApplication.run(Application.class, args); // NOSONAR
		// now check to see if the first parameter is true, if so then test the health of the
		// Spring environment
		if (args.length == 1) {
			// Get the value of the first argument
			// If it is true then do a health check and print it out
			if (Boolean.valueOf(args[0]) == true) {
				inspectSprintEnv(ctx);
			}
		}
	}

	/**
	 * Determines if the appropriate bean definitions are available
	 */
	public static void inspectSprintEnv(ApplicationContext ctx) {

		LOG.info("Spring Boot Beans");
		LOG.info("-----------------");

		String[] beanNames = ctx.getBeanDefinitionNames();
		Arrays.sort(beanNames);
		for (String beanName : beanNames) {
			LOG.info(beanName);
		}
	}

	@Bean
	public LocalValidatorFactoryBean getLocalValidatorFactoryBean() {
		return new LocalValidatorFactoryBean();
	}
}
