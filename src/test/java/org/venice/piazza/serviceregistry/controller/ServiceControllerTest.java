package org.venice.piazza.serviceregistry.controller;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.venice.piazza.servicecontroller.controller.ServiceController;
import org.venice.piazza.servicecontroller.data.mongodb.repository.ServiceRepository;
import org.venice.piazza.servicecontroller.model.Service;
import org.venice.piazza.servicecontroller.services.ServiceRegistryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
@EnableMongoRepositories
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = "classpath*:spring/application.properties")
public class ServiceControllerTest {
	
	List<Service> services;
	@Autowired
	private ServiceRegistryService srs;
	//private ServiceRepository serviceRepo;

	@Test
	public void test() {
		fail("Not yet implemented");
	}
	
	@Test
	public void testSave() {
		
		
		//services = new ArrayList<Service>();
		//Service testService = new Service();
		//testService.setName("TestService");
		//serviceRepo.save(testService);
	    //services = serviceRepo.findAll();
	    //assertEquals("there should be one service", 1, services.size());	
		
	}

}
