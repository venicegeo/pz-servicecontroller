package org.venice.piazza.servicecontroller.controller;

import org.springframework.web.bind.annotation.RestController;
import org.venice.piazza.servicecontroller.data.mongodb.repository.ServiceRepository;
import org.venice.piazza.servicecontroller.model.Service;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;

@RestController
public class ServiceController {

    
	
	@RequestMapping("/servicecontroller/index")
    public String index() {
        return "Piazza Service Controller";
    }
	
	
	
}
