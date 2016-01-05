package org.venice.piazza.serviceregistry.controller;

import org.springframework.web.bind.annotation.RestController;
import org.venice.piazza.serviceregistry.model.Service;
import org.venice.piazza.serviceregistry.data.mongodb.repository.ServiceRepository;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;

@RestController
public class ServiceController {

    
	
	@RequestMapping("/")
    public String index() {
        return "Piazza Service Registry";
    }
	
	
	
}
