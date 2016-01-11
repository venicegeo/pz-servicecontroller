package org.venice.piazza.servicecontroller.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.support.SessionStatus;
import org.springframework.web.util.UriComponentsBuilder;
import org.venice.piazza.servicecontroller.data.model.Message;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@RestController
/**
 * Service that performs String operations.
 * 
 * @author mlynum
 */

@RequestMapping("/string")
public class StringConversionController {

	
	 
	@RequestMapping(value = "/toUpper", method = RequestMethod.POST)
	@ResponseBody
    public String convertStringtoUpper(@ModelAttribute("aString") String aString) {
        System.out.println("Make the String uppercase" + aString);
 
        return "{\"result\":" + aString.toUpperCase() + "}";
    }
	
	@RequestMapping(value = "/convert", method = RequestMethod.POST, headers="Accept=application/json")
	public String convert(@PathVariable String theString, @PathVariable String conversionType) {
		String result = "Could not Convert, please check message";
		if (conversionType.equals(Message.UPPER))  {
			System.out.println("Make the String uppercase" + theString);
	        result=convertStringtoUpper(theString);
		} 
		else if (conversionType.equals(Message.LOWER))  {
			System.out.println("Make the String lower case" + theString);
			result=convertStringtoLower(theString);
	       
		}
		
		return result;

		
	}
	
	@RequestMapping(value = "/toLower", method = RequestMethod.POST)
	@ResponseBody
    public String convertStringtoLower(@ModelAttribute("aString") String aString) {
        System.out.println("Make the String uppercase" + aString);
 
        return "{\"result\":" + aString.toLowerCase() + "}";
    }
	
	
	

	
}
