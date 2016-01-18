package org.venice.piazza.servicecontroller.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.venice.piazza.servicecontroller.data.model.Message;



@RestController
/**
 * Service that performs String operations.  This class is to support testing of the
 * service controller.
 * 
 * @author mlynum
 */

@RequestMapping("/string")
public class StringConversionController {

	private final static Logger LOGGER = LoggerFactory.getLogger(StringConversionController.class);

	/**
	 * Rest call to convert a string to upper case
	 * Access 
	 * http://localhost:8080/string/toUpper?aString=<a string> 
	 * 
	 * @param aString
	 * @return JSON {result:<the converted string>}
	 */	 
	@RequestMapping(value = "/toUpper", method = RequestMethod.POST, produces=MediaType.APPLICATION_JSON_VALUE)
	@ResponseBody
    public String convertStringtoUpper(@ModelAttribute("aString") String aString ) {
        LOGGER.info("Make the String uppercase" + aString);
 
        return "{\"result\":" + aString.toUpperCase() + "}";
    }
	

	
	/**
	 * Rest call to convert a string in a message to upper or lower case
	 * Access http://localhost:8080/string/convert
	 * Accepts JSON: 
	 * { 
       "theString":"<a string>",
       "conversionType":"LOWER" // UPPER or LOWER
     * }
	 * @param msg
	 * @return JSON {result:<the converted string>}
	 */
	@RequestMapping(value = "/convert", method = RequestMethod.POST, headers="Accept=application/json")
	public @ResponseBody String convert(@RequestBody Message msg) {
		
		String result = "Could not Convert, please check message";
		String converstionType = msg.getConversionType();
		String theString = msg.gettheString();
		if (converstionType.equals(Message.UPPER))  {
			LOGGER.info("Make the String uppercase" + theString);
			LOGGER.info("The message" + msg);
	        result=convertStringtoUpper(theString);
		} 
		else if (converstionType.equals(Message.LOWER))  {
			LOGGER.info("Make the String lower case" + theString);
			result=convertStringtoLower(theString);
	       
		}
		
		return result;

		
	}
	/**
	 * Rest call to convert a string to upper case
	 * Access 
	 * http://localhost:8080/string/toLower?aString=<a string> 
	 * 
	 * @param aString
	 * @return JSON {result:<the converted string>}
	 */	 
	@RequestMapping(value = "/toLower", method = RequestMethod.POST)
	@ResponseBody
    public String convertStringtoLower(@ModelAttribute("aString") String aString) {
        LOGGER.info("Make the String uppercase" + aString);
 
        return "{\"result\":" + aString.toLowerCase() + "}";
    }
	
	
	

	
}
