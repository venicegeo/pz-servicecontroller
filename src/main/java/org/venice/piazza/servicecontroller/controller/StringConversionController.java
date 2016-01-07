package org.venice.piazza.servicecontroller.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@RestController
/**
 * Converts a String to Upper Case.
 *
 * @param stringToConvert The string to convert to upper case
 * @return The converted upper case string.
 */
@Api(value="servicecontroller", description="other items")
@RequestMapping(value = "/servicecontroller")
public class StringConversionController {

	@ApiOperation(value = "Convert the string")
	@RequestMapping(value = "/servicecontroller/convert", method = RequestMethod.POST)
	@ResponseBody
	String convertToUpper(@RequestBody String aString) {

		return aString.toUpperCase();

	}

	
}
