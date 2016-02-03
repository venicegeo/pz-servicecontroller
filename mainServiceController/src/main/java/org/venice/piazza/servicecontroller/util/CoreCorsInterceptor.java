package org.venice.piazza.servicecontroller.util;


import java.util.Arrays;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

public class CoreCorsInterceptor extends HandlerInterceptorAdapter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(CoreCorsInterceptor.class);

    public static final String REQUEST_ORIGIN_NAME = "Origin";

    public static final String CREDENTIALS_NAME = "Access-Control-Allow-Credentials";
    public static final String ORIGIN_NAME = "Access-Control-Allow-Origin";
    public static final String METHODS_NAME = "Access-Control-Allow-Methods";
    public static final String HEADERS_NAME = "Access-Control-Allow-Headers";
    public static final String MAX_AGE_NAME = "Access-Control-Max-Age";

 

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        response.setHeader(METHODS_NAME, "GET, OPTIONS, POST, PUT, DELETE");
        response.setHeader(HEADERS_NAME, "Origin, X-Requested-With, Content-Type, Accept");
        response.setHeader(MAX_AGE_NAME, "3600");
        
        String origin = request.getHeader(REQUEST_ORIGIN_NAME);
        if (origin != null) {
        	// TODO Need to check the origin to see where the request is coming from
        	// TODO Need to determine what is valid
            response.setHeader(ORIGIN_NAME, origin);
            return true; // Proceed
        } else {
            LOGGER.warn("Attempted access from non-allowed origin: {}", origin);
            // Include an origin to provide a clear browser error
            response.setHeader(ORIGIN_NAME, origin);
            return false; // No need to find handler
        }
    }

}