package org.venice.piazza.servicecontroller.util;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
/**
 * Purpose of this class is to add CORS support.  This class enables 
 * @author mlynum
 * @version 1.0
 *
 */

@Configuration
@EnableWebMvc
public class CoreWebConfig extends WebMvcConfigurerAdapter {

	@Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(new CoreCorsInterceptor());
    }
}