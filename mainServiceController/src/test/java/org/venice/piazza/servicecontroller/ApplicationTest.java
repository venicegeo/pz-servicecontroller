package org.venice.piazza.servicecontroller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
@PowerMockIgnore("javax.management.*")
@RunWith(PowerMockRunner.class)
public class ApplicationTest {
	@Mock
	private ConfigurableApplicationContext applicationContextMock;
	@Mock
	private SpringApplication springApplicationMock;
	@Before
    public void setup() {
		MockitoAnnotations.initMocks(this);
	}
	
	
	/**
	 * Purpose of this test is to test the the main application with no arguments
	 */
	@Test
	public void testMainNoArguments() {
		String[] args = new String[0];
		PowerMockito.when(springApplicationMock.run()).thenReturn(applicationContextMock);
		//Application.main(args);
		
	}
	
	/**
	 * Purpose of this test is to test the the main application with no arguments
	 */
	@Test
	public void testMainWithOneArgument() {
		PowerMockito.when(springApplicationMock.run()).thenReturn(applicationContextMock);
		String[] args = new String[1];
		args[0] = "true";
		//Application.main(args);
		
	}
}
