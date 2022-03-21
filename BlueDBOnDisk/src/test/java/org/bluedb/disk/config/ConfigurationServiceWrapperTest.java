package org.bluedb.disk.config;

import static org.junit.Assert.*;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.bluedb.disk.time.TimeService;
import org.junit.Test;

public class ConfigurationServiceWrapperTest {

	@Test
	public void test() {
		final long start = System.currentTimeMillis();
		final long half_minute = 30_000;
		final long one_minute = 60_000;
		
		ConfigurationService mockedConfigurationService = mock(ConfigurationService.class);
		doReturn(false).when(mockedConfigurationService).shouldValidateObjects();
		
		TimeService mockedTimeService = mock(TimeService.class);
		doReturn(start).when(mockedTimeService).getCurrentTime();
		
		ConfigurationService wrapperService = new ConfigurationServiceWrapper(mockedConfigurationService, mockedTimeService, one_minute);

		//If the service starts returning true and we haven't checked it yet then the first call should return true.
		doReturn(true).when(mockedConfigurationService).shouldValidateObjects();
		assertTrue(wrapperService.shouldValidateObjects());
		verify(mockedConfigurationService, times(1)).shouldValidateObjects();

		//The service can start returning false, but it won't check again until one minute has passed.
		doReturn(false).when(mockedConfigurationService).shouldValidateObjects();
		assertTrue(wrapperService.shouldValidateObjects());
		verify(mockedConfigurationService, times(1)).shouldValidateObjects();
		
		//Adding half a minute won't change anything. It'll still return true and won't call the method again
		doReturn(start + half_minute).when(mockedTimeService).getCurrentTime();
		assertTrue(wrapperService.shouldValidateObjects());
		verify(mockedConfigurationService, times(1)).shouldValidateObjects();
		
		//Setting the time to one minute after stop should result in checking again and noticing that its now false
		doReturn(start + one_minute).when(mockedTimeService).getCurrentTime();
		assertFalse(wrapperService.shouldValidateObjects());
		verify(mockedConfigurationService, times(2)).shouldValidateObjects();

		//Advancing 5 minutes and setting it back to true should result in another check and returning true
		doReturn(true).when(mockedConfigurationService).shouldValidateObjects();
		doReturn(start + (one_minute * 5)).when(mockedTimeService).getCurrentTime();
		assertTrue(wrapperService.shouldValidateObjects());
		verify(mockedConfigurationService, times(3)).shouldValidateObjects();
	}

}
