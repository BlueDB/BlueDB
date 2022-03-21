package org.bluedb.disk.config;

import org.bluedb.disk.time.TimeService;

public class ConfigurationServiceWrapper implements ConfigurationService {
	
	private final ConfigurationService service;
	private final TimeService timeService;
	private final long minTimeBetweenChecks;
	
	private boolean shouldValidateObjects;
	
	private long nextTimeToCheck = Long.MIN_VALUE;
	
	public ConfigurationServiceWrapper(ConfigurationService service, TimeService timeService, long minTimeBetweenChecks) {
		this.service = service;
		this.timeService = timeService;
		this.minTimeBetweenChecks = minTimeBetweenChecks;
	}

	@Override
	public synchronized boolean shouldValidateObjects() {
		checkIfNecessary();
		return shouldValidateObjects;
	}

	private void checkIfNecessary() {
		long now = timeService.getCurrentTime();
		if(now >= nextTimeToCheck) {
			shouldValidateObjects = service.shouldValidateObjects();
			nextTimeToCheck = now + minTimeBetweenChecks;
		}
	}
	
	public synchronized void resetNextTimeToCheck() {
		nextTimeToCheck = Long.MIN_VALUE;
	}

}
