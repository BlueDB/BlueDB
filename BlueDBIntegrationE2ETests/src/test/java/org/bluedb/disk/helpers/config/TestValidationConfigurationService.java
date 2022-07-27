package org.bluedb.disk.helpers.config;

import org.bluedb.disk.config.ConfigurationService;

/**
 * Used by tests that want object validation turned on
 */
public class TestValidationConfigurationService implements ConfigurationService {

	@Override
	public boolean shouldValidateObjects() {
		return true;
	}

}
