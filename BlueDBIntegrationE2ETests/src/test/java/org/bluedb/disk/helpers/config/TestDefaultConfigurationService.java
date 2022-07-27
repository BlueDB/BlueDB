package org.bluedb.disk.helpers.config;

import org.bluedb.disk.config.ConfigurationService;

/**
 * Most tests should use this so that there can't be any serialization errors in order for the tests to pass.
 */
public class TestDefaultConfigurationService implements ConfigurationService {

	@Override
	public boolean shouldValidateObjects() {
		return false;
	}

}
