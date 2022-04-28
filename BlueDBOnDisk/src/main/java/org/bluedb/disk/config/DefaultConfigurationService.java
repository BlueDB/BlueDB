package org.bluedb.disk.config;

public class DefaultConfigurationService implements ConfigurationService {

	@Override
	public boolean shouldValidateObjects() {
		return true; //By default we validate objects. A user has to turn it off until we have more confidence that it isn't needed.
	}

}
