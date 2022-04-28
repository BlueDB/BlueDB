package org.bluedb.disk.config;

public interface ConfigurationService {
	
	/** 
	 * This is how BlueDB will ask if it should validate objects before returning them in query results or persisting
	 * them to disk. We introduced object validation as a work around in order to avoid causing errors and/or corrupt
	 * BlueDB data when FST fails to convert bytes to an object correctly. Or potentially if BlueDB is given an object
	 * that already has some sort of problem.
	 * 
	 * This method will be called often in case the answer changes so so don't take a long time in the implementation. 
	 * However, BlueDB will limit how often it asks rather than asking before every single FST operation. The implementer
	 * can choose whether to cache the value or look it up.
	 * 
	 * We think we've identified the issue and patched FST, but we want to proceed forward with caution. By default,
	 * for now, object validation will be turned on. By supplying your own implementation of this service you can
	 * turn it off in order to get performance benefits. We will be testing BlueDB without object validation
	 * to ensure that there are no additional issues with FST. When we are confident then we plan on turning
	 * off object validation by default.
	 * 
	 * @return false if BlueDB should not expend resources to verify objects are valid before reading or writing
	 * to/from disk. This will be much faster, but it will assume objects given to it are valid and that FST
	 * can be trusted not to make mistakes. If really old data from before object validation is persisted
	 * in BlueDB, it could potentially have corrupt objects that can no longer be identified without object
	 * validation.
	 */
	public boolean shouldValidateObjects();
}
