package org.bluedb.api.metadata;

import java.util.HashMap;

public class BlueFileMetadata extends HashMap<BlueFileMetadataKey, Object> {

	@Override
	public Object put(BlueFileMetadataKey key, Object value) {
		validatePut(key, value);
		return super.put(key, value);
	}

	@Override
	public Object putIfAbsent(BlueFileMetadataKey key, Object value) {
		validatePut(key, value);
		return super.putIfAbsent(key, value);
	}

	private void validatePut(BlueFileMetadataKey key, Object value) {
		if (key == null || value == null) {
			throw new IllegalArgumentException("Key and value must not be null");
		}
		if (key.getValueClass() != value.getClass()) {
			throw new IllegalArgumentException("Invalid type for metadata key " + key.name() + ": " + value.getClass() +
					". Type must be " + key.getValueClass());
		}
	}

}
