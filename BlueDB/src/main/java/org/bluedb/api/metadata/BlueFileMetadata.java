package org.bluedb.api.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class BlueFileMetadata implements Serializable {

	private final Map<BlueFileMetadataKey, String> metadataMap;

	public BlueFileMetadata() {
		metadataMap = new HashMap<>();
	}
	
	public String get(BlueFileMetadataKey key) {
		return metadataMap.get(key);
	}

	public Object put(BlueFileMetadataKey key, String value) {
		if (key == null || value == null) {
			throw new IllegalArgumentException("Key and value must not be null");
		}
		return metadataMap.put(key, value);
	}
	
	public boolean containsKey(BlueFileMetadataKey key) {
		return metadataMap.containsKey(key);
	}
	
	public String remove(BlueFileMetadataKey key) {
		return metadataMap.remove(key);
	}
}