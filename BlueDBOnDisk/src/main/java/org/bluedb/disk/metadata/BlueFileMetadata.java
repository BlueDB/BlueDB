package org.bluedb.disk.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class BlueFileMetadata implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private final Map<BlueFileMetadataKey, String> metadataMap;

	public BlueFileMetadata() {
		metadataMap = new HashMap<>();
	}

	public Optional<String> get(BlueFileMetadataKey key) {
		return Optional.ofNullable(metadataMap.get(key));
	}
	
	public boolean isTrue(BlueFileMetadataKey key) {
		return Boolean.valueOf(metadataMap.get(key));
	}

	public String put(BlueFileMetadataKey key, String value) {
		if (key == null || value == null) {
			throw new IllegalArgumentException("Key and value must not be null");
		}
		return metadataMap.put(key, value);
	}

	public boolean containsKey(BlueFileMetadataKey key) {
		return metadataMap.containsKey(key);
	}

	public String remove(BlueFileMetadataKey key) {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		return metadataMap.remove(key);
	}

}
