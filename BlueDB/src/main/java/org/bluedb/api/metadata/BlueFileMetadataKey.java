package org.bluedb.api.metadata;

public enum BlueFileMetadataKey {

	ENCRYPTION_VERSION_KEY(String.class);
	
	private final Class<?> valueClass;
	
	BlueFileMetadataKey(Class<?> valueClass) {
		this.valueClass = valueClass;
	}

	public Class<?> getValueClass() {
		return valueClass;
	}
}