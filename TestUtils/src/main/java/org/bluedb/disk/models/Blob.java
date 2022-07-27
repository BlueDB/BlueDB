package org.bluedb.disk.models;

public class Blob {
	private String key;
	private String label;
	private byte[] bytes;
	
	public Blob(String key, String label, byte[] bytes) {
		this.key = key;
		this.label = label;
		this.bytes = bytes;
	}

	public String getKey() {
		return key;
	}
	
	public String getLabel() {
		return label;
	}
	
	public byte[] getBytes() {
		return bytes;
	}
}
