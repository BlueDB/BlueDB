package io.bluedb.api.keys;

public final class StringKey extends HashGroupedKey<String> {
	private static final long serialVersionUID = 1L;

	private final String id;

	public StringKey(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}
}
