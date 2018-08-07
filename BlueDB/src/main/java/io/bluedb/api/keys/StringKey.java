package io.bluedb.api.keys;

public final class StringKey extends HashGroupedKey {
	private static final long serialVersionUID = 1L;

	private String id;

	public StringKey(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		StringKey other = (StringKey) obj;
		if (id == null) {
			return other.id == null;
		} else if (!id.equals(other.id)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "StringKey [key=" + id + "]";
	}

	@Override
	public int compareTo(BlueKey other) {
		if(other == null) {
			return -1;
		}
		
		if (other instanceof StringKey) {
			StringKey stringKey = (StringKey) other;
			return id.compareTo(stringKey.id);
		}
		
		return getClass().getSimpleName().compareTo(other.getClass().getSimpleName());
	}
}
