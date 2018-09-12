package io.bluedb.api.keys;

import java.util.UUID;

public final class UUIDKey extends HashGroupedKey {
	private static final long serialVersionUID = 1L;

	private final UUID id;

	public UUIDKey(UUID id) {
		this.id = id;
	}

	public UUID getId() {
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
		UUIDKey other = (UUIDKey) obj;
		if (id == null) {
			return other.id == null;
		} else if (!id.equals(other.id)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return "UUIDKey [key=" + id + "]";
	}

	@Override
	public int compareTo(BlueKey other) {
		if(other == null) {
			return -1;
		}
		
		if (other instanceof UUIDKey) {
			UUIDKey stringKey = (UUIDKey) other;
			return id.compareTo(stringKey.id);
		}
		
		return getClass().getSimpleName().compareTo(other.getClass().getSimpleName());
	}
}
