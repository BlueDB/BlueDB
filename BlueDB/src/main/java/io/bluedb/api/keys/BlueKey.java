package io.bluedb.api.keys;

import java.io.Serializable;

public interface BlueKey extends Serializable {
	@Override
	public abstract int hashCode();

	@Override
	public abstract boolean equals(Object object);
}
