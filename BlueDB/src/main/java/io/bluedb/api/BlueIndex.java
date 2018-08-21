package io.bluedb.api;

import java.io.Serializable;

public interface BlueIndex<T extends Serializable> {
	public BlueQuery<T> query();
}
