package org.bluedb.api;

import java.io.Serializable;

@FunctionalInterface
public interface Mapper<V extends Serializable> {
	public V update(V object);
}
