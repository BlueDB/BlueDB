package org.bluedb.api;

import java.io.Serializable;

@FunctionalInterface
public interface Condition<V extends Serializable> {
	public boolean test(V object);
}
