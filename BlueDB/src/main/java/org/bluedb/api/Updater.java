package org.bluedb.api;

import java.io.Serializable;

@FunctionalInterface
public interface Updater<V extends Serializable> {
	public void update(V object);
}
