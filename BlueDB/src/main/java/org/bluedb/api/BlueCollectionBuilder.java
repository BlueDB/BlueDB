package org.bluedb.api;

import java.io.Serializable;

import org.bluedb.api.exceptions.BlueDbException;

public interface BlueCollectionBuilder<T extends Serializable> {
	public BlueCollectionBuilder<T> withName(String name);
	@SuppressWarnings("unchecked")
	public BlueCollectionBuilder<T> withRegisteredClasses(Class<? extends Serializable>... additionalRegisteredClasses);
	public BlueCollection<T> build() throws BlueDbException;
}