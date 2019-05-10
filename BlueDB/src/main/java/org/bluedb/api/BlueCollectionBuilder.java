package org.bluedb.api;

import java.io.Serializable;
import java.util.Collection;

import org.bluedb.api.exceptions.BlueDbException;

public interface BlueCollectionBuilder<T extends Serializable> {
	public BlueCollectionBuilder<T> withName(String name);
	public BlueCollectionBuilder<T> usingClasses(Collection<Class<? extends Serializable>> classesToRegister);
	public BlueCollection<T> build() throws BlueDbException;
}
