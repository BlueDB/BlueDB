package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.api.ReadableBlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.ReadableBlueDbOnDisk;
import org.bluedb.disk.query.ReadOnlyBlueTimeQueryOnDisk;

public class ReadOnlyBlueTimeCollectionOnDisk<T extends Serializable> extends ReadOnlyBlueCollectionOnDisk<T> implements ReadableBlueTimeCollection<T> {

	public ReadOnlyBlueTimeCollectionOnDisk(ReadableBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses);
	}

	@Override
	public ReadBlueTimeQuery<T> query() {
		return new ReadOnlyBlueTimeQueryOnDisk<T>(this);
	}

}
