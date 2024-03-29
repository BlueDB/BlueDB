package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.ReadBlueTimeQuery;
import org.bluedb.api.ReadableBlueTimeCollection;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.ReadableDbOnDisk;
import org.bluedb.disk.query.ReadOnlyTimeQueryOnDisk;

public class ReadOnlyTimeCollectionOnDisk<T extends Serializable> extends ReadOnlyCollectionOnDisk<T> implements ReadableBlueTimeCollection<T> {

	public ReadOnlyTimeCollectionOnDisk(ReadableDbOnDisk db, String name, BlueCollectionVersion requestedVersion, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		super(db, name, requestedVersion, requestedKeyType, valueType, additionalRegisteredClasses);
	}

	@Override
	public ReadBlueTimeQuery<T> query() {
		return new ReadOnlyTimeQueryOnDisk<T>(this);
	}

}
