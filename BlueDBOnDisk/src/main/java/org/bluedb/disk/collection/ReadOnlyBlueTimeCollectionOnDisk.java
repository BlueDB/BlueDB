package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.ReadOnlyBlueTimeCollection;
import org.bluedb.api.ReadOnlyBlueTimeQuery;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.ReadOnlyBlueDbOnDisk;
import org.bluedb.disk.query.ReadOnlyBlueTimeQueryOnDisk;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class ReadOnlyBlueTimeCollectionOnDisk<T extends Serializable> extends ReadOnlyBlueCollectionOnDisk<T> implements ReadOnlyBlueTimeCollection<T> {

	public ReadOnlyBlueTimeCollectionOnDisk(ReadOnlyBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
	}

	public ReadOnlyBlueTimeCollectionOnDisk(ReadOnlyBlueDbOnDisk db, String name, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses) throws BlueDbException {
		super(db, name, requestedKeyType, valueType, additionalRegisteredClasses);
	}

	@Override
	public ReadOnlyBlueTimeQuery<T> query() {
		return new ReadOnlyBlueTimeQueryOnDisk<T>(this);
	}

}
