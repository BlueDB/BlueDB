package org.bluedb.disk.collection;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.BlueCollectionVersion;
import org.bluedb.api.BlueTimeCollection;
import org.bluedb.api.BlueTimeQuery;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.disk.ReadWriteDbOnDisk;
import org.bluedb.disk.query.TimeQueryOnDisk;
import org.bluedb.disk.segment.SegmentSizeSetting;

public class ReadWriteTimeCollectionOnDisk<T extends Serializable> extends ReadWriteCollectionOnDisk<T> implements BlueTimeCollection<T> {

	public ReadWriteTimeCollectionOnDisk(ReadWriteDbOnDisk db, String name, BlueCollectionVersion requestedVersion, Class<? extends BlueKey> requestedKeyType, Class<T> valueType, List<Class<? extends Serializable>> additionalRegisteredClasses, SegmentSizeSetting segmentSize) throws BlueDbException {
		super(db, name, requestedVersion, requestedKeyType, valueType, additionalRegisteredClasses, segmentSize);
	}

	@Override
	public BlueTimeQuery<T> query() {
		return new TimeQueryOnDisk<T>(this);
	}

}
