package org.bluedb.disk.collection.index.extractors;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.keys.ActiveTimeKey;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.disk.segment.ReadableSegmentManager;

/**
 * Used to index the grouping number of key value pairs that represent active records (records with a key of type {@link ActiveTimeKey}).
 * This is used by default on time collections (version 2+) so that we can easily find exactly what keys should be included for
 * a query timeframe and what segments they are in.
 */
public class ActiveRecordTimeKeyExtractor<V extends Serializable> implements DefaultTimeKeyExtractor<LongTimeKey, V> {

	private static final long serialVersionUID = 1L;

	@Override
	public Class<LongTimeKey> getType() {
		return LongTimeKey.class;
	}
	
	@Override
	public List<LongTimeKey> extractKeys(BlueKey key, ReadableSegmentManager<V> segmentManager) {
		List<LongTimeKey> indexValueKeys = new LinkedList<>();
		if(key != null && key.isActiveTimeKey()) {
			indexValueKeys.add(new LongTimeKey(key.getGroupingNumber()));
		}
		return indexValueKeys;
	}

	@Override
	public List<LongTimeKey> extractKeys(V value) {
		throw new UnsupportedOperationException("DefaultTimeIndexKeyExtractor is a special case that is used for the default time index on time based collections. This method shouldn't be called, it should have special handling.");
	}

}
