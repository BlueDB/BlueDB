package org.bluedb.disk.collection.index.extractors;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.LongTimeKey;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.segment.ReadableSegmentManager;

/**
 * Returns the start grouping number of all segments that the record overlaps with the exception of the first segment. 
 * Given a timeframe, you should be able to use this index to easily find all of the segments that contain records that
 * started before it but overlap into it.
 */
public class OverlappingTimeSegmentsKeyExtractor<V extends Serializable> implements DefaultTimeKeyExtractor<LongTimeKey, V> {

	private static final long serialVersionUID = 1L;

	@Override
	public Class<LongTimeKey> getType() {
		return LongTimeKey.class;
	}
	
	@Override
	public List<LongTimeKey> extractKeys(BlueKey key, ReadableSegmentManager<V> segmentManager) {
		LinkedList<LongTimeKey> keys = StreamUtils.stream(segmentManager.getPathManager().getAllPossibleSegmentStartGroupingNumbers(key))
			.map(LongTimeKey::new)
			.collect(Collectors.toCollection(LinkedList::new));
		keys.remove(0);
		return keys;
	}

	@Override
	public List<LongTimeKey> extractKeys(V value) {
		throw new UnsupportedOperationException("DefaultTimeIndexKeyExtractor is a special case that is used for the default time index on time based collections. This method shouldn't be called, it should have special handling.");
	}

}
