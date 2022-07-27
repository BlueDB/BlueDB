package org.bluedb.disk.collection.index.extractors;

import java.io.Serializable;
import java.util.List;

import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.segment.ReadableSegmentManager;

public interface DefaultTimeKeyExtractor <K extends ValueKey, V extends Serializable> extends KeyExtractor<K, V> {
	public List<K> extractKeys(BlueKey key, ReadableSegmentManager<V> segmentManager);
}
