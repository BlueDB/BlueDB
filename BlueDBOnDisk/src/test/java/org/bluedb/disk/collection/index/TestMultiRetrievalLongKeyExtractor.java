package org.bluedb.disk.collection.index;

import java.util.Arrays;
import java.util.List;

import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.LongKey;
import org.bluedb.disk.TestValue;

public class TestMultiRetrievalLongKeyExtractor implements KeyExtractor<LongKey, TestValue> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<LongKey> extractKeys(TestValue object) {
		LongKey key1 = new LongKey(object.getCupcakes());
		LongKey key2 = new LongKey(object.getCupcakes() + 2);
		return Arrays.asList(key1, key2);
	}

	@Override
	public Class<LongKey> getType() {
		return LongKey.class;
	}
}
