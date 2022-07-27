package org.bluedb.disk.helpers.index;

import java.util.Arrays;
import java.util.List;

import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.LongKey;
import org.bluedb.disk.TestValue;

public class TestRetrievalLongKeyExtractor implements KeyExtractor<LongKey, TestValue> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<LongKey> extractKeys(TestValue object) {
		LongKey key = new LongKey(object.getCupcakes());
		return Arrays.asList(key);
	}

	@Override
	public Class<LongKey> getType() {
		return LongKey.class;
	}
}
