package org.bluedb.disk.collection.index;

import java.util.Arrays;
import java.util.List;

import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.StringKey;
import org.bluedb.disk.TestValue;

public class TestRetrievalStringKeyExtractor implements KeyExtractor<StringKey, TestValue> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<StringKey> extractKeys(TestValue object) {
		StringKey key = new StringKey(String.valueOf(object.getCupcakes()));
		return Arrays.asList(key);
	}

	@Override
	public Class<StringKey> getType() {
		return StringKey.class;
	}
}
