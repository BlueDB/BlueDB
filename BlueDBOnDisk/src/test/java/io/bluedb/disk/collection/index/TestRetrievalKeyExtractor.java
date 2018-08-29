package io.bluedb.disk.collection.index;

import io.bluedb.api.KeyExtractor;
import io.bluedb.api.keys.IntegerKey;
import io.bluedb.disk.TestValue;

public class TestRetrievalKeyExtractor implements KeyExtractor<IntegerKey, TestValue> {

	@Override
	public IntegerKey extractKey(TestValue object) {
		return new IntegerKey(object.getCupcakes());
	}

	@Override
	public Class<IntegerKey> getType() {
		return IntegerKey.class;
	}
}
