package io.bluedb.disk.collection.index;

import java.util.Arrays;
import java.util.List;
import io.bluedb.api.index.KeyExtractor;
import io.bluedb.api.keys.IntegerKey;
import io.bluedb.disk.TestValue;

public class TestRetrievalKeyExtractor implements KeyExtractor<IntegerKey, TestValue> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<IntegerKey> extractKeys(TestValue object) {
		IntegerKey key = new IntegerKey(object.getCupcakes());
		return Arrays.asList(key);
	}

	@Override
	public Class<IntegerKey> getType() {
		return IntegerKey.class;
	}
}
