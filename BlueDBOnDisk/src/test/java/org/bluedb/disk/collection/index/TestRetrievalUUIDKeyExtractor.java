package org.bluedb.disk.collection.index;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.UUIDKey;
import org.bluedb.disk.TestValue;

public class TestRetrievalUUIDKeyExtractor implements KeyExtractor<UUIDKey, TestValue> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<UUIDKey> extractKeys(TestValue object) {
		UUIDKey key = new UUIDKey(new UUID(object.getCupcakes(), object.getCupcakes()));
		return Arrays.asList(key);
	}

	@Override
	public Class<UUIDKey> getType() {
		return UUIDKey.class;
	}
}
