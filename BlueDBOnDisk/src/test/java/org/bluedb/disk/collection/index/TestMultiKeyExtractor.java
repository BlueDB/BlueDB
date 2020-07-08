package org.bluedb.disk.collection.index;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.disk.TestValue;

/*
 * This will extract many integer keys from a TestValue based on the value of TestValue#cupcakes. If the
 * value is event then it will return all event numbers from 0 to the value. Else it will return all
 * odd numbers from 1 to the value. It is just an easy way to test index stuff that requires many
 * index keys.
 */
public class TestMultiKeyExtractor implements KeyExtractor<IntegerKey, TestValue> {

	private static final long serialVersionUID = 1L;

	@Override
	public List<IntegerKey> extractKeys(TestValue object) {
		return IntStream.rangeClosed(0, object.getCupcakes())
			.boxed()
			.filter(i -> i % 2 == object.getCupcakes() % 2)
			.map(i -> new IntegerKey(i))
			.collect(Collectors.toList());
	}

	@Override
	public Class<IntegerKey> getType() {
		return IntegerKey.class;
	}
}
