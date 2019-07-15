package org.bluedb.api.index;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.UUID;

import org.bluedb.api.keys.IntegerKey;
import org.bluedb.disk.IndexableTestValue;
import org.junit.Test;

public class IntegerIndexExtractorTest {

	@Test
	public void test() {
		IndexableTestValue testValue = new IndexableTestValue(UUID.randomUUID(), 0, 5, "Whatever", 27);
		
		IntegerIndexKeyExtractor<IndexableTestValue> extractor = value -> Arrays.asList(value.getIntValue());
		assertEquals(Arrays.asList(testValue.getIntegerKey()), extractor.extractKeys(testValue));
		
		IntegerIndexKeyExtractor<IndexableTestValue> badExtractor = value -> null;
		assertEquals(Arrays.asList(), badExtractor.extractKeys(testValue));
		
		IntegerIndexKeyExtractor<IndexableTestValue> listExtractor = value -> Arrays.asList(value.getIntValue(), 10);
		assertEquals(Arrays.asList(testValue.getIntegerKey(), new IntegerKey(10)), listExtractor.extractKeys(testValue));
	}

}
