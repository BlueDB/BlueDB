package org.bluedb.api.index;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.UUID;

import org.bluedb.api.keys.LongKey;
import org.bluedb.disk.IndexableTestValue;
import org.junit.Test;

public class LongIndexExtractorTest {

	@Test
	public void test() {
		IndexableTestValue testValue = new IndexableTestValue(UUID.randomUUID(), 12, 45, "Whatever", 27);
		
		LongIndexKeyExtractor<IndexableTestValue> extractor = value -> Arrays.asList(value.getLongValue());
		assertEquals(Arrays.asList(testValue.getLongKey()), extractor.extractKeys(testValue));
		
		LongIndexKeyExtractor<IndexableTestValue> badExtractor = value -> null;
		assertEquals(Arrays.asList(), badExtractor.extractKeys(testValue));
		
		LongIndexKeyExtractor<IndexableTestValue> listExtractor = value -> Arrays.asList(value.getLongValue(), 10L);
		assertEquals(Arrays.asList(testValue.getLongKey(), new LongKey(10)), listExtractor.extractKeys(testValue));
	}

}
