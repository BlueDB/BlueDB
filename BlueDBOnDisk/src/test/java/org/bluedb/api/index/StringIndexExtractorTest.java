package org.bluedb.api.index;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.UUID;

import org.bluedb.api.keys.StringKey;
import org.bluedb.disk.IndexableTestValue;
import org.junit.Test;

public class StringIndexExtractorTest {

	@Test
	public void test() {
		IndexableTestValue testValue = new IndexableTestValue(UUID.randomUUID(), 0, 5, "Whatever", 27);
		
		StringIndexKeyExtractor<IndexableTestValue> extractor = value -> Arrays.asList(value.getStringValue());
		assertEquals(Arrays.asList(testValue.getStringKey()), extractor.extractKeys(testValue));
		
		StringIndexKeyExtractor<IndexableTestValue> badExtractor = value -> null;
		assertEquals(Arrays.asList(), badExtractor.extractKeys(testValue));
		
		StringIndexKeyExtractor<IndexableTestValue> listExtractor = value -> Arrays.asList(value.getStringValue(), "Extra");
		assertEquals(Arrays.asList(testValue.getStringKey(), new StringKey("Extra")), listExtractor.extractKeys(testValue));
	}

}
