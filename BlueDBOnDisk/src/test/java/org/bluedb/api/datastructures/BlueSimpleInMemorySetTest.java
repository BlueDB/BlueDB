package org.bluedb.api.datastructures;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

public class BlueSimpleInMemorySetTest {

	@Test
	public void test() {
		List<String> stringList = Arrays.asList("one", "two", "three", "four");
		Set<String> stringSet = new HashSet<String>(stringList);
		BlueSimpleInMemorySet<String> blueStringSet = new BlueSimpleInMemorySet<>(stringSet);
		
		for(String s : stringList) {
			assertTrue(blueStringSet.contains(s));
		}
		
		assertFalse(blueStringSet.contains("something else"));
		assertFalse(blueStringSet.contains("five"));
		
		Iterator<String> stringIterator = blueStringSet.iterator();
		
		Set<String> stringSetFromIterator = new HashSet<>();
		while(stringIterator.hasNext()) {
			stringSetFromIterator.add(stringIterator.next());
		}
		assertEquals(stringSet, stringSetFromIterator);
	}

}
