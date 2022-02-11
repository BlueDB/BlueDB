package org.bluedb.api.datastructures;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

public class BlueSimpleIteratorWrapperTest {

	@Test
	public void test() {
		int listSize = 10;
		
		List<String> stringList = new ArrayList<>();
		for(int i = 0; i < listSize; i++) {
			stringList.add(UUID.randomUUID().toString());
		}
		
		BlueSimpleIterator<String> iterator = new BlueSimpleIteratorWrapper<>(stringList.iterator());
		for(int i = 0; i < listSize; i++) {
			assertTrue(iterator.hasNext());
			assertEquals(stringList.get(i), iterator.next());
		}
		assertFalse(iterator.hasNext());
		
		iterator.close(); //Doesn't do anything, but don't need it flagged as uncovered.
	}

}
