package org.bluedb.disk;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

public class MappingIteratorTest {
	@Test
	public void test() {
		List<Integer> integerList = new LinkedList<>();
		for(int i = 0; i < 10; i++) {
			integerList.add(i);
		}
		
		MappingIterator<Integer, String> mappingIterator = new MappingIterator<Integer, String>(integerList.iterator(), String::valueOf);
		int i = 0;
		while(mappingIterator.hasNext()) {
			assertEquals(String.valueOf(i), mappingIterator.next());
			i++;
		}
		assertEquals(10, i);
		assertFalse(mappingIterator.hasNext());
	}

}
