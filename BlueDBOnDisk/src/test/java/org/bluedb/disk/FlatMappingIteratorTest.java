package org.bluedb.disk;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.junit.Test;

public class FlatMappingIteratorTest {
	@Test
	public void test() {
		List<List<Integer>> integerListOfLists = new LinkedList<>();
		for(int i = 0; i < 10; i+=2) {
			List<Integer> integerList = new LinkedList<>();
			integerList.add(i);
			integerList.add(i+1);
			integerListOfLists.add(integerList);
		}
		Function<List<Integer>, List<String>> mapper = integerList -> {
			return StreamUtils.stream(integerList)
				.map(String::valueOf)
				.collect(Collectors.toList());
		};
		
		FlatMappingIterator<List<Integer>, String> flatMappingIterator = new FlatMappingIterator<>(integerListOfLists.iterator(), mapper);
		int i = 0;
		while(flatMappingIterator.hasNext()) {
			assertEquals(String.valueOf(i), flatMappingIterator.next());
			i++;
		}
		assertEquals(10, i);
		assertFalse(flatMappingIterator.hasNext());
	}

}
