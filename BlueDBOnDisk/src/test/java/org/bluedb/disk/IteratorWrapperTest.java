package org.bluedb.disk;

import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.UncheckedBlueDbException;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperFlatMapper;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperMapper;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperValidator;
import org.junit.Test;

public class IteratorWrapperTest {
	
	@Test
	public void test_iteration() {
		List<Integer> intList = IntStream.range(0, 100)
				.boxed()
				.collect(Collectors.toList());
		
		Iterator<Integer> iterator = new IteratorWrapper<Integer, Integer>(intList.iterator(), Integer.class);
		List<Integer> resultList = readIntoList(iterator);
		
		assertEquals(intList, resultList);
	}
	
	@Test(expected = UncheckedBlueDbException.class)
	public void test_iterationThrowsExceptionWhenTypesDoNotMatch() {
		List<Integer> intList = IntStream.range(0, 100)
				.boxed()
				.collect(Collectors.toList());
		
		Iterator<String> iterator = new IteratorWrapper<Integer, String>(intList.iterator(), String.class);
		readIntoList(iterator);
	}
	
	@Test
	public void test_filters() {
		List<Integer> intList = IntStream.range(0, 100)
			.boxed()
			.collect(Collectors.toList());
		
		Iterator<Integer> iterator = new IteratorWrapper<Integer, Integer>(intList.iterator(), Integer.class)
				.addFilter(i -> isMultipleOfX(2, i));

		assertAllMultiplesOfX(2, iterator);
		
		iterator = new IteratorWrapper<Integer, Integer>(intList.iterator(), Integer.class)
				.addFilter(i -> isMultipleOfX(2, i) && isMultipleOfX(3, i));

		assertAllMultiplesOfX(2, iterator);
		assertAllMultiplesOfX(3, iterator);
	}
	
	@Test
	public void test_validators() {
		List<Integer> intList = IntStream.range(0, 100)
				.boxed()
				.collect(Collectors.toList());
			
		IteratorWrapperValidator<Integer> noOddNumbersValidator = i -> {
			if((i + 1) % 2 == 0) {
				throw new BlueDbException("Invalid odd integer: " + i);
			}
		};
		
		Iterator<Integer> iterator = new IteratorWrapper<Integer, Integer>(intList.iterator(), Integer.class)
				.addFilter(i -> isMultipleOfX(2, i))
				.addValidator(noOddNumbersValidator);
		readIntoList(iterator); //Doesn't throw an exception because odd numbers are filtered out
		
		try {
			iterator = new IteratorWrapper<Integer, Integer>(intList.iterator(), Integer.class)
					.addValidator(noOddNumbersValidator);
			readIntoList(iterator);
			fail("The iterator has odd numbers in it, so the noOddNumbersValidator should have thrown an exception");
		} catch(UncheckedBlueDbException e) {
			//expected
		}
	}
	
	@Test
	public void test_mapping() {
		List<Integer> intList = IntStream.range(0, 100)
				.boxed()
				.collect(Collectors.toList());
		
		List<String> stringList = intList.stream()
			.map(String::valueOf)
			.collect(Collectors.toList());
		
		IteratorWrapperMapper<Integer, String> mapper = String::valueOf;
		
		Iterator<String> iterator = new IteratorWrapper<Integer, String>(intList.iterator(), mapper);
		assertEquals(stringList, readIntoList(iterator));
	}
	
	@Test
	public void test_flatMapping() {
		List<List<Integer>> integerListOfLists = new LinkedList<>();
		for(int i = 0; i < 10; i+=2) {
			List<Integer> integerList = new LinkedList<>();
			integerList.add(i);
			integerList.add(i+1);
			integerListOfLists.add(integerList);
		}
		
		IteratorWrapperFlatMapper<List<Integer>, String> flatMapper = integerList -> {
			return StreamUtils.stream(integerList)
				.map(String::valueOf)
				.collect(Collectors.toList());
		};
		
		IteratorWrapper<List<Integer>, String> flatMappingIterator = new IteratorWrapper<>(integerListOfLists.iterator(), flatMapper);
		int i = 0;
		while(flatMappingIterator.hasNext()) {
			assertEquals(String.valueOf(i), flatMappingIterator.next());
			i++;
		}
		assertEquals(10, i);
		assertFalse(flatMappingIterator.hasNext());
	}
	
	private <T> List<T> readIntoList(Iterator<T> iterator) {
		List<T> list = new LinkedList<>();
		while(iterator.hasNext()) {
			list.add(iterator.next());
		}
		return list;
	}

	private void assertAllMultiplesOfX(int x, Iterator<Integer> iterator) {
		while(iterator.hasNext()) {
			Integer i = iterator.next();
			if(!isMultipleOfX(x, i)) {
				fail("Invalid value " + i + ". Expected all values in the iterator to be multiples of " + x);
			}
		}
	}

	private boolean isMultipleOfX(int x, int number) {
		return (number % x) == 0;
	}

}
