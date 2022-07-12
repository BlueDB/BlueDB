package org.bluedb.disk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.UncheckedBlueDbException;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperFlatMapper;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperMapper;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperValidator;
import org.junit.Test;

public class IteratorWrapperTest {
	
	private AtomicInteger index = new AtomicInteger(0);
	
	@Test
	public void test_iteration() {
		List<Integer> intList = IntStream.range(0, 100)
				.boxed()
				.collect(Collectors.toList());
		
		IteratorWrapper<Integer, Integer> iterator = new IteratorWrapper<Integer, Integer>(intList.iterator(), Integer.class);
		List<Integer> resultList = readIntoList(iterator);
		
		assertEquals(intList, resultList);
	}
	
	@Test(expected = UncheckedBlueDbException.class)
	public void test_iterationThrowsExceptionWhenTypesDoNotMatch() {
		List<Integer> intList = IntStream.range(0, 100)
				.boxed()
				.collect(Collectors.toList());
		
		IteratorWrapper<Integer, String> iterator = new IteratorWrapper<Integer, String>(intList.iterator(), String.class);
		readIntoList(iterator);
	}
	
	@Test
	public void test_filters() {
		List<Integer> intList = IntStream.range(0, 100)
			.boxed()
			.collect(Collectors.toList());
		
		IteratorWrapper<Integer, Integer> iterator = new IteratorWrapper<Integer, Integer>(intList.iterator(), Integer.class)
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
		
		IteratorWrapper<Integer, Integer> iterator = new IteratorWrapper<Integer, Integer>(intList.iterator(), Integer.class)
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
		
		IteratorWrapper<Integer, String> iterator = new IteratorWrapper<Integer, String>(intList.iterator(), mapper);
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
	
	private <I, O> List<O> readIntoList(IteratorWrapper<I, O> iteratorWrapper) {
		List<O> list = new LinkedList<>();
		while(hasNext(iteratorWrapper)) {
			list.add(next(iteratorWrapper));
		}
		return list;
	}

	private <I> void assertAllMultiplesOfX(int x, IteratorWrapper<I, Integer> iteratorWrapper) {
		while(hasNext(iteratorWrapper)) {
			Integer i = next(iteratorWrapper);
			if(!isMultipleOfX(x, i)) {
				fail("Invalid value " + i + ". Expected all values in the iterator to be multiples of " + x);
			}
		}
	}

	private boolean isMultipleOfX(int x, int number) {
		return (number % x) == 0;
	}
	
	private <I, O> boolean hasNext(IteratorWrapper<I, O> iteratorWrapper) {
		//We want to alternate between hasNext and peek != null to make sure that both work
		if(index.get() % 2 == 0) {
			return iteratorWrapper.hasNext();
		} else {
			O peek = iteratorWrapper.peek();
			return peek != null;
		}
	}
	
	private <I, O> O next(IteratorWrapper<I, O> iteratorWrapper) {
		//We want to alternate between next and peek/next to make sure that both work
		if(index.getAndIncrement() % 2 == 0) {
			return iteratorWrapper.next();
		} else {
			O peek = iteratorWrapper.peek();
			iteratorWrapper.next();
			return peek;
		}
	}

}
