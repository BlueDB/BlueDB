package org.bluedb.disk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.bluedb.TestCloseableIterator;
import org.bluedb.api.CloseableIterator;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.exceptions.UncheckedBlueDbException;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperFlatMapper;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperMapper;
import org.bluedb.disk.IteratorWrapper.IteratorWrapperValidator;
import org.junit.Test;

public class CloseableIteratorWrapperTest {
	
	private AtomicInteger index = new AtomicInteger(0);
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_close() {
		CloseableIterator<Integer> iterator = mock(CloseableIterator.class);
		CloseableIteratorWrapper<Integer, Integer> closeableIteratorWrapper = new CloseableIteratorWrapper<Integer, Integer>(iterator, Integer.class);
		closeableIteratorWrapper.close();
		verify(iterator).close();
	}
	
	@Test
	@SuppressWarnings("unchecked")
	public void test_keepAlive() {
		CloseableIterator<Integer> iterator = mock(CloseableIterator.class);
		try(CloseableIteratorWrapper<Integer, Integer> closeableIteratorWrapper = new CloseableIteratorWrapper<Integer, Integer>(iterator, Integer.class)) {
			closeableIteratorWrapper.keepAlive();
			verify(iterator).keepAlive();
		}
	}
	
	@Test
	public void test_iteration() {
		List<Integer> intList = IntStream.range(0, 100)
				.boxed()
				.collect(Collectors.toList());
		
		CloseableIterator<Integer> inputIterator = new TestCloseableIterator<Integer>(intList.iterator());
		try(CloseableIteratorWrapper<Integer, Integer> iterator = new CloseableIteratorWrapper<Integer, Integer>(inputIterator, Integer.class)) {
			List<Integer> resultList = readIntoList(iterator);
			assertEquals(intList, resultList);
		}
	}
	
	@Test(expected = UncheckedBlueDbException.class)
	public void test_iterationThrowsExceptionWhenTypesDoNotMatch() {
		List<Integer> intList = IntStream.range(0, 100)
				.boxed()
				.collect(Collectors.toList());
		
		CloseableIterator<Integer> inputIterator = new TestCloseableIterator<Integer>(intList.iterator());
		try(CloseableIteratorWrapper<Integer, String> iterator = new CloseableIteratorWrapper<Integer, String>(inputIterator, String.class)) {
			readIntoList(iterator);
		}
	}
	
	@Test
	public void test_filters() {
		List<Integer> intList = IntStream.range(0, 100)
			.boxed()
			.collect(Collectors.toList());
		
		CloseableIterator<Integer> inputIterator = new TestCloseableIterator<Integer>(intList.iterator());
		try(CloseableIteratorWrapper<Integer, Integer> iterator = new CloseableIteratorWrapper<Integer, Integer>(inputIterator, Integer.class)) {
			iterator.addFilter(i -> isMultipleOfX(2, i));
			assertAllMultiplesOfX(2, iterator);
		}
		
		inputIterator = new TestCloseableIterator<Integer>(intList.iterator());
		try(CloseableIteratorWrapper<Integer, Integer> iterator = new CloseableIteratorWrapper<Integer, Integer>(inputIterator, Integer.class)) {
			iterator.addFilter(i -> isMultipleOfX(2, i) && isMultipleOfX(3, i));
			assertAllMultiplesOfX(2, iterator);
			assertAllMultiplesOfX(3, iterator);
		}
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
		
		CloseableIterator<Integer> inputIterator = new TestCloseableIterator<Integer>(intList.iterator());
		try(CloseableIteratorWrapper<Integer, Integer> iterator = new CloseableIteratorWrapper<Integer, Integer>(inputIterator, Integer.class)) {
			iterator.addFilter(i -> isMultipleOfX(2, i));
			iterator.addValidator(noOddNumbersValidator);
			readIntoList(iterator); //Doesn't throw an exception because odd numbers are filtered out
		}
		
		inputIterator = new TestCloseableIterator<Integer>(intList.iterator());
		try(CloseableIteratorWrapper<Integer, Integer> iterator = new CloseableIteratorWrapper<Integer, Integer>(inputIterator, Integer.class)) {
			iterator.addValidator(noOddNumbersValidator);
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
		
		CloseableIterator<Integer> inputIterator = new TestCloseableIterator<Integer>(intList.iterator());
		try(CloseableIteratorWrapper<Integer, String> iterator = new CloseableIteratorWrapper<Integer, String>(inputIterator, mapper)) {
			assertEquals(stringList, readIntoList(iterator));
		}
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

		CloseableIterator<List<Integer>> inputIterator = new TestCloseableIterator<List<Integer>>(integerListOfLists.iterator());
		try(CloseableIteratorWrapper<List<Integer>, String> flatMappingIterator = new CloseableIteratorWrapper<>(inputIterator, flatMapper)) {
			int i = 0;
			while(flatMappingIterator.hasNext()) {
				assertEquals(String.valueOf(i), flatMappingIterator.next());
				i++;
			}
			assertEquals(10, i);
			assertFalse(flatMappingIterator.hasNext());
		}
	}
	

	
	private <I, O> List<O> readIntoList(CloseableIteratorWrapper<I, O> iteratorWrapper) {
		List<O> list = new LinkedList<>();
		while(hasNext(iteratorWrapper)) {
			list.add(next(iteratorWrapper));
		}
		return list;
	}

	private <I> void assertAllMultiplesOfX(int x, CloseableIteratorWrapper<I, Integer> iteratorWrapper) {
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
	
	private <I, O> boolean hasNext(CloseableIteratorWrapper<I, O> iteratorWrapper) {
		//We want to alternate between hasNext and peek != null to make sure that both work
		if(index.get() % 2 == 0) {
			return iteratorWrapper.hasNext();
		} else {
			O peek = iteratorWrapper.peek();
			return peek != null;
		}
	}
	
	private <I, O> O next(CloseableIteratorWrapper<I, O> iteratorWrapper) {
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
