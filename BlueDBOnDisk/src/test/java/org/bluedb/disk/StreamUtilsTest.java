package org.bluedb.disk;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.Test;

public class StreamUtilsTest {
	
	@Test
	public void test_streamItem() {
		assertStreamEquals(Arrays.asList(), StreamUtils.stream((Object)null));
		assertStreamEquals(Arrays.asList("A"), StreamUtils.stream("A"));
	}
	
	@Test
	public void test_streamArray() {
		assertStreamEquals(Arrays.asList(), StreamUtils.stream((Object[])null));
		assertStreamEquals(Arrays.asList(), StreamUtils.stream(new String[] { }));
		assertStreamEquals(Arrays.asList("A", "B", "C"), StreamUtils.stream(new String[] { "A", "B", "C" }));
	}
	
	@Test
	public void test_intStreamIntArray() {
		assertStreamEquals(Arrays.asList(), StreamUtils.intStream((int[])null));
		assertStreamEquals(Arrays.asList(), StreamUtils.intStream(new int[] { }));
		assertStreamEquals(Arrays.asList(1, 2, 3), StreamUtils.intStream(new int[] { 1, 2, 3 }));
	}
	
	@Test
	public void test_streamIntArray() {
		assertStreamEquals(Arrays.asList(), StreamUtils.stream((int[])null));
		assertStreamEquals(Arrays.asList(), StreamUtils.stream(new int[] { }));
		assertStreamEquals(Arrays.asList(1, 2, 3), StreamUtils.stream(new int[] { 1, 2, 3 }));
	}
	
	@Test
	public void test_streamCollection() {
		assertStreamEquals(Arrays.asList(), StreamUtils.stream((List<String>)null));
		assertStreamEquals(Arrays.asList("A", "B", "C"), StreamUtils.stream(Arrays.asList("A", "B", "C")));
	}
	
	@Test
	public void test_streamStream() {
		assertStreamEquals(Arrays.asList(), StreamUtils.stream((Stream<String>)null));
		assertStreamEquals(Arrays.asList("A", "B", "C"), StreamUtils.stream(Arrays.asList("A", "B", "C").stream()));
	}
	
	@Test
	public void test_streamMap() {
		Map<String, String> map = new HashMap<>();
		map.put("1", "One");
		map.put("2", "Two");
		map.put("3", "Three");
		
		assertStreamEquals(Arrays.asList(), StreamUtils.stream((Map<String, String>)null));
		assertStreamEquals(map.entrySet().stream().collect(Collectors.toList()), StreamUtils.stream(map));
	}

	@Test
	public void test_concat() {
		Stream<String> stream1 = Stream.of("A", "B");
		Stream<String> stream2 = Stream.of("C", "D");
		Stream<String> stream3 = Stream.of("E", "F");

		Stream<String> concatenatedStream = StreamUtils.concat(stream1, null, stream2, stream3);
		assertStreamEquals(Arrays.asList("A", "B", "C", "D", "E", "F"), concatenatedStream);
	}
	
	private void assertStreamEquals(List<?> expectedList, Stream<?> actualStream) {
		assertEquals(expectedList, actualStream.collect(Collectors.toList()));
	}
	
	private void assertStreamEquals(List<?> expectedList, IntStream actualIntStream) {
		assertEquals(expectedList, actualIntStream.boxed().collect(Collectors.toList()));
	}

}
