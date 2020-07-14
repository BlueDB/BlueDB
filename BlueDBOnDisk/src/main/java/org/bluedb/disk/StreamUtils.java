package org.bluedb.disk;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public final class StreamUtils {

	private StreamUtils() {}

	public static <T> Stream<T> stream(T item) {
		return item == null ? Stream.empty() : Stream.of(item);
	}

	public static <T> Stream<T> stream(T[] array) {
		return array == null || array.length == 0 ? Stream.empty() : Stream.of(array);
	}

	public static IntStream intStream(int[] array) {
		return array == null ? IntStream.empty() : IntStream.of(array);
	}

	public static Stream<Integer> stream(int[] array) {
		return intStream(array).mapToObj(Integer::valueOf);
	}

	public static <T> Stream<T> stream(Collection<T> collection) {
		return collection == null ? Stream.empty() : collection.stream();
	}

	public static <T> Stream<T> stream(Stream<T> stream) {
		return stream == null ? Stream.empty() : stream;
	}

	public static <T1, T2> Stream<Entry<T1, T2>> stream(Map<T1, T2> map) {
		return map == null ? Stream.empty() : stream(map.entrySet());
	}

	@SafeVarargs
	public static <T> Stream<T> concat(Stream<T>... streams) {
		Stream<T> combinedStreams = Stream.empty();

		for (Stream<T> stream : streams) {
			if (stream != null) {
				combinedStreams = Stream.concat(combinedStreams, stream);
			}
		}

		return combinedStreams;
	}

}
