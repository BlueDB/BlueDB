package org.bluedb.disk;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.bluedb.api.Condition;
import org.bluedb.api.exceptions.BlueDbException;

public class Blutils {
	public static <X extends Serializable> boolean meetsConditions(List<Condition<X>> conditions, X object) {
		for (Condition<X> condition: conditions) {
			if (!condition.test(object)) {
				return false;
			}
		}
		return true;
	}

	public static long roundDownToMultiple(long value, long multiple) {
		if (value >= 0) {
			return value - (value % multiple);
		} else if (Long.MIN_VALUE + multiple > value) {  // don't overflow
			return Long.MIN_VALUE;
		} else {
			return (value + 1) - ((value + 1) % multiple) - multiple;
		}
	}

	public static <X, Y extends Comparable<Y>> void sortByMappedValue(List<X> values, Function<? super X, ? extends Y> mapper) {
		Comparator<X> comparator = new Comparator<X>() {
			@Override
			public int compare(X x1, X x2) {
				Y y1 = mapper.apply(x1);
				Y y2 = mapper.apply(x2);
				return y1.compareTo(y2);
			}
		};
		Collections.sort(values, comparator);
	}

	@FunctionalInterface
	public interface CheckedFunction<T, R> {
	   R apply(T t) throws BlueDbException;
	}
	
	public static <X, Y> List<Y> map(List<? extends X> values, CheckedFunction<? super X, ? extends Y> mapper) throws BlueDbException {
		List<Y> results = new ArrayList<>();
		for (X originalValue: values) {
			Y newValue = mapper.apply(originalValue);
			results.add(newValue);
		}
		return results;
	}

	public static <X, Y> List<Y> mapIgnoringExceptions(List<? extends X> values, CheckedFunction<? super X, ? extends Y> mapper) {
		List<Y> results = new ArrayList<>();
		for (X originalValue: values) {
			try {
				Y newValue = mapper.apply(originalValue);
				results.add(newValue);
			} catch (Throwable e) {
			}
		}
		return results;
	}

	public static <X> List<X> filter(List<X> values, Predicate<X> condition) {
		List<X> results = new ArrayList<>();
		for (X value: values) {
			if (condition.test(value)) {
				results.add(value);
			}
		}
		return results;
	}

	public static <X> int lastIndex(List<X> values, Predicate<X> condition) {
		int result = -1;
		for (int i = 0; i < values.size(); i++) {
			X value = values.get(i);
			if (condition.test(value)) {
				result = i;
			}
		}
		return result;
	}

	public static boolean trySleep(long timeMillis) {
		try {
			Thread.sleep(timeMillis);
			return true;
		} catch (InterruptedException e) {
			return false;
		}
	}

	public static void recursiveDelete(File file) {
		if (file.isDirectory()) {
			for (File f: file.listFiles()) {
				recursiveDelete(f);
			}
			file.delete();
		} else {
			file.delete();
		}
	}

	@FunctionalInterface
	public interface UnreliableFunction<T, E extends Throwable> {
	   T get() throws E;
	}
	

	public static <T, E extends Throwable> T tryMultipleTimes(int attempts, UnreliableFunction<T, E> function) throws Throwable {
		if (attempts <= 0) {
			throw new IllegalArgumentException("Number of attempts must be > 0.");
		}
		int failures = 0;
		Throwable lastThrowable = null;
		while (failures < attempts) {
			try {
				return function.get();
			} catch (Throwable t) {
				failures++;
				lastThrowable = t;
				trySleep(1);
			}
		}
		throw lastThrowable;
	}
	
	
	public static String toHex(byte[] bytes) {
		return toHex(bytes, 0, bytes.length);
	}
	
	private final static char[] hexArray = "0123456789ABCDEF".toCharArray();
	public static String toHex(byte[] bytes, int offset, int length) {
	    char[] hexChars = new char[length * 3];
	    for (int i=0; i<length; i++) {
	    	int v = bytes[i+offset] & 0xFF;
	        hexChars[i * 3] = hexArray[v >>> 4];
	        hexChars[i * 3 + 1] = hexArray[v & 0x0F];
	        hexChars[i * 3 + 2] = ' ';
	    }
	    return new String(hexChars);
	}
	
	public static Runnable surroundTaskWithTryCatch(Runnable r) {
		return () -> {
			try {
				r.run();
			} catch(Throwable t) {
				t.printStackTrace();
			}
		};
	}
}
