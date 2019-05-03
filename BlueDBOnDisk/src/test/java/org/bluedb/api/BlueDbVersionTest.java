package org.bluedb.api;

import static org.junit.Assert.*;

import org.junit.Test;

public class BlueDbVersionTest {

	static BlueDbVersion _1_0_0_null = new BlueDbVersion (1, 0, 0, null);
	static BlueDbVersion _1_0_0 = new BlueDbVersion (1, 0, 0, "");
	static BlueDbVersion _1_0_1 = new BlueDbVersion (1, 0, 1, "");
	static BlueDbVersion _1_1_0 = new BlueDbVersion (1, 1, 0, "");
	static BlueDbVersion _1_0_0_a = new BlueDbVersion (1, 0, 0, "a");
	static BlueDbVersion _2_0_0 = new BlueDbVersion (2, 0, 0, "");
	
	@Test
	public void test_compareTo() {
		assertTrue(_1_0_0.compareTo(_1_0_0_null) == 0);
		assertTrue(_1_0_0.compareTo(_1_0_0_a) < 0);
		assertTrue(_1_0_0_a.compareTo(_1_0_0) > 0);
		assertTrue(_1_0_0.compareTo(_1_1_0) < 0);
		assertTrue(_2_0_0.compareTo(_1_0_0) > 0);
	}

	@Test
	public void test_equals() {
		assertTrue(_1_0_0.equals(_1_0_0));
		assertTrue(_1_0_0.equals(_1_0_0_null));
		assertFalse(_1_0_0.equals(_1_0_0_a));
		assertFalse(_1_0_0_a.equals(_1_0_0));
		assertFalse(_1_0_1.equals(_1_0_0));
		assertTrue(_1_0_0_a.equals(_1_0_0_a));
		assertFalse(_1_0_0.equals(_1_1_0));
		assertFalse(_2_0_0.equals(_1_0_0));
		assertFalse(_2_0_0.equals(null));
	}

	@Test
	public void test_hashCode() {
		assertEquals(_1_0_0.hashCode(), _1_0_0.hashCode());
		assertEquals(_1_0_0.hashCode(), _1_0_0_null.hashCode());
		assertNotSame(_1_0_0.hashCode(), _1_0_0_a.hashCode());
		assertNotSame(_1_0_0.hashCode(), _1_0_1.hashCode());
		assertNotSame(_1_0_0.hashCode(), _1_1_0.hashCode());
		assertNotSame(_1_0_0.hashCode(), _2_0_0.hashCode());
	}

	@Test
	public void test_toString() {
		assertTrue(_1_0_0_a.toString().contains("1.0.0-a"));
	}
}
