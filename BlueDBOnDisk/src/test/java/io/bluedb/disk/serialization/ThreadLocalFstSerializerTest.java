package io.bluedb.disk.serialization;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import io.bluedb.disk.TestValue;
import io.bluedb.disk.TestValue2;

public class ThreadLocalFstSerializerTest {
	
	@Test
	public void testStaticRegisteredClassesProblem() {
		ThreadLocalFstSerializer s1 = new ThreadLocalFstSerializer(TestValue.class);
		TestValue value1 = new TestValue("Derek", 1);
		TestValue clone1 = s1.clone(value1);
		assertEquals(value1, clone1);
		
		ThreadLocalFstSerializer s2 = new ThreadLocalFstSerializer(TestValue2.class);
		TestValue value2 = new TestValue("Derek2", 2);
		TestValue clone2 = s2.clone(value2);
		assertEquals(value2, clone2);
		
		TestValue value3 = new TestValue("Derek3", 3);
		TestValue clone3 = s1.clone(value3);
		assertEquals(value3, clone3);
	}
	
	@Test
	public void testAddingRegisteredClass() {
		ThreadLocalFstSerializer s1 = new ThreadLocalFstSerializer();
		ThreadLocalFstSerializer s2 = new ThreadLocalFstSerializer(TestValue.class);
		
		TestValue originalValue = new TestValue("Derek", 3);
		byte[] bytes = s1.serializeObjectToByteArray(originalValue);
		TestValue newValue = (TestValue) s2.deserializeObjectFromByteArray(bytes);
		
		assertEquals(originalValue, newValue);
	}
}
