package org.bluedb.disk.recovery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.config.TestDefaultConfigurationService;
import org.bluedb.disk.serialization.BlueSerializer;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.junit.Test;
import org.mockito.Mockito;

public class SortedChangeIteratorTest {

	@Test
	public void test_iteration() throws BlueDbException {
		BlueSerializer serializer = new ThreadLocalFstSerializer(new TestDefaultConfigurationService(), new Class[] {});
		
		List<IndividualChange<TestValue>> sortedChanges = new LinkedList<>();
		for(int i = 0; i < 10; i++) {
			BlueKey key = new TimeKey(i, i);
			TestValue oldValue = new TestValue(String.valueOf(i), i);
			sortedChanges.add(IndividualChange.createUpdateChange(key, oldValue, value -> value.addCupcake(), serializer));
		}
		
		SortedChangeSupplier<TestValue> sortedChangeSupplier = new InMemorySortedChangeSupplier<TestValue>(sortedChanges);
		SortedChangeIterator<TestValue> sortedChangeIterator = new SortedChangeIterator<TestValue>(sortedChangeSupplier);
		
		int i = 0;
		while(sortedChangeIterator.hasNext()) {
			assertEquals(new TimeKey(i, i), sortedChangeIterator.next().getKey());
			i++;
		}
		assertEquals(10, i);
		assertFalse(sortedChangeIterator.hasNext());
	}
	
	@Test
	public void test_hasNextException() throws BlueDbException {
		@SuppressWarnings("unchecked")
		SortedChangeSupplier<TestValue> mockedSortedChangeSupplier = (SortedChangeSupplier<TestValue>) Mockito.mock(SortedChangeSupplier.class);
		Mockito.doThrow(new BlueDbException("Bad News")).when(mockedSortedChangeSupplier).getNextChange();
		
		SortedChangeIterator<TestValue> sortedChangeIterator = new SortedChangeIterator<TestValue>(mockedSortedChangeSupplier);
		assertFalse(sortedChangeIterator.hasNext()); //Should return false even if it experiences an error trying to check if it has a next
		assertNull(sortedChangeIterator.next());  //Should return false even if it experiences an error trying to get next
	}

}
