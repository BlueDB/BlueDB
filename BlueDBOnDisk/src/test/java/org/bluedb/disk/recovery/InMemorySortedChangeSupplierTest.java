package org.bluedb.disk.recovery;

import org.bluedb.disk.TestValue;
import org.junit.After;

public class InMemorySortedChangeSupplierTest extends SortedChangeSupplierTest {

	@Override
	protected SortedChangeSupplier<TestValue> createSortedChangeSupplier() {
		return new InMemorySortedChangeSupplier<TestValue>(changeList);
	}
	
	@After
	public void after() {
		sortedChangeSupplier.close();
	}

	/*
	 * This class creates a specific implementation of SortedChangeSupplier then runs the generic tests on it
	 * which are defined in the parent class. 
	 */
}
