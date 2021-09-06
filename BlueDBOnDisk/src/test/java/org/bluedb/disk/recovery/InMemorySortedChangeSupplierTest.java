package org.bluedb.disk.recovery;

import org.bluedb.disk.TestValue;

public class InMemorySortedChangeSupplierTest extends SortedChangeSupplierTest {

	@Override
	protected SortedChangeSupplier<TestValue> createSortedChangeSupplier() {
		return new InMemorySortedChangeSupplier<TestValue>(changeList);
	}

	/*
	 * This class creates a specific implementation of SortedChangeSupplier then runs the generic tests on it
	 * which are defined in the parent class. 
	 */
}
