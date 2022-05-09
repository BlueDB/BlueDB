package org.bluedb.disk.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.index.conditions.OnDiskIndexCondition;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;
import org.mockito.Mockito;

public class QueryIndexConditionGroupTest {

	@Test
	public void test_constructor_nullIndexConditionsResultsInEmptyList() {
		QueryIndexConditionGroup<TestValue> indexConditionGroup = new QueryIndexConditionGroup<TestValue>(true, null);
		assertEquals(Arrays.asList(), indexConditionGroup.getIndexConditions());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void test_fieldSettingAndModification() {
		OnDiskIndexCondition<?, TestValue> conditionMock1 = (OnDiskIndexCondition<?, TestValue>) Mockito.mock(OnDiskIndexCondition.class);
		OnDiskIndexCondition<?, TestValue> conditionMock2 = (OnDiskIndexCondition<?, TestValue>) Mockito.mock(OnDiskIndexCondition.class);
		OnDiskIndexCondition<?, TestValue> conditionMock3 = (OnDiskIndexCondition<?, TestValue>) Mockito.mock(OnDiskIndexCondition.class);
		
		List<OnDiskIndexCondition<?, TestValue>> originalConditionsList = new LinkedList<>(Arrays.asList(conditionMock1, null, conditionMock2));
		List<OnDiskIndexCondition<?, TestValue>> expectedConditionsList = new LinkedList<>(Arrays.asList(conditionMock1, conditionMock2));
		
		QueryIndexConditionGroup<TestValue> indexConditionGroup = new QueryIndexConditionGroup<TestValue>(false, originalConditionsList);
		assertEquals(expectedConditionsList, indexConditionGroup.getIndexConditions());
		assertFalse(indexConditionGroup.isShouldAnd());
		
		originalConditionsList.remove(0);
		assertEquals("Modifying the original index conditins should affect those in the group", expectedConditionsList, indexConditionGroup.getIndexConditions());
		
		indexConditionGroup.addIndexCondition(conditionMock3);
		expectedConditionsList = new LinkedList<>(Arrays.asList(conditionMock1, conditionMock2, conditionMock3));
		assertEquals(expectedConditionsList, indexConditionGroup.getIndexConditions());
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void test_test() {
		BlueEntity<TestValue> entity = new BlueEntity<>(new TimeKey(1, 1), new TestValue("Bob"));
		
		OnDiskIndexCondition<?, TestValue> passingConditionMock = (OnDiskIndexCondition<?, TestValue>) Mockito.mock(OnDiskIndexCondition.class);
		Mockito.doReturn(true).when(passingConditionMock).test(entity);
		
		OnDiskIndexCondition<?, TestValue> failingConditionMock = (OnDiskIndexCondition<?, TestValue>) Mockito.mock(OnDiskIndexCondition.class);
		Mockito.doReturn(false).when(failingConditionMock).test(entity);
		
		QueryIndexConditionGroup<TestValue> singlePassingAndedCondition = new QueryIndexConditionGroup<TestValue>(true, Arrays.asList(passingConditionMock));
		assertTrue(singlePassingAndedCondition.test(entity));
		
		QueryIndexConditionGroup<TestValue> singlePassingOrCondition = new QueryIndexConditionGroup<TestValue>(false, Arrays.asList(passingConditionMock));
		assertTrue(singlePassingOrCondition.test(entity));
		
		QueryIndexConditionGroup<TestValue> singleFailingAndedCondition = new QueryIndexConditionGroup<TestValue>(true, Arrays.asList(failingConditionMock));
		assertFalse(singleFailingAndedCondition.test(entity));
		
		QueryIndexConditionGroup<TestValue> singlFailingOrCondition = new QueryIndexConditionGroup<TestValue>(false, Arrays.asList(failingConditionMock));
		assertFalse(singlFailingOrCondition.test(entity));
		
		QueryIndexConditionGroup<TestValue> andConditionGroup = new QueryIndexConditionGroup<TestValue>(true, Arrays.asList(passingConditionMock, failingConditionMock));
		assertFalse(andConditionGroup.test(entity));
		
		QueryIndexConditionGroup<TestValue> orConditionGroup = new QueryIndexConditionGroup<TestValue>(false, Arrays.asList(passingConditionMock, failingConditionMock));
		assertTrue(orConditionGroup.test(entity));
	}

}
