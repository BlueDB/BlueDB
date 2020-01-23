package org.bluedb.disk.collection.task;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.bluedb.TestUtils;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.recovery.IndividualChange;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.junit.Test;
import org.mockito.Mockito;

public class ReplaceMultipleTaskTest {

	@Test
	public void testCreateChangesWithInvalidObject() throws BlueDbException, URISyntaxException, IOException {
		BlueEntity<Call> invalidCall = TestUtils.loadCorruptCall();
		long invalidCallStart = invalidCall.getValue().getStart();
		
		List<BlueEntity<Call>> startingCalls = Arrays.asList(
				Call.generateBasicTestCallEntity(invalidCallStart - 10),
				invalidCall,
				Call.generateBasicTestCallEntity(invalidCallStart + 10)
		); 
		
		ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());
		
		@SuppressWarnings("unchecked")
		ReadWriteCollectionOnDisk<Call> collectionMock = Mockito.mock(ReadWriteCollectionOnDisk.class);
		Mockito.when(collectionMock.getSerializer()).thenReturn(serializer);
		
		ReplaceMultipleTask<Call> task = new ReplaceMultipleTask<>(collectionMock, null, null);
		List<IndividualChange<Call>> changes = task.createChanges(startingCalls, call -> {
			Call clone = call.clone();
			clone.setReceivingParty("testParty");	
			return clone;
		});
		
		//Changes should not include the invalid object but should still include the other two
		assertFalse(changes.stream().anyMatch(call -> call.getKey().equals(invalidCall.getKey())));
		assertEquals(2, changes.size());
		assertTrue(changes.stream().allMatch(call -> "testParty".equals(call.getNewValue().getReceivingParty())));
	}

}
