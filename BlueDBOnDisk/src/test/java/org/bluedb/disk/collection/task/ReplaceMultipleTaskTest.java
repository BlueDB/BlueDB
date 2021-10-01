package org.bluedb.disk.collection.task;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import org.bluedb.TestUtils;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.collection.ReadWriteCollectionOnDisk;
import org.bluedb.disk.models.calls.Call;
import org.bluedb.disk.serialization.BlueEntity;
import org.bluedb.disk.serialization.ThreadLocalFstSerializer;
import org.bluedb.disk.serialization.validation.SerializationException;
import org.junit.Test;
import org.mockito.Mockito;

public class ReplaceMultipleTaskTest {

	@Test
	public void testCreateChangesWithInvalidObject() throws BlueDbException, URISyntaxException, IOException {
		BlueEntity<Call> invalidCall = TestUtils.loadCorruptCall();
		long invalidCallStart = invalidCall.getValue().getStart();
		
		BlueEntity<Call> goodCall1 = Call.generateBasicTestCallEntity(invalidCallStart - 10);
		BlueEntity<Call> coodCall2 = Call.generateBasicTestCallEntity(invalidCallStart + 10);
		
		ThreadLocalFstSerializer serializer = new ThreadLocalFstSerializer(Call.getClassesToRegister());
		
		@SuppressWarnings("unchecked")
		ReadWriteCollectionOnDisk<Call> collectionMock = Mockito.mock(ReadWriteCollectionOnDisk.class);
		Mockito.when(collectionMock.getSerializer()).thenReturn(serializer);
		
		ReplaceMultipleTask<Call> task = new ReplaceMultipleTask<>(collectionMock, null, call -> {
			Call clone = call.clone();
			clone.setReceivingParty("testParty");	
			return clone;
		});
		
		try {
			task.createChange(invalidCall);
			fail();
		} catch(SerializationException e) {
			//expected
		}
		
		assertTrue("testParty".equals(task.createChange(goodCall1).getNewValue().getReceivingParty()));
		assertTrue("testParty".equals(task.createChange(coodCall2).getNewValue().getReceivingParty()));
	}

}
