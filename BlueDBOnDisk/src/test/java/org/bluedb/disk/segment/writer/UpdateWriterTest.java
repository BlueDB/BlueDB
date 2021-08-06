package org.bluedb.disk.segment.writer;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.file.BlueObjectInput;
import org.bluedb.disk.file.BlueObjectOutput;
import org.bluedb.disk.metadata.BlueFileMetadata;
import org.bluedb.disk.metadata.BlueFileMetadataKey;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;
import static org.mockito.Mockito.*;

public class UpdateWriterTest {

	@Test
	@SuppressWarnings("unchecked")
	public void test_process_shouldSkipEncryption_skipEncryptionWriteCalledWithExpectedParam() throws BlueDbException {
		// Arrange
		byte[] expected = new byte[] {'r', 'a', 'w'};

		BlueKey updateWriterKey = new TimeKey(1, 1);
		BlueKey otherKey = new TimeKey(2, 1);

		BlueFileMetadata metadata = new BlueFileMetadata();
		metadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key");

		BlueObjectInput<BlueEntity<TestValue>> mockInput = (BlueObjectInput<BlueEntity<TestValue>>) mock(BlueObjectInput.class);
		doReturn(metadata).when(mockInput).getMetadata();
		doReturn(expected).when(mockInput).getLastRawBytes();
		when(mockInput.hasNext()).thenReturn(true, true, false);
		when(mockInput.next()).thenReturn(new BlueEntity<>(otherKey, null));

		BlueObjectOutput<BlueEntity<TestValue>> mockOutput = (BlueObjectOutput<BlueEntity<TestValue>>) mock(BlueObjectOutput.class);
		doReturn(metadata).when(mockOutput).getMetadata();

		UpdateWriter<TestValue> updateWriter = new UpdateWriter<>(updateWriterKey, new TestValue("update this!"));

		// Act
		updateWriter.process(mockInput, mockOutput);

		// Assert
		verify(mockOutput, times(2)).writeBytesAndForceSkipEncryption(expected);
		verify(mockOutput, never()).writeBytesAndAllowEncryption(any());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void test_process_shouldNotSkipEncryption_allowEncryptionWriteCalledWithExpectedParam() throws BlueDbException {
		// Arrange
		byte[] expected = new byte[] {'u', 'n', 'e', 'n', 'c', 'r', 'y', 'p', 't', 'e', 'd'};

		BlueKey updateWriterKey = new TimeKey(1, 1);
		BlueKey otherKey = new TimeKey(2, 1);

		BlueFileMetadata inputMetadata = new BlueFileMetadata();
		inputMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key-1");
		BlueFileMetadata outputMetadata = new BlueFileMetadata();
		outputMetadata.put(BlueFileMetadataKey.ENCRYPTION_VERSION_KEY, "valid-key-2");

		BlueObjectInput<BlueEntity<TestValue>> mockInput = (BlueObjectInput<BlueEntity<TestValue>>) mock(BlueObjectInput.class);
		doReturn(inputMetadata).when(mockInput).getMetadata();
		doReturn(expected).when(mockInput).getLastUnencryptedBytes();
		when(mockInput.hasNext()).thenReturn(true, true, false);
		when(mockInput.next()).thenReturn(new BlueEntity<>(otherKey, null));

		BlueObjectOutput<BlueEntity<TestValue>> mockOutput = (BlueObjectOutput<BlueEntity<TestValue>>) mock(BlueObjectOutput.class);
		doReturn(outputMetadata).when(mockOutput).getMetadata();

		UpdateWriter<TestValue> updateWriter = new UpdateWriter<>(updateWriterKey, new TestValue("update this!"));

		// Act
		updateWriter.process(mockInput, mockOutput);

		// Assert
		verify(mockOutput, times(2)).writeBytesAndAllowEncryption(expected);
		verify(mockOutput, never()).writeBytesAndForceSkipEncryption(any());
	}

}
