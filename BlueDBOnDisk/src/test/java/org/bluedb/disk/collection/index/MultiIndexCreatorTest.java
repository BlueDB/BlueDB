package org.bluedb.disk.collection.index;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

import org.bluedb.api.CloseableIterator;
import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.api.index.BlueIndexInfo;
import org.bluedb.api.index.KeyExtractor;
import org.bluedb.api.keys.BlueKey;
import org.bluedb.api.keys.IntegerKey;
import org.bluedb.api.keys.LongKey;
import org.bluedb.api.keys.TimeKey;
import org.bluedb.api.keys.ValueKey;
import org.bluedb.disk.BlueDbDiskTestBase;
import org.bluedb.disk.Blutils;
import org.bluedb.disk.TestValue;
import org.bluedb.disk.collection.index.MultiIndexCreator.IndexManagerService;
import org.bluedb.disk.collection.index.MultiIndexCreator.MultiIndexCreatorResult;
import org.bluedb.disk.serialization.BlueEntity;
import org.junit.Test;
import org.mockito.Mockito;

public class MultiIndexCreatorTest extends BlueDbDiskTestBase {
	private TestValue valueFred1 = new TestValue("Fred", 1);
	private TestValue valueBob3 = new TestValue("Bob", 3);
	private TestValue valueJoe3 = new TestValue("Joe", 3);
	private TimeKey timeKeyFred1 = createTimeKey(1, valueFred1);
	private TimeKey timeKeyBob3 = createTimeKey(2, valueBob3);
	private TimeKey timeKeyJoe3 = createTimeKey(3, valueJoe3);
	
	private IntegerKey integerKey1 = new IntegerKey(1);
	private IntegerKey integerKey2 = new IntegerKey(2);
	private IntegerKey integerKey3 = new IntegerKey(3);
	
	private LongKey longKey1 = new LongKey(1);
	private LongKey longKey2 = new LongKey(2);
	private LongKey longKey3 = new LongKey(3);
	
	private Set<BlueKey> emptyList = new HashSet<>(Arrays.asList());
	private Set<BlueKey> bobAndJoe = new HashSet<>(Arrays.asList(timeKeyBob3, timeKeyJoe3));
	private Set<BlueKey> bobAndJoeAndFred = new HashSet<>(Arrays.asList(timeKeyBob3, timeKeyJoe3, timeKeyFred1));
	private Set<BlueKey> justFred = new HashSet<>(Arrays.asList(timeKeyFred1));
	
	private BlueIndexInfo<IntegerKey, TestValue> index1Info = new BlueIndexInfo<IntegerKey, TestValue>("index1", IntegerKey.class, new TestRetrievalKeyExtractor());
	private BlueIndexInfo<LongKey, TestValue> index2Info = new BlueIndexInfo<LongKey, TestValue>("index2", LongKey.class, new TestMultiRetrievalLongKeyExtractor());
	
	@Test
	public void test_createIndices_multipleIndicesWorks() throws BlueDbException {
		getTimeCollection().insert(timeKeyFred1, valueFred1);
		getTimeCollection().insert(timeKeyBob3, valueBob3);
		getTimeCollection().insert(timeKeyJoe3, valueJoe3);
		
		MultiIndexCreator<TestValue> indexCreator = new MultiIndexCreator<>(Arrays.asList(index1Info, index2Info), getTimeCollection().getIndexManager());
		MultiIndexCreatorResult<TestValue> createIndicesResult = indexCreator.createIndices();
		
		assertTrue(createIndicesResult.getFailedIndexNames().isEmpty());
		assertEquals(2, createIndicesResult.getNewlyCreatedIndices().size());
		
		ReadWriteIndexOnDisk<ValueKey, TestValue> index1 = createIndicesResult.getNewlyCreatedIndices().get(0);
		assertEquals(justFred, index1.getKeys(integerKey1));
		assertEquals(emptyList, index1.getKeys(integerKey2));
		assertEquals(bobAndJoe, index1.getKeys(integerKey3));
		
		ReadWriteIndexOnDisk<ValueKey, TestValue> index2 = createIndicesResult.getNewlyCreatedIndices().get(1);
		assertEquals(justFred, index2.getKeys(longKey1));
		assertEquals(emptyList, index2.getKeys(longKey2));
		assertEquals(bobAndJoeAndFred, index2.getKeys(longKey3));
	}

	@Test
	public void test_createIndices_preExistingLegacyIndicesDoesNotGetInitialized() throws BlueDbException, IOException {
		getTimeCollection().createIndices(Arrays.asList(index1Info, index2Info));
		
		getTimeCollection().insert(timeKeyFred1, valueFred1);
		getTimeCollection().insert(timeKeyBob3, valueBob3);
		getTimeCollection().insert(timeKeyJoe3, valueJoe3);
		
		ReadWriteIndexOnDisk<ValueKey, TestValue> index1 = getTimeCollection().getIndexManager().lookupExistingIndexByName(index1Info.getName()).get();
		deleteNeedsInitializationFile(index1); //Makes it behave like a legacy index collection since they don't have the needs initialization file
		deleteIndexData(index1);
		
		ReadWriteIndexOnDisk<ValueKey, TestValue> index2 = getTimeCollection().getIndexManager().lookupExistingIndexByName(index2Info.getName()).get();
		deleteNeedsInitializationFile(index2); //Makes it behave like a legacy index collection since they don't have the needs initialization file
		deleteIndexData(index2);
		
		MultiIndexCreator<TestValue> indexCreator = new MultiIndexCreator<>(Arrays.asList(index1Info, index2Info), getTimeCollection().getIndexManager());
		MultiIndexCreatorResult<TestValue> createIndicesResult = indexCreator.createIndices();
		
		//Didn't need to create or initialize either of the indices
		assertTrue(createIndicesResult.getFailedIndexNames().isEmpty());
		assertEquals(0, createIndicesResult.getNewlyCreatedIndices().size());
		
		assertEquals(emptyList, index1.getKeys(integerKey1));
		assertEquals(emptyList, index1.getKeys(integerKey2));
		assertEquals(emptyList, index1.getKeys(integerKey3));
		
		assertEquals(emptyList, index2.getKeys(longKey1));
		assertEquals(emptyList, index2.getKeys(longKey2));
		assertEquals(emptyList, index2.getKeys(longKey3));
	}

	@Test
	public void test_createIndices_existingIndicesThatNeedInitializationAreInitialized() throws BlueDbException, IOException {
		getTimeCollection().createIndices(Arrays.asList(index1Info, index2Info));
		
		getTimeCollection().insert(timeKeyFred1, valueFred1);
		getTimeCollection().insert(timeKeyBob3, valueBob3);
		getTimeCollection().insert(timeKeyJoe3, valueJoe3);
		
		ReadWriteIndexOnDisk<ValueKey, TestValue> index1 = getTimeCollection().getIndexManager().lookupExistingIndexByName(index1Info.getName()).get();
		deleteIndexData(index1);
		index1.markAsNeedsInitialization();
		
		ReadWriteIndexOnDisk<ValueKey, TestValue> index2 = getTimeCollection().getIndexManager().lookupExistingIndexByName(index2Info.getName()).get();
		deleteIndexData(index2);
		index2.markAsNeedsInitialization();
		
		//Index data is gone
		assertEquals(emptyList, index1.getKeys(integerKey1));
		assertEquals(emptyList, index1.getKeys(integerKey2));
		assertEquals(emptyList, index1.getKeys(integerKey3));
		
		assertEquals(emptyList, index2.getKeys(longKey1));
		assertEquals(emptyList, index2.getKeys(longKey2));
		assertEquals(emptyList, index2.getKeys(longKey3));
		
		MultiIndexCreator<TestValue> indexCreator = new MultiIndexCreator<>(Arrays.asList(index1Info, index2Info), getTimeCollection().getIndexManager());
		MultiIndexCreatorResult<TestValue> createIndicesResult = indexCreator.createIndices();
		
		//Didn't need to create either of the indices, but it did initialize them
		assertTrue(createIndicesResult.getFailedIndexNames().isEmpty());
		assertEquals(0, createIndicesResult.getNewlyCreatedIndices().size());
		
		assertEquals(justFred, index1.getKeys(integerKey1));
		assertEquals(emptyList, index1.getKeys(integerKey2));
		assertEquals(bobAndJoe, index1.getKeys(integerKey3));
		
		assertEquals(justFred, index2.getKeys(longKey1));
		assertEquals(emptyList, index2.getKeys(longKey2));
		assertEquals(bobAndJoeAndFred, index2.getKeys(longKey3));
	}

	@Test
	public void test_createIndices_existingIndicesWithDifferentKeyTypeFail() throws BlueDbException, IOException {
		getTimeCollection().createIndices(Arrays.asList(index1Info, index2Info));
		
		getTimeCollection().insert(timeKeyFred1, valueFred1);
		getTimeCollection().insert(timeKeyBob3, valueBob3);
		getTimeCollection().insert(timeKeyJoe3, valueJoe3);

		BlueIndexInfo<LongKey, TestValue> index1WrongInfo = new BlueIndexInfo<LongKey, TestValue>("index1", LongKey.class, new TestMultiRetrievalLongKeyExtractor());
		BlueIndexInfo<IntegerKey, TestValue> index2WrongInfo = new BlueIndexInfo<IntegerKey, TestValue>("index2", IntegerKey.class, new TestRetrievalKeyExtractor());
		
		MultiIndexCreator<TestValue> indexCreator = new MultiIndexCreator<>(Arrays.asList(index1WrongInfo, index2WrongInfo), getTimeCollection().getIndexManager());
		MultiIndexCreatorResult<TestValue> createIndicesResult = indexCreator.createIndices();
		
		//Both should have failed
		assertEquals(2, createIndicesResult.getFailedIndexNames().size());
		assertEquals(0, createIndicesResult.getNewlyCreatedIndices().size());
	}

	@Test
	public void test_createIndices_oneSuccessfulAndOneFailure() throws BlueDbException {
		getTimeCollection().insert(timeKeyFred1, valueFred1);
		getTimeCollection().insert(timeKeyBob3, valueBob3);
		getTimeCollection().insert(timeKeyJoe3, valueJoe3);
		
		IndexManagerService<TestValue> indexManagerServiceThatWillNotCreateIndex2 = new IndexManagerService<TestValue>() {
			@Override
			public Optional<ReadWriteIndexOnDisk<ValueKey, TestValue>> lookupExistingIndexByName(String indexName) throws BlueDbException {
				return getTimeCollection().getIndexManager().lookupExistingIndexByName(indexName);
			}
			
			@Override
			public ReadWriteIndexOnDisk<ValueKey, TestValue> createNewIndex(String name, KeyExtractor<? extends ValueKey, TestValue> keyExtractor) throws BlueDbException {
				if(name.equals(index2Info.getName())) {
					throw new BlueDbException("Expected Exception Creating Index " + name);
				}
				return getTimeCollection().getIndexManager().createNewIndex(name, keyExtractor);
			}
			
			@Override
			public CloseableIterator<BlueEntity<TestValue>> getEntityIteratorForEntireCollection() throws BlueDbException {
				return getTimeCollection().getIndexManager().getEntityIteratorForEntireCollection();
			}

			@Override
			public int getMaxRecordsInInitializationChunks() {
				return 2;
			}
		};
		
		MultiIndexCreator<TestValue> indexCreator = new MultiIndexCreator<>(Arrays.asList(index1Info, index2Info), indexManagerServiceThatWillNotCreateIndex2);
		MultiIndexCreatorResult<TestValue> createIndicesResult = indexCreator.createIndices();
		
		assertEquals(1, createIndicesResult.getFailedIndexNames().size());
		assertTrue(createIndicesResult.getFailedIndexNames().contains(index2Info.getName()));
		assertEquals(1, createIndicesResult.getNewlyCreatedIndices().size());
		
		ReadWriteIndexOnDisk<ValueKey, TestValue> index1 = createIndicesResult.getNewlyCreatedIndices().get(0);
		assertEquals(justFred, index1.getKeys(integerKey1));
		assertEquals(emptyList, index1.getKeys(integerKey2));
		assertEquals(bobAndJoe, index1.getKeys(integerKey3));
		
		assertFalse(Files.exists(index1.indexPath.getParent().resolve(index2Info.getName()))); //Index 2 directory shouldn't exist
		assertEquals(Optional.empty(), getTimeCollection().getIndexManager().lookupExistingIndexByName(index2Info.getName()));
	}

	@Test
	public void test_createIndices_allFailWhenEntityIteratorFails() throws BlueDbException {
		getTimeCollection().insert(timeKeyFred1, valueFred1);
		getTimeCollection().insert(timeKeyBob3, valueBob3);
		getTimeCollection().insert(timeKeyJoe3, valueJoe3);
		
		IndexManagerService<TestValue> indexManagerServiceThatReturnsUnusableEntityIterator = new IndexManagerService<TestValue>() {
			@Override
			public Optional<ReadWriteIndexOnDisk<ValueKey, TestValue>> lookupExistingIndexByName(String indexName) throws BlueDbException {
				return getTimeCollection().getIndexManager().lookupExistingIndexByName(indexName);
			}
			
			@Override
			public ReadWriteIndexOnDisk<ValueKey, TestValue> createNewIndex(String name, KeyExtractor<? extends ValueKey, TestValue> keyExtractor) throws BlueDbException {
				return getTimeCollection().getIndexManager().createNewIndex(name, keyExtractor);
			}
			
			@Override
			public CloseableIterator<BlueEntity<TestValue>> getEntityIteratorForEntireCollection() throws BlueDbException {
				@SuppressWarnings("unchecked")
				CloseableIterator<BlueEntity<TestValue>> mockedIterator = Mockito.mock(CloseableIterator.class);
				Mockito.doThrow(new BlueDbException("Expected exception using entity iterator")).when(mockedIterator.hasNext());
				return mockedIterator;
			}

			@Override
			public int getMaxRecordsInInitializationChunks() {
				return 2;
			}
		};
		
		MultiIndexCreator<TestValue> indexCreator = new MultiIndexCreator<>(Arrays.asList(index1Info, index2Info), indexManagerServiceThatReturnsUnusableEntityIterator);
		MultiIndexCreatorResult<TestValue> createIndicesResult = indexCreator.createIndices();
		
		assertEquals(2, createIndicesResult.getFailedIndexNames().size());
		assertEquals(2, createIndicesResult.getNewlyCreatedIndices().size()); //Indices are created, the just fail during initialization
		assertTrue(Files.exists(getTimeCollection().getPath().resolve(".index").resolve(index1Info.getName())));
		assertTrue(Files.exists(getTimeCollection().getPath().resolve(".index").resolve(index2Info.getName())));
	}

	@Test
	public void test_createIndices_oneBadIndexCausesOneFailure() throws BlueDbException {
		getTimeCollection().insert(timeKeyFred1, valueFred1);
		getTimeCollection().insert(timeKeyBob3, valueBob3);
		getTimeCollection().insert(timeKeyJoe3, valueJoe3);
		
		IndexManagerService<TestValue> indexManagerServiceThatWillNotCreateIndex2 = new IndexManagerService<TestValue>() {
			@Override
			public Optional<ReadWriteIndexOnDisk<ValueKey, TestValue>> lookupExistingIndexByName(String indexName) throws BlueDbException {
				return getTimeCollection().getIndexManager().lookupExistingIndexByName(indexName);
			}
			
			@SuppressWarnings("unchecked")
			@Override
			public ReadWriteIndexOnDisk<ValueKey, TestValue> createNewIndex(String name, KeyExtractor<? extends ValueKey, TestValue> keyExtractor) throws BlueDbException {
				if(name.equals(index2Info.getName())) {
					return Mockito.mock(ReadWriteIndexOnDisk.class);
				}
				return getTimeCollection().getIndexManager().createNewIndex(name, keyExtractor);
			}
			
			@Override
			public CloseableIterator<BlueEntity<TestValue>> getEntityIteratorForEntireCollection() throws BlueDbException {
				return getTimeCollection().getIndexManager().getEntityIteratorForEntireCollection();
			}

			@Override
			public int getMaxRecordsInInitializationChunks() {
				return 2;
			}
		};
		
		MultiIndexCreator<TestValue> indexCreator = new MultiIndexCreator<>(Arrays.asList(index1Info, index2Info), indexManagerServiceThatWillNotCreateIndex2);
		MultiIndexCreatorResult<TestValue> createIndicesResult = indexCreator.createIndices();
		
		assertEquals(1, createIndicesResult.getFailedIndexNames().size());
		assertEquals(2, createIndicesResult.getNewlyCreatedIndices().size()); //Both indices are created, one just fails during initialization
		assertTrue(Files.exists(getTimeCollection().getPath().resolve(".index").resolve(index1Info.getName())));
		assertFalse(Files.exists(getTimeCollection().getPath().resolve(".index").resolve(index2Info.getName())));
		
		ReadWriteIndexOnDisk<ValueKey, TestValue> index1 = createIndicesResult.getNewlyCreatedIndices().get(0);
		assertEquals(justFred, index1.getKeys(integerKey1));
		assertEquals(emptyList, index1.getKeys(integerKey2));
		assertEquals(bobAndJoe, index1.getKeys(integerKey3));
		
		ReadWriteIndexOnDisk<ValueKey, TestValue> index2 = createIndicesResult.getNewlyCreatedIndices().get(1);
		assertEquals(emptyList, index2.getKeys(longKey1));
		assertEquals(emptyList, index2.getKeys(longKey2));
		assertEquals(emptyList, index2.getKeys(longKey3));
	}

	private void deleteNeedsInitializationFile(ReadWriteIndexOnDisk<ValueKey, TestValue> index) throws IOException {
		Files.delete(index.indexPath.resolve(ReadWriteIndexOnDisk.FILE_KEY_NEEDS_INITIALIZING));
	}

	private void deleteIndexData(ReadWriteIndexOnDisk<ValueKey, TestValue> index) throws IOException {
		Files.list(index.indexPath)
			.filter(Files::isDirectory)
			.map(Path::toFile)
			.forEach(Blutils::recursiveDelete);
	}

}
