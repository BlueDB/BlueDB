package org.bluedb.disk.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.metadata.BlueFileMetadataKey;

public class BlueObjectStreamSorter<T extends ComparableAndSerializable<T>> {
	private final BlueObjectStreamSorterConfig config;
	
	private final Path outputFilepath;
	private final ReadWriteFileManager fileManager;
	private final Map<BlueFileMetadataKey, String> metadataEntries;
	
	private final Iterator<T> input;

	private final AtomicInteger nextSortedChunkFileId = new AtomicInteger();
	private final List<Path> largeSortedChunkFiles = new LinkedList<>();
	private final List<Path> sortedChunkFiles = new LinkedList<>();
	
	public BlueObjectStreamSorter(Path outputFilepath, ReadWriteFileManager fileManager, Map<BlueFileMetadataKey, String> metadataEntries, BlueObjectStreamSorterConfig config) {
		this(null, outputFilepath, fileManager, metadataEntries, config);
	}
	
	public BlueObjectStreamSorter(Iterator<T> input, Path outputFilepath, ReadWriteFileManager fileManager, Map<BlueFileMetadataKey, String> metadataEntries, BlueObjectStreamSorterConfig config) {
		this.input = input;
		this.outputFilepath = outputFilepath;
		this.fileManager = fileManager;
		this.metadataEntries = metadataEntries;
		this.config = config != null ? config : BlueObjectStreamSorterConfig.createDefault();
	}
	
	public void addBatchOfObjectsToBeSorted(List<T> batchToBeSorted) throws BlueDbException {
		List<T> sortedChunk = StreamUtils.stream(batchToBeSorted)
			.sorted(Comparator.nullsFirst(T::compareTo))
			.collect(Collectors.toList());
		writeSortedChunkToNextChunkFile(sortedChunk);
	}
	
	public void sortAndWriteToFile() throws BlueDbException {
		try {
			List<T> firstSortedChunkFromInput = readAndSortNextChunkFromInput();
			
			if(writeFirstSortedChunkFromInputIfItContainsEverything(firstSortedChunkFromInput)) {
				return;
			}

			writeEverythingFromInputToSortedChunkFiles(firstSortedChunkFromInput);
			
			moveLargeSortedChunkFilesBackIntoMainChunkList();
			
			combineSortedChunkFiles();
			
			renameFinalSortedChunkFileToTargetOutputFilename();
		} finally {
			moveLargeSortedChunkFilesBackIntoMainChunkList();
			for(Path p : sortedChunkFiles) {
				try {
					Files.deleteIfExists(p);
				} catch(Throwable t2) {
					//ignore
				}
			}
		}
	}

	private boolean writeFirstSortedChunkFromInputIfItContainsEverything(List<T> firstSortedChunkFromInput) throws BlueDbException {
		moveLargeSortedChunkFilesBackIntoMainChunkList();
		
		if(!sortedChunkFiles.isEmpty()) {
			//One or more chunks were added before looking at the input iterator, so return false
			return false; 
		}

		if(input == null || !input.hasNext()) {
			//There is nothing left in the input chunk, so it was all contained in the first chunk
			try(BlueObjectOutput<T> blueObjectOutput = createBlueOutputStream(outputFilepath)) {
				writeSortedChunkToOutput(firstSortedChunkFromInput, blueObjectOutput);
				return true;
			}
		}
		
		return false;
	}

	private List<T> readAndSortNextChunkFromInput() {
		List<T> nextChunk = new ArrayList<>();
		while(nextChunk.size() < config.maxRecordsInInitialChunks && inputExistsAndHasNext()) {
			nextChunk.add(input.next());
		}
		Collections.sort(nextChunk, Comparator.nullsFirst(T::compareTo));
		return nextChunk;
	}
	
	private boolean inputExistsAndHasNext() {
		return input != null && input.hasNext();
	}

	private BlueObjectOutput<T> createBlueOutputStream(Path path) throws BlueDbException {
		BlueObjectOutput<T> blueObjectOutput = fileManager.getBlueOutputStreamWithoutLock(path);
			StreamUtils.stream(metadataEntries)
				.forEach(entry -> blueObjectOutput.setMetadataValue(entry.getKey(), entry.getValue()));
			return blueObjectOutput;
	}

	private void writeEverythingFromInputToSortedChunkFiles(List<T> firstSortedChunkFromInput) throws BlueDbException {
		writeSortedChunkToNextChunkFile(firstSortedChunkFromInput);
		while(inputExistsAndHasNext()) {
			List<T> sortedChunk = readAndSortNextChunkFromInput();
			writeSortedChunkToNextChunkFile(sortedChunk);
		}
	}

	private void writeSortedChunkToNextChunkFile(List<T> sortedChunk) throws BlueDbException {
		Path nextSortedChunkFile = createNextChunkFilePath();
		sortedChunkFiles.add(nextSortedChunkFile);
		try(BlueObjectOutput<T> blueObjectChunkOutput = createBlueOutputStream(nextSortedChunkFile)) {
			writeSortedChunkToOutput(sortedChunk, blueObjectChunkOutput);
		}
		combineSortedChunkFilesIfOverMax();
	}

	private Path createNextChunkFilePath() throws BlueDbException {
		return FileUtils.createTempFilePathInDirectory(outputFilepath.getParent(), outputFilepath.getFileName() + "-chunk-" + nextSortedChunkFileId.getAndIncrement());
	}
	
	private void writeSortedChunkToOutput(List<T> sortedChunk, BlueObjectOutput<T> output) {
		for(T obj : sortedChunk) {
			try {
				output.write(obj);
			} catch (Throwable t) {
				new BlueDbException("Failed to write sorted object to file, skipping object. Object Details:" + obj, t).printStackTrace();
			}
		}
	}

	private void combineSortedChunkFilesIfOverMax() throws BlueDbException {
		if(largeSortedChunkFiles.size() >= config.maxLargeChunkCount) {
			moveLargeSortedChunkFilesBackIntoMainChunkList();
		}
		
		if(sortedChunkFiles.size() >= config.maxChunkCount) {
			combineSortedChunkFiles();
		}
	}

	private void combineSortedChunkFiles() throws BlueDbException {
		while(sortedChunkFiles.size() > 1) {
			combineNextSortedChunkFiles();
		}
		moveCombinedChunkResultToLargeChunkList();
	}

	private void moveLargeSortedChunkFilesBackIntoMainChunkList() {
		sortedChunkFiles.addAll(largeSortedChunkFiles);
		largeSortedChunkFiles.clear();
	}

	private void moveCombinedChunkResultToLargeChunkList() {
		largeSortedChunkFiles.add(sortedChunkFiles.get(0));
		sortedChunkFiles.clear();
	}

	@SuppressWarnings("resource")
	private void combineNextSortedChunkFiles() throws BlueDbException {
		List<BlueObjectInput<T>> nextSortedChunkFileInputsToCombine = removeNextSortedChunkFileInputsToCombine();
		
		Path nextSortedChunkFile = createNextChunkFilePath();
		sortedChunkFiles.add(nextSortedChunkFile);
		try(BlueObjectOutput<T> blueObjectChunkOutput = createBlueOutputStream(nextSortedChunkFile)) {
			BlueObjectInput<T> inputWithNextChange = getInputWithNextChange(nextSortedChunkFileInputsToCombine);
			while(inputWithNextChange != null) {
				blueObjectChunkOutput.write(inputWithNextChange.next());
				inputWithNextChange = getInputWithNextChange(nextSortedChunkFileInputsToCombine);
			}
		} finally {
			closeAndDeleteProcessedFileInputs(nextSortedChunkFileInputsToCombine);
		}
	}

	private List<BlueObjectInput<T>> removeNextSortedChunkFileInputsToCombine() throws BlueDbException {
		long maxSizeToCombineOnThisPass = StreamUtils.stream(sortedChunkFiles)
			.map(FileUtils::size)
			.sorted()
			.limit(config.maxChunksToCombineAtOnce)
			.max(Long::compareTo)
			.orElse(Long.MAX_VALUE);
		
		List<BlueObjectInput<T>> nextSortedChunkFileInputsToCombine = new LinkedList<>();
		
		Iterator<Path> it = sortedChunkFiles.iterator();
		while(nextSortedChunkFileInputsToCombine.size() < config.maxChunksToCombineAtOnce && it.hasNext()) {
			Path nextSortedChunkFilePath = it.next();
			if(FileUtils.size(nextSortedChunkFilePath) <= maxSizeToCombineOnThisPass) {
				//Combine small files first
				it.remove();
				BlueReadLock<Path> readLock = fileManager.getReadLockIfFileExists(nextSortedChunkFilePath);
				nextSortedChunkFileInputsToCombine.add(fileManager.getBlueInputStream(readLock));
			}
		}
		return nextSortedChunkFileInputsToCombine;
	}

	private BlueObjectInput<T> getInputWithNextChange(List<BlueObjectInput<T>> nextSortedChunkFileInputsToCombine) {
		BlueObjectInput<T> inputWithNextChange = StreamUtils.stream(nextSortedChunkFileInputsToCombine)
			.filter(BlueObjectInput::hasNext)
			.min(Comparator.comparing(BlueObjectInput::peek))
			.orElse(null);
		return inputWithNextChange;
	}

	private void closeAndDeleteProcessedFileInputs(List<BlueObjectInput<T>> sortedChunkFileInputsToDelete) {
		for(BlueObjectInput<T> input : sortedChunkFileInputsToDelete) {
			input.close();
			input.getPath().toFile().delete();
		};
	}

	private void renameFinalSortedChunkFileToTargetOutputFilename() throws BlueDbException {
		try {
			Path finalSortedChunkFile = largeSortedChunkFiles.get(0);
			Files.move(finalSortedChunkFile, outputFilepath);
		} catch (Throwable t) {
			throw new BlueDbException("Blue Object Stream Sorter failed to save final sorted file.", t);
		}
	}
	
	/**
	 * The {@link BlueObjectStreamSorter} class allows you to sort a bunch of serializable objects on disk instead of in memory. This
	 * works by saving sorted chunks of records to disk and then combining them until you are left with one big sorted file. This 
	 * object allows you to tweak the numbers behind this algorithm.
	 */
	public static class BlueObjectStreamSorterConfig {
		/** The max number of records to read into memory at one time to be sorted and saved to disk. */
		public int maxRecordsInInitialChunks;
		
		/** The max number of chunks to allow to be saved to disk before combining them. */
		public int maxChunkCount;
		
		/** The max number of large chunks on disk before combining them. Large chunks are created when
		 * smaller chunks are combined into a large chunk. These are ignored for awhile in order to avoid constantly re-write large
		 * files. 
		 */
		public int maxLargeChunkCount;
		
		/** The number of chunks/files that are combined at one time. The system will read from all of the files at once and combine
		 * them into a single sorted file. */
		public int maxChunksToCombineAtOnce;
		
		/**
		 * The {@link BlueObjectStreamSorter} class allows you to sort a bunch of serializable objects on disk instead of in memory. This
		 * works by saving sorted chunks of records to disk and then combining them until you are left with one big sorted file. This 
		 * object allows you to tweak the numbers behind this algorithm.
		 * @param maxRecordsInInitialChunks - The max number of records to read into memory at one time to be sorted and saved to disk.
		 * @param maxChunkCount - The max number of chunks to allow to be saved to disk before combining them.
		 * @param maxLargeChunkCount - The max number of large chunks on disk before combining them. Large chunks are created when
		 * smaller chunks are combined into a large chunk. These are ignored for awhile in order to avoid constantly re-write large
		 * files.
		 * @param maxChunksToCombineAtOnce - The number of chunks/files that are combined at one time. The system will read from all of the files at once and combine
		 * them into a single sorted file.
		 */
		public BlueObjectStreamSorterConfig(int maxRecordsInInitialChunks, int maxChunkCount, int maxLargeChunkCount, int maxChunksToCombineAtOnce) {
			this.maxRecordsInInitialChunks = maxRecordsInInitialChunks;
			this.maxChunkCount = maxChunkCount;
			this.maxLargeChunkCount = maxLargeChunkCount;
			this.maxChunksToCombineAtOnce = maxChunksToCombineAtOnce;
		}
		
		public static BlueObjectStreamSorterConfig createDefault() {
			return new BlueObjectStreamSorterConfig(5000, 125, 125, 5);
		}
	}
}
