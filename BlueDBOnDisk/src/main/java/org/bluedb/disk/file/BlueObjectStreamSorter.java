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

import org.bluedb.api.exceptions.BlueDbException;
import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.lock.BlueReadLock;
import org.bluedb.disk.metadata.BlueFileMetadataKey;

public class BlueObjectStreamSorter<T extends ComparableAndSerializable<T>> {
	private static int DEFAULT_SIZE_PER_CHUNK = 1000;
	private static int DEFAULT_CHUNKS_TO_COMBINE_AT_ONCE = 5;
	
	private Iterator<T> input;
	private Path outputFilepath;
	private ReadWriteFileManager fileManager;
	private Map<BlueFileMetadataKey, String> metadataEntries;

	private AtomicInteger nextSortedChunkFileId = new AtomicInteger();
	private List<Path> sortedChunkFiles;
	
	public BlueObjectStreamSorter(Iterator<T> input, Path outputFilepath, ReadWriteFileManager fileManager, Map<BlueFileMetadataKey, String> metadataEntries) {
		this.input = input;
		this.outputFilepath = outputFilepath;
		this.fileManager = fileManager;
		this.metadataEntries = metadataEntries;
	}
	
	public void sortAndWriteToFile() throws BlueDbException {
		try {
			List<T> sortedChunk = readAndSortNextChunk();
			if(!input.hasNext()) {
				try(BlueObjectOutput<T> blueObjectOutput = createBlueOutputStream(outputFilepath)) {
					writeSortedChunkToOutput(sortedChunk, blueObjectOutput);
					return;
				}
			}
			
			sortedChunkFiles = new LinkedList<>();
			writeSortedChunkToNextChunkFile(sortedChunk);
			
			while(input.hasNext()) {
				sortedChunk = readAndSortNextChunk();
				writeSortedChunkToNextChunkFile(sortedChunk);
			}
			
			while(sortedChunkFiles.size() > 1) {
				combineNextSortedChunkFiles();
			}
			
			Path finalSortedChunkFile = sortedChunkFiles.get(0);
			Files.move(finalSortedChunkFile, outputFilepath);
		} catch(Throwable t) {
			throw new BlueDbException("Blue Object Stream Sort Failed", t);
		}
	}

	private BlueObjectOutput<T> createBlueOutputStream(Path path) throws BlueDbException {
		BlueObjectOutput<T> blueObjectOutput = fileManager.getBlueOutputStreamWithoutLock(path);
			StreamUtils.stream(metadataEntries)
				.forEach(entry -> blueObjectOutput.setMetadataValue(entry.getKey(), entry.getValue()));
			return blueObjectOutput;
	}

	private List<T> readAndSortNextChunk() {
		List<T> nextChunk = new ArrayList<>();
		while(nextChunk.size() < DEFAULT_SIZE_PER_CHUNK && input.hasNext()) {
			nextChunk.add(input.next());
		}
		Collections.sort(nextChunk, Comparator.nullsFirst(T::compareTo));
		return nextChunk;
	}

	private void writeSortedChunkToNextChunkFile(List<T> sortedChunk) throws BlueDbException {
		Path nextSortedChunkFile = createNextChunkFilePath();
		sortedChunkFiles.add(nextSortedChunkFile);
		try(BlueObjectOutput<T> blueObjectChunkOutput = createBlueOutputStream(nextSortedChunkFile)) {
			writeSortedChunkToOutput(sortedChunk, blueObjectChunkOutput);
		}
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
		List<BlueObjectInput<T>> nextSortedChunkFileInputsToCombine = new LinkedList<>();
		
		Iterator<Path> it = sortedChunkFiles.iterator();
		while(nextSortedChunkFileInputsToCombine.size() < DEFAULT_CHUNKS_TO_COMBINE_AT_ONCE && it.hasNext()) {
			Path nextSortedChunkFilePathToCombine = it.next();
			it.remove();
			BlueReadLock<Path> readLock = fileManager.getReadLockIfFileExists(nextSortedChunkFilePathToCombine);
			nextSortedChunkFileInputsToCombine.add(fileManager.getBlueInputStream(readLock));
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

	private void closeAndDeleteProcessedFileInputs(List<BlueObjectInput<T>> nextSortedChunkFileInputsToCombine) {
		for(BlueObjectInput<T> input : nextSortedChunkFileInputsToCombine) {
			input.close();
			input.getPath().toFile().delete();
		};
	}
}
