package io.bluedb.disk.segment;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.stream.Collectors;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.serialization.BlueSerializer;

public class Segment <T extends Serializable> {
//	private final String pathString;
	private final Path path;
	private final BlueSerializer serializer;
	
	public Segment(Path segmentPath, BlueSerializer serializer) {
		this.path = segmentPath;
//		this.pathString = this.path.toString();
		this.serializer = serializer;
	}
//
//	public Segment(Path collectionPath, String segmentId, BlueSerializer serializer) {
//		this(Paths.get(collectionPath.toString(), segmentId), serializer);
//	}
//
//	public Segment(Path collectionPath, long segmentId, BlueSerializer serializer) {
//		this(collectionPath, String.valueOf(segmentId), serializer);
//	}

	public void put(BlueKey key, T value) throws BlueDbException {
		TreeMap<BlueKey, BlueEntity<T>> data = load(key.getGroupingNumber());
		BlueEntity<T> entity = new BlueEntity<T>(key, value);
		data.put(key, entity);
		String pathString = Paths.get(path.toString(), String.valueOf(key.getGroupingNumber())).toString();
		Blutils.save(pathString, data, serializer);
	}

	public void delete(BlueKey key) throws BlueDbException {
		TreeMap<BlueKey, BlueEntity<T>> data = load(key.getGroupingNumber());
		data.remove(key);
		String pathString = Paths.get(path.toString(), String.valueOf(key.getGroupingNumber())).toString();
		Blutils.save(pathString, data, serializer);
	}

	public BlueEntity<T> read(BlueKey key) throws BlueDbException {
		Collection<BlueEntity<T>> values = load(key.getGroupingNumber()).values();
		for (BlueEntity<T> entity: values) {
			if (entity.getKey().equals(key))
				return entity;
		}
		return null;
	}

	public List<BlueEntity<T>> read(long minTime, long maxTime) throws BlueDbException {
		List<BlueEntity<T>> results = new ArrayList<>();
		List<Long> groupingNumbers = getAllFiles().stream()
				.filter((f) -> isFileNameALong(f))
				.map((f) -> f.getName())
				.map((filename) -> Long.valueOf(filename))
				.collect(Collectors.toList());
		for (long groupingNumber: groupingNumbers) {
			TreeMap<BlueKey, BlueEntity<T>> data = load(groupingNumber);
			for (BlueEntity<T>entity: data.values()) {
			if (Blutils.meetsTimeConstraint(entity.getKey(), minTime, maxTime))
				results.add(entity);
			}
		}
		return results;
	}

	
	// TODO refactor and test and maybe move somewhere else
	protected static boolean isFileNameALong(File file) {
		try {
			String fileName = file.getName();
			long fileNameAsLong = Long.valueOf(fileName);
			return true;
		} catch(Exception e) {
			return false;
		}
	}

	// TODO refactor and test and maybe move somewhere else
	protected static boolean isFileNameALongInRange(File file, long minValue, long maxValue) {
		try {
			String fileName = file.getName();
			long fileNameAsLong = Long.valueOf(fileName);
			return fileNameAsLong >= minValue && fileNameAsLong <= maxValue;
		} catch(Exception e) {
			return false;
		}
	}

	// TODO refactor and test
	private List<File> getAllFiles() {
		File segmentFolder = path.toFile();
		if (!segmentFolder.exists()) {
			return new ArrayList<>();
		} else {
			return Arrays.asList(segmentFolder.listFiles());
		}
	}

	@SuppressWarnings("unchecked")
	private TreeMap<BlueKey, BlueEntity<T>> load(long groupingNumber) throws BlueDbException {
		Path groupPath = Paths.get(path.toString(), String.valueOf(groupingNumber));
		try {
			File file = groupPath.toFile();
			if (!file.exists()) {
				return new TreeMap<>();
			} else {
				byte[] bytes = Files.readAllBytes(groupPath);
				return (TreeMap<BlueKey, BlueEntity<T>>) serializer.deserializeObjectFromByteArray(bytes);
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new BlueDbException("error loading segment from disk for " + groupPath.toString(), e);
		}
	}

	@Override
	public String toString() {
		return "<Segment for path " + path.toString() + ">";
	}
}
