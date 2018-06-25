package io.bluedb.disk.segment;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.stream.Collectors;

import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.api.keys.BlueKey;
import io.bluedb.api.keys.TimeFrameKey;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.collection.BlueCollectionImpl;
import io.bluedb.disk.serialization.BlueSerializer;

public class Segment <T extends Serializable> {

	private final BlueSerializer serializer;
	private final Path segmentPath;

	public Segment(Path segmentPath, BlueSerializer serializer) {
		this.segmentPath = segmentPath;
		this.serializer = serializer;
	}

	public Segment(BlueCollectionImpl<T> db, long segmentId) {
		this(db, String.valueOf(segmentId));
	}

	@Override
	public String toString() {
		return "<Segment for path " + segmentPath.toString() + ">";
	}

	public boolean contains(BlueKey key) throws BlueDbException {
		File file = getFileFor(key);
		if (!file.exists()) {
			return false;
		}
		Map<BlueKey, T> fileContents = fetchAsMap(file);
		return fileContents.containsKey(key);
	}

	public void put(BlueKey key, T value) throws BlueDbException {
		File file = getFileFor(key);
		Map<BlueKey, T> fileContents = fetchAsMap(file);
		fileContents.put(key, value);
		persist(file, fileContents);
	}

	public void delete(BlueKey key) throws BlueDbException {
		File file = getFileFor(key);
		if (file.exists()) {
			Map<BlueKey, T> fileContents = fetchAsMap(file);
			fileContents.remove(key);
			persist(file, fileContents);
		}
	}

	// TODO test or delete?
	public BlueEntity<T> read(BlueKey key) throws BlueDbException {
        File file = getFileFor(key);
        Map<BlueKey, T> fileContents = fetchAsMap(file);
        for (BlueEntity<T> entity: fileContents.values()) {
            if (entity.getKey().equals(key))
                return entity;
        }
        return null;
    }
	public T get(BlueKey key) throws BlueDbException {
		File file = getFileFor(key);
		Map<BlueKey, T> fileContents = fetchAsMap(file);
		return fileContents.get(key);
	}

	public List<T> getAll() throws BlueDbException {
		File[] filesInFolder = segmentPath.toFile().listFiles();
		List<T> results = new ArrayList<>();
		for (File file: filesInFolder) {
			Map<BlueKey, T> fileContents = fetchAsMap(file);
			results.addAll(fileContents.values());
		}
		return results;
	}

	// TODO delete?
    public List<BlueEntity<T>> read(long minTime, long maxTime) throws BlueDbException {
	    return getRange(minTime, maxTime);
    }


//    public List<BlueEntity<T>> read(long minTime, long maxTime) throws BlueDbException {
//        List<BlueEntity<T>> results = new ArrayList<>();
//        List<Long> groupingNumbers = getAllFiles().stream()
//                .filter((f) -> isFileNameALong(f))
//                .map((f) -> f.getName())
//                .map((filename) -> Long.valueOf(filename))
//                .collect(Collectors.toList());
//        for (long groupingNumber: groupingNumbers) {
//            TreeMap<BlueKey, BlueEntity<T>> data = load(groupingNumber);
//            for (BlueEntity<T>entity: data.values()) {
//                if (Blutils.meetsTimeConstraint(entity.getKey(), minTime, maxTime))
//                    results.add(entity);
//            }
//        }
//        return results;
//    }

    public List<BlueEntity<T>> getRange(long minTime, long maxTime) throws BlueDbException {
		File[] filesInFolder = segmentPath.toFile().listFiles();
		List<BlueEntity<T>> results = new ArrayList<>();
		for (File file: filesInFolder) {
			List<BlueEntity<T>> fileContents = fetch(file);
			for (BlueEntity<T> entity: fileContents) {
				BlueKey key = entity.getKey();
				if (inTimeRange(minTime, maxTime, key)) {
					results.add(entity);
				}
			}
		}
		return results;
	}

//
//    // TODO refactor and test and maybe move somewhere else
//    protected static boolean isFileNameALong(File file) {
//        try {
//            String fileName = file.getName();
//            long fileNameAsLong = Long.valueOf(fileName);
//            return true;
//        } catch(Exception e) {
//            return false;
//        }
//    }
//
//    // TODO refactor and test and maybe move somewhere else
//    protected static boolean isFileNameALongInRange(File file, long minValue, long maxValue) {
//        try {
//            String fileName = file.getName();
//            long fileNameAsLong = Long.valueOf(fileName);
//            return fileNameAsLong >= minValue && fileNameAsLong <= maxValue;
//        } catch(Exception e) {
//            return false;
//        }
//    }
//
//    // TODO refactor and test
//    private List<File> getAllFiles() {
//        File segmentFolder = path.toFile();
//        if (!segmentFolder.exists()) {
//            return new ArrayList<>();
//        } else {
//            return Arrays.asList(segmentFolder.listFiles());
//        }
//    }

	protected File getFileFor(BlueKey key) {
		return getPathFor(key).toFile();
	}

	protected Path getPath() {
		return segmentPath;
	}

	private Path getPathFor(BlueKey key) {
		String fileName = String.valueOf(key.getGroupingNumber());
		return Paths.get(segmentPath.toString(), fileName);
	}

//    @SuppressWarnings("unchecked")
//    private TreeMap<BlueKey, BlueEntity<T>> load(long groupingNumber) throws BlueDbException {
//        Path groupPath = Paths.get(path.toString(), String.valueOf(groupingNumber));
//        try {
//            File file = groupPath.toFile();
//            if (!file.exists()) {
//                return new TreeMap<>();
//            } else {
//                byte[] bytes = Files.readAllBytes(groupPath);
//                return (TreeMap<BlueKey, BlueEntity<T>>) serializer.deserializeObjectFromByteArray(bytes);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//            throw new BlueDbException("error loading segment from disk for " + groupPath.toString(), e);
//        }
//    }

	@SuppressWarnings("unchecked")
	private TreeMap<BlueKey, T> fetchAsMap(File file) throws BlueDbException {
		List<BlueEntity<T>> entities = fetch(file);
		return asMap(entities);
	}

	// TODO handle locking?
	@SuppressWarnings("unchecked")
	private List<BlueEntity<T>> fetch(File file) throws BlueDbException {
		if (!file.exists())
			return new ArrayList<BlueEntity<T>>();
		byte[] fileData = load(file.toPath());
		List<BlueEntity<T>> fileContents =  (ArrayList<BlueEntity<T>>) serializer.deserializeObjectFromByteArray((fileData));
		return fileContents;
	}

	// TODO handle locking?
	private void persist(File file, Map<BlueKey, T> data) throws BlueDbException {
		if (data.isEmpty()) {
			file.delete();
		} else {
			ArrayList<BlueEntity<T>> entites = asEntityArrayList(data);
			save(file.toPath(), entites);
		}
	}

	private TreeMap<BlueKey, T> asMap(List<BlueEntity<T>> list) {
		TreeMap<BlueKey, T> map = new TreeMap<>();
		for (BlueEntity<T> entity: list) {
			map.put(entity.getKey(), entity.getObject());
		}
		return map;
	}

	private ArrayList<BlueEntity<T>> asEntityArrayList(Map<BlueKey, T> data) {
		ArrayList<BlueEntity<T>> list = new ArrayList<>();
		for (Entry<BlueKey, T> entry: data.entrySet()) {
			BlueEntity<T> entity = new BlueEntity<T>(entry.getKey(), entry.getValue());
			list.add(entity);
		}
		return list;
	}

	private static boolean inTimeRange(long minTime, long maxTime, BlueKey key) {
		if (key instanceof TimeFrameKey) {
			TimeFrameKey timeFrameKey = (TimeFrameKey) key;
			return timeFrameKey.getEndTime() >= minTime && timeFrameKey.getStartTime() <= maxTime;
		} else {
			return key.getGroupingNumber() >= minTime && key.getGroupingNumber() <= maxTime;
		}
	}

	public byte[] load(Path path) throws BlueDbException {
		File file = path.toFile();
		if (!file.exists())
			return null;
		try {
			return Files.readAllBytes(path);
		} catch (IOException e) {
			e.printStackTrace();
			// TODO delete the file ?
			throw new BlueDbException("error writing to disk (" + path +")", e);
		}
	}

	public void save(Path path, Object o) throws BlueDbException {
		File file = path.toFile();
		file.getParentFile().mkdirs();
		byte[] bytes = serializer.serializeObjectToByteArray(o);
		try (FileOutputStream fos = new FileOutputStream(file)) {
			fos.write(bytes);
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
			// TODO delete the file
			throw new BlueDbException("error writing to disk (" + path +")", e);
		}
	}

}
