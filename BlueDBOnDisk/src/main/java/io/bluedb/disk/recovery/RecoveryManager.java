package io.bluedb.disk.recovery;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import io.bluedb.api.exceptions.BlueDbException;
import io.bluedb.disk.Blutils;
import io.bluedb.disk.segment.BlueEntity;

public class RecoveryManager {

	public void saveChange(PendingChange change) throws BlueDbException {
		String filename = getFileName(change);
		Blutils.save(filename, change);
	}

	public void removeChange(PendingChange change) throws BlueDbException {
		String filename = getFileName(change);
		File file = new File(filename);
		if (!file.delete()) {
			// TODO do we want to throw an exception
			throw new BlueDbException("failed to remove pending change from recovery folder: " + change);
		}
	}

	public static String getFileName(PendingChange change) {
		return  String.valueOf(change.getTimeCreated()) + ".pending";
	}
	public static List<BlueEntity> getPendingChanges() {
		// TODO
		return new ArrayList<>();
	}
}
