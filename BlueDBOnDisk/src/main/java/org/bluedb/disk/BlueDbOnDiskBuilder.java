package org.bluedb.disk;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.bluedb.api.BlueDb;
import org.bluedb.api.ReadOnlyBlueDb;

/**
 * A builder for the {@link ReadOnlyBlueDbOnDisk} class
 */
public class BlueDbOnDiskBuilder {
	private Path path = Paths.get(".", "bluedb");
	
	/**
	 * Sets the path you wish to use for the BlueDB data
	 * @param path the path directory that will contain the BlueDB data
	 * @return itself with the path set
	 */
	public BlueDbOnDiskBuilder withPath(Path path) {
		this.path = path;
		return this;
	}

	/**
	 * Builds the {@link BlueDb} object
	 * @return the {@link BlueDb} built
	 */
	public BlueDb build() {
		return new BlueDbOnDisk(path);
	}

	/**
	 * Builds the {@link ReadOnlyBlueDb} object
	 * @return the {@link ReadOnlyBlueDb} built
	 */
	public ReadOnlyBlueDb buildReadOnly() {
		return new ReadOnlyBlueDbOnDisk(path);
	}
	
	/**
	 * Replaced by withPath<br><br>
	 * Sets the path you wish to use for the BlueDB data
	 * @param path the path directory that will contain the BlueDB data
	 * @return itself with the path set
	 */
	@Deprecated
	public BlueDbOnDiskBuilder setPath(Path path) {
		this.path = path;
		return this;
	}
}
