package org.bluedb.disk;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * A builder for the {@link BlueDbOnDisk} class
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
	 * Builds the {@link BlueDbOnDisk} object
	 * @return the {@link BlueDbOnDisk} built
	 */
	public BlueDbOnDisk build() {
		return new BlueDbOnDisk(path);
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
