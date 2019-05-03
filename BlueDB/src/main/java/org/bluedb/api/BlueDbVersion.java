package org.bluedb.api;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

public class BlueDbVersion implements Comparable<BlueDbVersion>, Serializable {
	
	private static final long serialVersionUID = 1L;

	public static BlueDbVersion CURRENT = new BlueDbVersion(2, 0, 0, "");

	private final int major;
	private final int minor;
	private final int patch;
	private final String label;

	public BlueDbVersion(int major, int minor, int patch, String label) {
		this.major = major;
		this.minor = minor;
		this.patch = patch;
		this.label = (label == null) ? "" : label;
	}

	@Override
	public int compareTo(BlueDbVersion other) {
		return Comparator
		        .comparing(BlueDbVersion::getMajor)
		        .thenComparing(BlueDbVersion::getMinor)
		        .thenComparing(BlueDbVersion::getPatch)
		        .thenComparing(BlueDbVersion::getLabel)
		        .compare(this, other);
	}

	@Override
	public boolean equals(final Object other) {
		if (!(other instanceof BlueDbVersion)) {
			return false;
		}
		BlueDbVersion castOther = (BlueDbVersion) other;
		return Objects.equals(major, castOther.major) 
				&& Objects.equals(minor, castOther.minor) 
				&& Objects.equals(patch, castOther.patch) 
				&& Objects.equals(label, castOther.label);
	}

	public int getMajor() {
		return major;
	}

	public int getMinor() {
		return minor;
	}

	public int getPatch() {
		return patch;
	}

	public String getLabel() {
		return label;
	}

	@Override
	public int hashCode() {
		return Objects.hash(major, minor, patch, label);
	}

	@Override
	public String toString() {
		return "BlueDbVersion " + major + "." + minor + "." + patch + "-" + label;
	}
}
