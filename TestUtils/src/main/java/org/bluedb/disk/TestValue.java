package org.bluedb.disk;

import java.io.Serializable;

public class TestValue implements Serializable {
	private static final long serialVersionUID = 1L;

	private String name;
	private int cupcakes = 0;

	public TestValue(String name) {
		this.name = name;
	}

	public TestValue(String name, int cupcakes) {
		this.name = name;
		this.cupcakes = cupcakes;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getCupcakes() {
		return cupcakes;
	}

	public void addCupcake() {
		cupcakes += 1;
	}

	public void setCupcakes(int cupcakes) {
		this.cupcakes = cupcakes;
	}

	public void doSomethingNaughty() {
		throw new RuntimeException("you shall not pass!");
	}

	@Override
	public String toString() {
		return name + " has " + cupcakes + " cupcakes";
	}
	
	public TestValue clone() {
		return new TestValue(this.name, this.cupcakes);
	}
	
	public TestValue cloneWithNewCupcakeCount(int cupcakes) {
		return new TestValue(this.name, cupcakes);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof TestValue))
			return false;
		TestValue otherTestValue = (TestValue) obj;
		return nullSafeEquals(name, otherTestValue.name) && cupcakes == otherTestValue.cupcakes;
	}

	private static boolean nullSafeEquals(String a, String b) {
		if (a == null) {
			return b == null;
		} else {
			return a.equals(b);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + cupcakes;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}
}
