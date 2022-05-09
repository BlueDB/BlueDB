package org.bluedb.api;

public enum BlueCollectionVersion {
	/**
	 * Original version. Uses duplicate records in overlapping segments in order to support timeframe queries on timeframe collections.
	 */
	VERSION_1,
	
	/**
	 * No longer uses duplicate records in overlapping timeframes. Uses a default time index in order to support timeframe queries on timeframe collections.
	 */
	VERSION_2,
	;
	
	public static BlueCollectionVersion getDefault() {
		/*
		 * We're going to use version 1 as the default for now. Version 2 is recommended, but if a user wants it then they have
		 * to request it at this time. We don't want old code to start automatically using version 2 and potentially put old 
		 * working stuff at risk. Version 2 collections would also stop working if a customer got a version 2 collection and
		 * then downgraded to an older version that didn't support them. So for now we'll continue with version 1 as the default
		 * and will probably change it in the future.
		 */
		return VERSION_1;
	}
	
	public boolean utilizesDefaultTimeIndex() {
		switch(this) {
		case VERSION_1:
			return false;
		default:
			return true;
		}
	}
}
