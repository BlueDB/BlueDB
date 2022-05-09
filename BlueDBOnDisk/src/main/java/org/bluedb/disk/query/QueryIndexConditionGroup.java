package org.bluedb.disk.query;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.bluedb.disk.StreamUtils;
import org.bluedb.disk.collection.index.conditions.OnDiskIndexCondition;
import org.bluedb.disk.serialization.BlueEntity;

public class QueryIndexConditionGroup<T extends Serializable> {
	private final List<OnDiskIndexCondition<?, T>> indexConditions;
	private final boolean shouldAnd;
	
	public QueryIndexConditionGroup(boolean shouldAnd) {
		this.indexConditions = new LinkedList<>();
		this.shouldAnd = shouldAnd;
	}
	
	public QueryIndexConditionGroup(boolean shouldAnd, Collection<OnDiskIndexCondition<?, T>> indexConditions) {
		this.indexConditions = StreamUtils.stream(indexConditions)
				.filter(Objects::nonNull)
				.collect(Collectors.toCollection(LinkedList::new));
		this.shouldAnd = shouldAnd;
	}
	
	public void addIndexCondition(OnDiskIndexCondition<?, T> indexCondition) {
		indexConditions.add(indexCondition);
	}
	
	public List<OnDiskIndexCondition<?, T>> getIndexConditions() {
		return indexConditions;
	}
	
	public boolean isShouldAnd() {
		return shouldAnd;
	}

	public boolean test(BlueEntity<T> entityToTest) {
		Stream<OnDiskIndexCondition<?, T>> conditionStream = StreamUtils.stream(indexConditions);
		Predicate<? super OnDiskIndexCondition<?, T>> conditionTest = indexCondition -> indexCondition.test(entityToTest);
		if(shouldAnd) {
			return conditionStream.allMatch(conditionTest);
		} else {
			return conditionStream.anyMatch(conditionTest);
		}
	}
	
}
