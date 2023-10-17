package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/*
 * Student 1 name: Leoul Gezu
 * Student 2 name: Yab Lemma
 * Date: 10/16/2023
 */

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * 
 * @author Doug Shook
 *
 */
public class Aggregator {

	AggregateOperator o;
	boolean groupBy;
	TupleDesc td;
	ArrayList<Tuple> tuples;
	HashMap<Tuple, Integer> counts;  // Only needed to relcalculate average in handleAvg

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		// your code here
		this.o = o;
		this.groupBy = groupBy;
		this.td = td;
		tuples = new ArrayList<Tuple>();
		counts = new HashMap<Tuple, Integer>();

	}

	/**
	 * Merges the given tuple into the current aggregation
	 * 
	 * @param t the tuple to be aggregated
	 * @throws Exception 
	 */
	public void merge(Tuple t) throws Exception {
		// your code here
		
		// COUNT has special empty behavior but everything else can be taken care of here
		if (tuples.isEmpty() && o != AggregateOperator.COUNT) {
			tuples.add(t);
			counts.put(t,1);
			return;
		}
		
		switch (o) {
		case MAX:
			handleMinAndMax(t, AggregateOperator.MAX);
			break;
		case MIN:
			handleMinAndMax(t, AggregateOperator.MIN);
			break;
		case AVG:
			handleSumAndAvg(t, AggregateOperator.AVG);
			break;
		case COUNT:
			handleCount(t);
			break;
		case SUM:
			handleSumAndAvg(t, AggregateOperator.SUM);
			break;
		}
	}
	
	private boolean findGBYTupleToUpdate(Tuple t, Tuple defaultTupleToUpdate) {
		// Only diff between GBY and !GBY is looking for the right tuple to update
		Field newGBYField = t.getField(0);
		boolean isFound = false;
		for (Tuple curr: tuples) {
			Field currGBYField = curr.getField(0);
			if (newGBYField.compare(RelationalOperator.EQ, currGBYField)) {
				defaultTupleToUpdate = curr;
				isFound = true;
			}
		}
		return isFound;
	}

	private void handleSumAndAvg(Tuple t, AggregateOperator op) throws Exception {
		if (!groupBy && t.getField(0).getType().equals(Type.STRING) 
				|| groupBy && t.getField(1).getType().equals(Type.STRING)) {
			throw new Exception("SUM or AVG operator is invalid for string field type");
		}
		
		Tuple tupleToUpdate = tuples.get(0);
		if (groupBy) {
			if (!findGBYTupleToUpdate(t, tupleToUpdate)) {
				// Add the tuple and exit if it is a new GBY field
				tuples.add(t);
				return;
			};
		}
		
		int aggregateFieldIndex = groupBy ? 1 : 0;
		int currAggregateValue = ((IntField) tupleToUpdate.getField(aggregateFieldIndex)).getValue();
		int newAggregateValue = ((IntField) t.getField(aggregateFieldIndex)).getValue();
		
		tupleToUpdate.setField(aggregateFieldIndex, op == AggregateOperator.SUM ? new IntField(newAggregateValue + currAggregateValue):
			new IntField(Math.round(recalculateAverage(counts.get(tupleToUpdate), currAggregateValue, newAggregateValue))));
		counts.put(tupleToUpdate, counts.get(tupleToUpdate)+1);
	}
	
	private Float recalculateAverage(Integer count, Integer prevAverage, Integer newValue) {
		Float newAverage =  ((float)(count*prevAverage + newValue)/(float)(count + 1));
		return newAverage;
	}
	
	private void handleCount(Tuple t) {
		if (tuples.isEmpty()) {
			t.setField(1, new IntField(1));
			tuples.add(t);
			counts.put(t,1);
			return;
		}
		
		Tuple tupleToUpdate = tuples.get(0);
		if (groupBy) {
			if (!findGBYTupleToUpdate(t, tupleToUpdate)) {
				// Add the tuple and exit if it is a new GBY field
				tuples.add(t);
				return;
			};
		}
		
		int aggregateFieldIndex = groupBy ? 1 : 0;
		int currAggregateValue = ((IntField) tupleToUpdate.getField(aggregateFieldIndex)).getValue();
		tupleToUpdate.setField(aggregateFieldIndex, new IntField(currAggregateValue+1));
		counts.put(tupleToUpdate, counts.get(tupleToUpdate)+1);
	}

	private void handleMinAndMax(Tuple t, AggregateOperator op) {
		Tuple tupleToUpdate = tuples.get(0);
		if (groupBy) {
			if (!findGBYTupleToUpdate(t, tupleToUpdate)) {
				// Add the tuple and exit if it is a new GBY field
				tuples.add(t);
				return;
			};
		}
		
		int aggregateFieldIndex = groupBy ? 1 : 0;
		Field newAggregateField = t.getField(aggregateFieldIndex);
		Field currAggregateField = tupleToUpdate.getField(aggregateFieldIndex);
		if ((op == AggregateOperator.MAX && newAggregateField.compare(RelationalOperator.GT, currAggregateField))
				|| (op == AggregateOperator.MIN && newAggregateField.compare(RelationalOperator.LT, currAggregateField))) {
			tupleToUpdate.setField(aggregateFieldIndex, newAggregateField);
			counts.put(tupleToUpdate, counts.get(tupleToUpdate)+1);
		} 
	}

	/**
	 * Returns the result of the aggregation
	 * 
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		// your code here
		return tuples;
	}

}
