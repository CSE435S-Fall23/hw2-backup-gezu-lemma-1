package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

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
	 */
	public void merge(Tuple t) {
		// your code here
		switch (o) {
		case MAX:
			handleMinAndMax(t, AggregateOperator.MAX);
			break;
		case MIN:
			handleMinAndMax(t, AggregateOperator.MIN);
			break;
		case AVG:
			try { handleAvg(t); } catch (Exception e) { e.printStackTrace(); }
			break;
		case COUNT:
			handleCount(t);
			break;
		case SUM:
			try { handleSum(t); } catch (Exception e) { e.printStackTrace(); }
			break;
		}
	}

	private void handleSum(Tuple t) throws Exception {
		if (!groupBy && t.getField(0).getType() == Type.STRING) {
			throw new Exception("SUM operator is invalid for string field type");
		}
		
		if (tuples.isEmpty()) {
			tuples.add(t);
			counts.put(t,1);
			return;
		}

		if (!groupBy) {
			int newAggregateValue = ((IntField) t.getField(0)).getValue();
			Tuple curr = tuples.get(0);
			int currAggregateValue = ((IntField) curr.getField(0)).getValue();
			curr.setField(0, new IntField(newAggregateValue + currAggregateValue));
			counts.put(curr, counts.get(curr)+1);
			return;
		}
		
		Field newGBYField = t.getField(0);
		for (Tuple curr: tuples) {
			Field currGBYField = curr.getField(0);
			if (newGBYField.compare(RelationalOperator.EQ, currGBYField)) {
				// We've already confirmed that the aggregate type is not a string so we can safely cast here
				int currAggregateValue = ((IntField) curr.getField(1)).getValue();
				int newAggregateValue = ((IntField) t.getField(1)).getValue();
				curr.setField(1, new IntField(newAggregateValue + currAggregateValue));
				counts.put(curr, counts.get(curr)+1);
				return;
			}
		}
		tuples.add(t);
	}

	private void handleCount(Tuple t) {
		if (tuples.isEmpty()) {
			t.setField(1, new IntField(1));
			tuples.add(t);
			counts.put(t,1);
			return;
		}

		if (!groupBy) {
			Tuple curr = tuples.get(0);
			int currAggregateValue = ((IntField) curr.getField(0)).getValue();
			curr.setField(0, new IntField(currAggregateValue+1));
			counts.put(curr, counts.get(curr)+1);
			return;
		}
		
		Field newGBYField = t.getField(0);
		for (Tuple curr: tuples) {
			Field currGBYField = curr.getField(0);
			if (newGBYField.compare(RelationalOperator.EQ, currGBYField)) {
				int currAggregateValue = ((IntField) curr.getField(1)).getValue();
				curr.setField(1, new IntField(currAggregateValue+1));
				counts.put(curr, counts.get(curr)+1);
				return;
			}
		}
		tuples.add(t);
	}

	private void handleAvg(Tuple t) throws Exception {
		if (!groupBy && t.getField(0).getType() == Type.STRING) {
			throw new Exception("AVG operator is invalid for string field type");
		}
		
		if (tuples.isEmpty()) {
			tuples.add(t);
			counts.put(t,1);
			return;
		}
		
		if (!groupBy) {
			Tuple curr = tuples.get(0);
			int currAggregateValue = ((IntField) curr.getField(0)).getValue();
			int newAggregateValue = ((IntField) t.getField(0)).getValue();
			curr.setField(0, new IntField(Math.round(recalculateAverage(counts.get(curr), currAggregateValue, newAggregateValue))));
			counts.put(curr, counts.get(curr)+1);
			return;
		}
		
		Field newGBYField = t.getField(0);
		for (Tuple curr: tuples) {
			Field currGBYField = curr.getField(0);
			if (newGBYField.compare(RelationalOperator.EQ, currGBYField)) {
				// We've already confirmed that the aggregate type is not a string so we can safely cast here
				int currAggregateValue = ((IntField) curr.getField(1)).getValue();
				int newAggregateValue = ((IntField) t.getField(1)).getValue();
				curr.setField(1, new IntField(Math.round(recalculateAverage(counts.get(curr), currAggregateValue, newAggregateValue))));
				counts.put(curr, counts.get(curr)+1);
				return;
			}
		}
		tuples.add(t);
	}
	
	private Float recalculateAverage(Integer count, Integer prevAverage, Integer newValue) {
		Float newAverage =  ((float)(count*prevAverage + newValue)/(float)(count + 1));
		return newAverage;
	}

	private void handleMinAndMax(Tuple t, AggregateOperator op) {
		if (tuples.isEmpty()) {
			tuples.add(t);
			counts.put(t,1);
			return;
		}

		if (!groupBy) {
			Tuple curr = tuples.get(0);
			Field newAggregateField = t.getField(0);
			Field currAggregateField = curr.getField(0);
			if ((op == AggregateOperator.MAX && newAggregateField.compare(RelationalOperator.GT, currAggregateField))
					|| (op == AggregateOperator.MIN && newAggregateField.compare(RelationalOperator.LT, currAggregateField))) {
				curr.setField(0, newAggregateField);
			} 
			counts.put(curr, counts.get(curr)+1);
			return;
		}
		
		Field newGBYField = t.getField(0);
		for (Tuple curr: tuples) {
			Field currGBYField = curr.getField(0);
			if (newGBYField.compare(RelationalOperator.EQ, currGBYField)) {
				Field newAggregateField = t.getField(1);
				Field currAggregateField = curr.getField(1);
				if ((op == AggregateOperator.MAX && newAggregateField.compare(RelationalOperator.GT, currAggregateField))
						|| (op == AggregateOperator.MIN && newAggregateField.compare(RelationalOperator.LT, currAggregateField))) {
					curr.setField(1, newAggregateField);
				} 
				counts.put(curr, counts.get(curr)+1);
				return;
			}
		}
		tuples.add(t);
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
