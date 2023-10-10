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

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		// your code here
		this.o = o;
		this.groupBy = groupBy;
		// TODO: delete this comment
		if (groupBy) {
			System.out.println("GROUPBY");
		} else {
			System.out.println("NOT GROUPBY");
		}
		this.td = td;
		tuples = new ArrayList<Tuple>();
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
			handleAvg(t);
			break;
		case COUNT:
			handleCount(t);
			break;
		case SUM:
			handleSum(t);
			break;
		}
	}

	private void handleSum(Tuple t) {
		if (tuples.isEmpty()) {
			tuples.add(t);
			return;
		}

		if (!groupBy) {
			int newAggregateValue = ((IntField) t.getField(0)).getValue();
			Tuple curr = tuples.get(0);
			int currAggregateValue = ((IntField) curr.getField(0)).getValue();
			curr.setField(0, new IntField(newAggregateValue + currAggregateValue));
			return;
		}

		int newGroupByValue = ((IntField) t.getField(0)).getValue();
		int newAggregateValue = ((IntField) t.getField(1)).getValue();
		for (Tuple curr : tuples) {
			int currGroupByValue = ((IntField) curr.getField(0)).getValue();
			if (newGroupByValue == currGroupByValue) {
				int currAggregateValue = ((IntField) curr.getField(1)).getValue();
				curr.setField(1, new IntField(newAggregateValue + currAggregateValue));
				return;
			}
		}
		tuples.add(t);
	}

	private void handleCount(Tuple t) {
		if (tuples.isEmpty()) {
			t.setField(1, new IntField(1));
			tuples.add(t);
			return;
		}

		if (!groupBy) {
			int newAggregateValue = ((IntField) t.getField(0)).getValue();
			Tuple curr = tuples.get(0);
			int currAggregateValue = ((IntField) curr.getField(0)).getValue();
			curr.setField(0, new IntField(currAggregateValue+1));
			return;
		}

		int newGroupByValue = ((IntField) t.getField(0)).getValue();
		int newAggregateValue = ((IntField) t.getField(1)).getValue();
		for (Tuple curr : tuples) {
			int currGroupByValue = ((IntField) curr.getField(0)).getValue();
			if (newGroupByValue == currGroupByValue) {
				int currAggregateValue = ((IntField) curr.getField(1)).getValue();
				curr.setField(1, new IntField(currAggregateValue+1));
				return;
			}
		}
		tuples.add(t);

	}

	private void handleAvg(Tuple t) {
		// TODO Auto-generated method stub

	}

	private void handleMinAndMax(Tuple t, AggregateOperator op) {
		if (tuples.isEmpty()) {
			tuples.add(t);
			return;
		}

		if (!groupBy) {
			int newAggregateValue = ((IntField) t.getField(0)).getValue();
			Tuple curr = tuples.get(0);
			int currAggregateValue = ((IntField) curr.getField(0)).getValue();
			curr.setField(0, new IntField(op == AggregateOperator.MIN ? Math.min(newAggregateValue, currAggregateValue)
					: Math.max(newAggregateValue, currAggregateValue)));
			return;
		}

		int newGroupByValue = ((IntField) t.getField(0)).getValue();
		int newAggregateValue = ((IntField) t.getField(1)).getValue();
		for (Tuple curr : tuples) {
			int currGroupByValue = ((IntField) curr.getField(0)).getValue();
			if (newGroupByValue == currGroupByValue) {
				int currAggregateValue = ((IntField) curr.getField(1)).getValue();
				curr.setField(1,
						new IntField(op == AggregateOperator.MIN ? Math.min(newAggregateValue, currAggregateValue)
								: Math.max(newAggregateValue, currAggregateValue)));
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
