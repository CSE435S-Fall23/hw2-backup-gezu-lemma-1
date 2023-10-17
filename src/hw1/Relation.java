package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		// TODO: handle edge cases and field value bounds?
		ArrayList<Tuple> output = new ArrayList<>();
		for (Tuple t: this.tuples) {
			if (t.getField(field).compare(op, operand)) {
				output.add(t);
			}
		}
		this.tuples = output;
		return this;
		
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		// TODO: handle edge cases and field value bounds?
		String[] updatedFields = this.td.getFields();
		for (Integer field: fields) {
			updatedFields[field] = names.get(field);
		}
		this.td.setFields(updatedFields);
		
		// Update all the tuples with the new TupleDesc
		for (Tuple t: this.tuples) {
			t.setDesc(this.td);
		}
		
		
		return this;
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		
		// Grab the specified fields and types from the input to make new TupleDesc
		Type[] newTypes = new Type[fields.size()];
		String[] newFields = new String[fields.size()];
		for (int i=0; i<fields.size(); i++) {
			newTypes[i] = this.td.getType(fields.get(i));
			newFields[i] = this.td.getFieldName(fields.get(i));
		}
		
		TupleDesc newTd = new TupleDesc(newTypes, newFields);
		
		// Create new tuples with just the new fields
		ArrayList<Tuple> newTuples = new ArrayList<>();
		
		for (Tuple t: tuples) {
			Tuple newTuple = new Tuple(newTd);
			// Set the fields for the new tuples
			for (int i = 0; i<fields.size(); i++) {
				newTuple.setField(i, t.getField(fields.get(i)));
			}
			newTuples.add(newTuple);
		}
		
		return new Relation(newTuples, newTd);

	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		Type[] newTypes = new Type[this.td.getFields().length + other.td.getFields().length];
		String[] newFields = new String[this.td.getFields().length + other.td.getFields().length];
		
		int i = 0;
		while (i < this.td.getFields().length) {
			newTypes[i] = this.td.getType(i);
			newFields[i] = this.td.getFieldName(i);
			i++;
		}
		int j = 0;
		while (j < other.td.getFields().length) {
			newTypes[i+j] = other.td.getType(j);
			newFields[i+j] = other.td.getFieldName(j);
			j++;
		}
		
		TupleDesc newTd = new TupleDesc(newTypes, newFields);
		ArrayList<Tuple> newTuples = new ArrayList<>();
		
		for (int k = 0; k < this.tuples.size(); k++) {
			for (int l = 0; l < other.getTuples().size(); l++) {
				if (tuples.get(k).getField(field1).compare(RelationalOperator.EQ, other.getTuples().get(l).getField(field2))) {
					Tuple newTuple = new Tuple(newTd);
					int n = 0;
					while (n < this.td.getFields().length) {
						newTuple.setField(n, tuples.get(k).getField(n));
						n++;
					}
					for (int m = 0; m < other.td.getFields().length; m++) {
						newTuple.setField(m+n, other.getTuples().get(l).getField(m));
					}
					newTuples.add(newTuple);
				}
			}
		}
		return new Relation(newTuples, newTd);
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 * @throws Exception 
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) throws Exception {
		//your code here
		Aggregator agg = new Aggregator(op, groupBy, td);
		for (Tuple t: this.tuples) {
			agg.merge(t);
		}
		return new Relation(agg.getResults(), td);
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
    // Setter for tuples
    public void setTuples(ArrayList<Tuple> tuples) {
        this.tuples = tuples;
    }

    // Setter for td (TupleDesc)
    public void setDesc(TupleDesc td) {
        this.td = td;
    }
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		String output = "";
		output += this.td.toString() + "\n";
		for (Tuple t: tuples) {
			output += t;
		}
		return output;		
	}
}
