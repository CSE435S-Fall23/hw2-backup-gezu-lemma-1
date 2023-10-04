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
			// Set the fields from the new tuple
			for (int fieldIndex: fields) {
				newTuple.setField(fieldIndex, t.getField(fieldIndex));
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
		return null;
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		return null;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		return null;
	}
}
