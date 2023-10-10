package hw1;

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/*
 * Student 1 name: Leoul Gezu
 * Student 2 name: Yab Lemma
 * Date: 9/16/2023
 */

/**
 * This class represents a tuple that will contain a single row's worth of
 * information from a table. It also includes information about where it is
 * stored
 * 
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {

	private TupleDesc td;
	private int pid; // page ID
	private int id;
	private HashMap<Integer, Field> tupleData;

	/**
	 * Creates a new tuple with the given description
	 * 
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		// your code here
		this.td = t;
		this.tupleData = new HashMap<Integer, Field>();
	}

	public TupleDesc getDesc() {
		// your code here
		return this.td;
	}

	/**
	 * retrieves the page id where this tuple is stored
	 * 
	 * @return the page id of this tuple
	 */
	public int getPid() {
		return pid;
	}

	public void setPid(int pid) {
		// your code here
		this.pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * 
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		// your code here
		return this.id;
	}

	public void setId(int id) {
		// your code here
		this.id = id;
	}

	public void setDesc(TupleDesc td) {
		// your code here;
		this.td = td;
	}
	
	public HashMap<Integer, Field> getTupleData() {
		return this.tupleData;
	}
	
	public void setTupleData(HashMap<Integer, Field> tupleData) {
		this.tupleData = tupleData;
	}

	/**
	 * Stores the given data at the i-th field
	 * 
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		// your code here
		tupleData.put(i, v);
	}

	public Field getField(int i) {
		// your code here
		return tupleData.get(i);
	}

	/**
	 * Creates a string representation of this tuple that displays its contents. You
	 * should convert the binary data into a readable format (i.e. display the ints
	 * in base-10 and convert the String columns to readable text).
	 */
	public String toString() {
		// your code here
		String output = "";
		
		for (Integer i: tupleData.keySet()) {
			byte[] bytes = tupleData.get(i).toByteArray();
			if (tupleData.get(i) instanceof IntField) {
				int intValue = ((IntField) tupleData.get(i)).getValue();
				output += intValue;
			} else if (tupleData.get(i) instanceof StringField) {
				String stringValue = ((StringField) tupleData.get(i)).getValue();
				output += stringValue;
			}
			output += ",";
		}
		// Cut off annoying trailing comma
		return output.substring(0, output.length()-1);
	}
}