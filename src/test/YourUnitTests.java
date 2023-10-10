package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.NoSuchElementException;

import org.junit.Before;
import org.junit.Test;

import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.HeapPage;
import hw1.Tuple;
import hw1.TupleDesc;
import hw1.Type;

public class YourUnitTests {

	private HeapFile hf;
	private TupleDesc td;
	private Catalog c;
	private HeapPage hp;

	// Copied over from HW1Tests to help with random types here
	private Type[] randomTypes(int n) {
		Type[] t = new Type[n];
		for (int i = 0; i < n; i++) {
			if (Math.random() > .5) {
				t[i] = Type.INT;
			} else {
				t[i] = Type.STRING;
			}
		}

		return t;
	}

	@Before
	public void setup() {

		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(),
					StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}

		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");

		int tableId = c.getTableId("test");
		td = c.getTupleDesc(tableId);
		hf = c.getDbFile(tableId);
		hp = hf.readPage(0);
	}
	
	/*
	 * This test makes sure that getFieldName returns the right value
	 * when passed a valid index
	 */
	@Test
	public void testTupleDescGetFieldNameSuccess() {

		Type[] types = new Type[] { Type.INT, Type.STRING };
		String[] fields = new String[] { "TESTFIELD1", "TESTFIELD2" };
		TupleDesc td = new TupleDesc(types, fields);
		Tuple tup = new Tuple(td);
		
		assertTrue(tup.getDesc().getFieldName(0).equals("TESTFIELD1"));

	}


	/*
	 * This test makes sure that an exception is thrown if you try to get a field
	 * name that is not a valid index
	 */
	@Test
	public void testTupleDescGetFieldNameFailure() {

		Type[] types = new Type[] { Type.INT, Type.STRING };
		String[] fields = new String[] { "TESTFIELD1", "TESTFIELD2" };
		TupleDesc td = new TupleDesc(types, fields);
		Tuple tup = new Tuple(td);

		Exception exception = assertThrows(NoSuchElementException.class, () -> {
			tup.getDesc().getFieldName(10);
		});

	}

	/*
	 * This test makes sure that an exception is thrown if you try to get a type
	 * that is not a valid index
	 */
	@Test
	public void testTupleDescGetType() {

		Type[] types = new Type[] { Type.INT, Type.STRING };
		String[] fields = new String[] { "TESTFIELD1", "TESTFIELD2" };
		TupleDesc td = new TupleDesc(types, fields);
		Tuple tup = new Tuple(td);

		Exception exception = assertThrows(NoSuchElementException.class, () -> {
			tup.getDesc().getType(10);
		});

	}

	/*
	 * This test is for the addTuple method in HeapPage and it explicitly tests two
	 * things: 1) It makes sure the right exception is thrown for trying to input a
	 * null tuple 2) Makes sure the right exception is thrown if for trying to input
	 * a Tuple that does not have a matching TupleDesc
	 */
	@Test
	public void testHeapPageAddTuple() throws Exception {

		// Null tuple test
		Tuple nullTup = null;
		Exception nullException = assertThrows(NoSuchElementException.class, () -> {
			hp.addTuple(nullTup);
		});

		// Not matching TupleDesc test
		Type[] types = randomTypes(5);
		String[] fields = new String[] { "TESTFIELD1", "TESTFIELD2", "TESTFIELD3", "TESTFIELD4", "TESTFIELD5" };
		TupleDesc td = new TupleDesc(types, fields);
		Tuple notMatchingTDTup = new Tuple(td);

		Exception notMatchingTDException = assertThrows(Exception.class, () -> {
			hp.addTuple(notMatchingTDTup);
		});

	}

}
