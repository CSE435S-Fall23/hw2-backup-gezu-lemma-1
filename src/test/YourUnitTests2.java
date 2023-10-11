package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw1.AggregateOperator;
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.Relation;
import hw1.RelationalOperator;
import hw1.TupleDesc;

public class YourUnitTests2 {

	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;

	@Before
	public void setup() {

		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(),
					StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(),
					StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}

		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");

		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);

		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");

		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}


	@Test
	public void testAggregateCount() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.COUNT, false);

		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField) (ar.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 8);
	}

	@Test
	public void testGroupByCount() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.aggregate(AggregateOperator.COUNT, true);
		System.out.println("predicted" + ar.getTuples().size());
		assertTrue(ar.getTuples().size() == 4);
		assertTrue(ar.getTuples().get(0).getField(0).equals(new IntField(530)));
		assertTrue(ar.getTuples().get(0).getField(1).equals(new IntField(5)));

	}
	
	@Test
	public void testAggregateMin() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.MIN, false);

		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField) (ar.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 1);
	}

	@Test
	public void testGroupByMin() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.aggregate(AggregateOperator.MIN, true);

		assertTrue(ar.getTuples().size() == 4);
		assertTrue(ar.getTuples().get(0).getField(0).equals(new IntField(530)));
		assertTrue(ar.getTuples().get(0).getField(1).equals(new IntField(1)));

	}
	
	@Test
	public void testAggregateMax() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.MAX, false);

		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField) (ar.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 8);
	}

	@Test
	public void testGroupByMax() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.aggregate(AggregateOperator.MAX, true);

		assertTrue(ar.getTuples().size() == 4);
		assertTrue(ar.getTuples().get(0).getField(0).equals(new IntField(530)));
		assertTrue(ar.getTuples().get(0).getField(1).equals(new IntField(7)));

	}
	
	@Test
	public void testAggregateAvg() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ArrayList<Integer> c = new ArrayList<Integer>();
		c.add(1);
		ar = ar.project(c);
		ar = ar.aggregate(AggregateOperator.AVG, false);
		
		assertTrue(ar.getTuples().size() == 1);
		IntField agg = (IntField) (ar.getTuples().get(0).getField(0));
		assertTrue(agg.getValue() == 5);
	}

	@Test
	public void testGroupByAvg() {
		Relation ar = new Relation(ahf.getAllTuples(), atd);
		ar = ar.aggregate(AggregateOperator.AVG, true);

		assertTrue(ar.getTuples().size() == 4);
		System.out.println(ar.getTuples().get(0).getField(0));
		assertTrue(ar.getTuples().get(0).getField(0).equals(new IntField(530)));
		assertTrue(ar.getTuples().get(0).getField(1).equals(new IntField(5)));

	}


}
