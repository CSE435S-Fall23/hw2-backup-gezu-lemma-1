package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;

/*
 * Student 1 name: Leoul Gezu
 * Student 2 name: Yab Lemma
 * Date: 9/16/2023
 */

/**
 * A heap file stores a collection of tuples. It is also responsible for
 * managing pages. It needs to be able to manage page creation as well as
 * correctly manipulating pages when tuples are added or deleted.
 * 
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {

	public static final int PAGE_SIZE = 4096;
	private File file;
	private TupleDesc type;

	/**
	 * Creates a new heap file in the given location that can accept tuples of the
	 * given type
	 * 
	 * @param f     location of the heap file
	 * @param types type of tuples contained in the file
	 */
	public HeapFile(File f, TupleDesc type) {
		// your code here
		this.file = f;
		this.type = type;
	}

	public File getFile() {
		// your code here
		return file;
	}

	public TupleDesc getTupleDesc() {
		// your code here
		return type;
	}
	
	public void setFile(File file) {
		this.file = file;
	}

	public void setTupleDesc(TupleDesc type) {
		this.type = type;
	}


	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a
	 * RandomAccessFile object should be used here.
	 * 
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		// your code here
		long pageStart = PAGE_SIZE * id;

		try {
			RandomAccessFile raf = new RandomAccessFile(this.file, "r");
			raf.seek(pageStart);
			byte[] data = new byte[PAGE_SIZE];
			raf.read(data);
			raf.close();
			return new HeapPage(id, data, this.getId());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;  //TODO: try not to return null
	}

	/**
	 * Returns a unique id number for this heap file. Consider using the hash of the
	 * File itself.
	 * 
	 * @return
	 */
	public int getId() {
		// your code here
		return file.hashCode();
	}

	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the
	 * file, a RandomAccessFile object should be used in this method.
	 * 
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		// your code here
		long pageStart = PAGE_SIZE * p.getId();
		byte[] data = p.getPageData();

		try {
			RandomAccessFile raf = new RandomAccessFile(this.file, "rw");
			raf.seek(pageStart);
			raf.write(data);
			raf.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating
	 * a new page if all others are full. It then passes the tuple to this page to
	 * be stored. It then writes the page to disk (see writePage)
	 * 
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		// your code here
		for (int i = 0; i < getNumPages(); i++) {
			HeapPage current = this.readPage(i);
			// Check for page with open slot
			for (int j = 0; j < current.getNumSlots(); j++) {
				if (!current.slotOccupied(j)) {
					try {
						current.addTuple(t);
						writePage(current);
						return current;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		
		// Create a new page
		try {
			HeapPage newPage = new HeapPage(this.getNumPages(), new byte[PAGE_SIZE], this.getId());
			newPage.addTuple(t);
			writePage(newPage);
			return newPage;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;  //TODO: try not to return null
	}

	/**
	 * This method will examine the tuple to find out where it is stored, then
	 * delete it from the proper HeapPage. It then writes the modified page to disk.
	 * 
	 * @param t the Tuple to be deleted
	 */
	public void deleteTuple(Tuple t) {
		// your code here
		HeapPage current = this.readPage(t.getPid());
		try {
			current.deleteTuple(t);
			this.writePage(current);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * 
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		// your code here
		ArrayList<Tuple> allTuples = new ArrayList<>();
		for (int i = 0; i < getNumPages(); i++) {
			Iterator<Tuple> iterator = this.readPage(i).iterator();
			iterator.forEachRemaining((Tuple t) -> {
				allTuples.add(t);
			});
		}
		return allTuples;
	}

	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * 
	 * @return the number of pages
	 */
	public int getNumPages() {
		// your code here
		return (int) Math.ceil((file.length() / PAGE_SIZE));
	}
}