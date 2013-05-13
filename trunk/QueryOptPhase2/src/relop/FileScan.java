package relop;

import global.RID;
import heap.*;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {
	HeapScan scanner;
	Schema fileSchema;
	HeapFile currentFile;
	boolean isOpen = false;
	RID lastRID;

	/**
	 * Constructs a file scan, given the schema and heap file.
	 */
	public FileScan(Schema schema, HeapFile file) {
		// throw new UnsupportedOperationException("Not implemented");
		setSchema(schema);
		scanner = file.openScan();
		fileSchema = schema;
		currentFile = file;
		isOpen = true;
		lastRID = new RID();
	}

	/**
	 * Gives a one-line explanation of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		indent(depth);
		System.out.println("FileScan of file: " + currentFile.toString());
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		// throw new UnsupportedOperationException("Not implemented");
		if (isOpen) {
			scanner.close();
			isOpen = false;
		}
		scanner = currentFile.openScan();
		isOpen = true;
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		// throw new UnsupportedOperationException("Not implemented");
		return isOpen;
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		// throw new UnsupportedOperationException("Not implemented");
		isOpen = false;
		scanner.close();
		fileSchema = null;
		currentFile = null;
		lastRID = null;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		// throw new UnsupportedOperationException("Not implemented");
		if (isOpen)
			return scanner.hasNext();
		return false;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		// throw new UnsupportedOperationException("Not implemented");
		if (!isOpen)
			throw new IllegalStateException("File scan closed");

		RID rid = new RID();
		byte[] returened = scanner.getNext(rid);
		lastRID = rid;
		Tuple returenedTuple = new Tuple(fileSchema, returened);
		return returenedTuple;

	}

	/**
	 * Gets the RID of the last tuple returned.
	 */
	public RID getLastRID() {
		// throw new UnsupportedOperationException("Not implemented");
		return lastRID;
	}
} // public class FileScan extends Iterator
