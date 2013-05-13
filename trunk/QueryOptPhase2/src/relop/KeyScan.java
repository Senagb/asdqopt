package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {
	HashScan scanner;
	Schema fileSchema;
	HeapFile currentFile;
	HashIndex myIndex;
	boolean isOpen = false;
	SearchKey myKey;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
		// throw new UnsupportedOperationException("Not implemented");
		setSchema(schema);
		scanner = index.openScan(key);
		fileSchema = schema;
		currentFile = file;
		isOpen = true;
		myIndex = index;
		myKey = key;
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		throw new UnsupportedOperationException("Not implemented");
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		// throw new UnsupportedOperationException("Not implemented");
		scanner.close();
		scanner = myIndex.openScan(myKey);

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
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		// throw new UnsupportedOperationException("Not implemented");
		return scanner.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() throws IllegalStateException {
		// throw new UnsupportedOperationException("Not implemented");
		RID rid = new RID();
		rid = scanner.getNext();
		byte[] returened = currentFile.selectRecord(rid);
		Tuple returenedTuple = new Tuple(fileSchema, returened);
		return returenedTuple;
	}
} // public class KeyScan extends Iterator
