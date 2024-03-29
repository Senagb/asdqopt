package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.BucketScan;
import index.HashIndex;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {
	BucketScan scanner;
	Schema fileSchema;
	HeapFile currentFile;
	HashIndex myIndex;
	boolean isOpen = false;

	/**
	 * Constructs an index scan, given the hash index and schema.
	 */
	public IndexScan(Schema schema, HashIndex index, HeapFile file) {
		// throw new UnsupportedOperationException("Not implemented");
		setSchema(schema);
		scanner = index.openScan();
		fileSchema = schema;
		currentFile = file;
		isOpen = true;
		myIndex = index;
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
		scanner = myIndex.openScan();
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

	/**
	 * Gets the key of the last tuple returned.
	 */
	public SearchKey getLastKey() {
		// throw new UnsupportedOperationException("Not implemented");
		return scanner.getLastKey();
	}

	/**
	 * Returns the hash value for the bucket containing the next tuple, or
	 * maximum number of buckets if none.
	 */
	public int getNextHash() {
		// throw new UnsupportedOperationException("Not implemented");
		return scanner.getNextHash();
	}

} // public class IndexScan extends Iterator
