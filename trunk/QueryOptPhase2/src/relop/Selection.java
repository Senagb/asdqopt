package relop;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by AND operators.
 */
public class Selection extends Iterator {
	Predicate[] given;
	Iterator iter;
	Schema currentSchema;
	Tuple nextTuple;

	/**
	 * Constructs a selection, given the underlying iterator and predicates.
	 */
	public Selection(Iterator iter, Predicate... preds) {
		// throw new UnsupportedOperationException("Not implemented");
		setSchema(iter.getSchema());
		given = preds;
		this.iter = iter;
		currentSchema = iter.getSchema();

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
		iter.restart();

	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		// throw new UnsupportedOperationException("Not implemented");
		return iter.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		// throw new UnsupportedOperationException("Not implemented");
		iter.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		// throw new UnsupportedOperationException("Not implemented");
		while (iter.hasNext()) {
			Tuple temp = iter.getNext();
			boolean pass = true;
			for (int i = 0; i < given.length; i++) {
				if (!given[i].evaluate(temp)) {
					pass = false;
					break;
				}
			}
			if (pass) {
				nextTuple = temp;
				return true;
			}
		}
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

		return nextTuple;
	}
} // public class Selection extends Iterator
