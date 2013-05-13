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

	private Tuple popTillPass() {
		while (iter.hasNext()) {
			Tuple temp = iter.getNext();
			for (int i = 0; i < given.length; i++)
				if (given[i].evaluate(temp))
					return temp;
		}
		return null;
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		if (nextTuple == null)
			nextTuple = popTillPass();
		return nextTuple != null;
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() {
		Tuple ret = nextTuple;
		nextTuple = popTillPass();
		return ret;
	}
} // public class Selection extends Iterator
