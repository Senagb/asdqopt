package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {
	Predicate[] given;
	Iterator iterLeft, iterRight;
	Schema currentSchema;
	Tuple nextTuple;

	/**
	 * Constructs a join, given the left and right iterators and join predicates
	 * (relative to the combined schema).
	 */
	public SimpleJoin(Iterator left, Iterator right, Predicate... preds) {
		// throw new UnsupportedOperationException("Not implemented");
		iterLeft = left;
		iterRight = right;
		given = preds;
		currentSchema = Schema
				.join(iterLeft.getSchema(), iterRight.getSchema());
		setSchema(currentSchema);
		nextTuple = null;

	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// throw new UnsupportedOperationException("Not implemented");
		indent(depth);
		String explaination = "SimpleJoin with predicates: ";
		for (int i = 0; i < given.length; i++) {
			if (i == 0)
				explaination += given[i].toString();
			else
				explaination += " OR " + given[i].toString();
		}
		System.out.println(explaination);
		iterLeft.explain(depth + 1);
		iterRight.explain(depth + 1);
	}

	/**
	 * Restarts the iterator, i.e. as if it were just constructed.
	 */
	public void restart() {
		// throw new UnsupportedOperationException("Not implemented");
		iterLeft.restart();
		iterRight.restart();
		nextTuple = null;
	}

	/**
	 * Returns true if the iterator is open; false otherwise.
	 */
	public boolean isOpen() {
		// throw new UnsupportedOperationException("Not implemented");
		return iterLeft.isOpen() && iterRight.isOpen();
	}

	/**
	 * Closes the iterator, releasing any resources (i.e. pinned pages).
	 */
	public void close() {
		// throw new UnsupportedOperationException("Not implemented");
		iterLeft.close();
		iterRight.close();
	}

	/**
	 * Returns true if there are more tuples, false otherwise.
	 */
	public boolean hasNext() {
		// throw new UnsupportedOperationException("Not implemented");
		if (!isOpen())
			return false;
		Tuple temp = null, temp1 = null;
		while (true) {
			if (temp == null) {
				if (!iterLeft.hasNext()) {
					nextTuple = null;
					return false;
				}

				temp = iterLeft.getNext();
			}

			while (iterRight.hasNext()) {
				temp1 = iterRight.getNext();
				Tuple joinTuple = Tuple.join(temp, temp1, currentSchema);

				boolean pass = false;

				for (int i = 0; i < given.length; i++) {
					if (given[i].evaluate(joinTuple)) {
						pass = true;
						break;
					}
				}

				if (pass) {
					nextTuple = joinTuple;
					return true;
				}
			}

			// Right depleted, restart
			temp = null; // Signal the left to fetch new tuple
			iterRight.restart();
		}

	}

	public Tuple getNext() throws IllegalStateException {
		return nextTuple;

	}
} // public class SimpleJoin extends Iterator
