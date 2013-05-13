package relop;

/**
 * The simplest of all join algorithms: nested loops (see textbook, 3rd edition,
 * section 14.4.1, page 454).
 */
public class SimpleJoin extends Iterator {
	Predicate[] given;
	Iterator iterLeft, iterRight;
	Schema currentSchema;

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
		iterLeft.restart();
		iterRight.restart();
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
		return iterLeft.hasNext()||iterRight.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	Tuple lastLeft=null;
	public Tuple getNext() throws IllegalStateException {
		// throw new UnsupportedOperationException("Not implemented");
		Tuple temp = null, temp1 = null;
		boolean breakLoop = false;
		if(!iterRight.hasNext())
		{
			iterRight.restart();
			lastLeft=iterLeft.getNext();
		}
		if(lastLeft==null&&hasNext())
			lastLeft=iterLeft.getNext();
		//temp = iterLeft.getNext();

//		breakLoop = false;
//		while (!breakLoop) {
			temp1 = iterRight.getNext();
//			Tuple res = Tuple.join(temp, temp1, currentSchema);
//			breakLoop = true;
//			for (int i = 0; i < given.length; i++) {
//				if (!given[i].evaluate(res)) {
//					breakLoop = false;
//					break;
//				}
//			}
//		}
		//iterRight.restart();
		return Tuple.join(lastLeft, temp1, currentSchema);

	}
} // public class SimpleJoin extends Iterator
