package relop;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {
	Integer[] given;
	Iterator iter;
	Schema new_schema;

	/**
	 * Constructs a projection, given the underlying iterator and field numbers.
	 */
	public Projection(Iterator iter, Integer... fields) {
		// throw new UnsupportedOperationException("Not implemented");
		new_schema = new Schema(fields.length);
		for (int i = 0; i < fields.length; i++) {
			new_schema.initField(i, iter.getSchema(), fields[i]);
		}

		setSchema(new_schema);
		this.iter = iter;
		given = fields;
	}

	/**
	 * Gives a one-line explaination of the iterator, repeats the call on any
	 * child iterators, and increases the indent depth along the way.
	 */
	public void explain(int depth) {
		// throw new UnsupportedOperationException("Not implemented");
		indent(depth);
		String str = "Projection on fields: ";
		for (int i = 0; i < given.length; i++) {
			if (i == 0)
				str += new_schema.fieldName(i) + "(" + given[i] + ")";
			else
				str += "  " + new_schema.fieldName(i) + "(" + given[i] + ")";
		}
		System.out.println(str);
		iter.explain(depth + 1);
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
		if (!isOpen())
			return false;
		return iter.hasNext();
	}

	/**
	 * Gets the next tuple in the iteration.
	 * 
	 * @throws IllegalStateException
	 *             if no more tuples
	 */
	public Tuple getNext() throws IllegalStateException {
		// throw new UnsupportedOperationException("Not implemented");
		Tuple temp = iter.getNext();
		Tuple res = new Tuple(new_schema);
		for (int i = 0; i < given.length; i++) {
			res.setField(i, temp.getField(given[i]));
		}
		return res;
	}

} // public class Projection extends Iterator
