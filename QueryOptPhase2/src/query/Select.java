package query;

import heap.HeapFile;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.PriorityQueue;

import parser.AST_Select;
import relop.FileScan;
import relop.Iterator;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if validation fails
	 */

	private Iterator selectIter = null;
	private boolean isExplain = false;

	public Select(AST_Select tree) throws QueryException {
		// init
		String[] tableName = tree.getTables();
		String[] selectColName = tree.getColumns();
		Predicate[][] andPreds = tree.getPredicates();
		// check parameters
		if (tableName == null || tableName.length == 0)
			throw new QueryException("Tables array invalid in SELECT");
		if (selectColName == null)
			throw new QueryException("Columns array invalid in SELECT");
		// create schemas
		Schema[] tableSchema = new Schema[tableName.length];
		for (int i = 0; i < tableSchema.length; i++)
			tableSchema[i] = QueryCheck.tableExists(tableName[i]);
		// schema join
		Schema finalSchema = tableSchema[0];
		for (int i = 1; i < tableSchema.length; i++)
			finalSchema = Schema.join(finalSchema, tableSchema[i]);
		// columns
		Integer[] colids = null;
		colids = new Integer[selectColName.length];
		for (int i = 0; i < selectColName.length; i++)
			colids[i] = QueryCheck.columnExists(finalSchema, selectColName[i]);
		// check predicates
		QueryCheck.predicates(finalSchema, andPreds);
		// prioritise predicates
		PriorityQueue<Pair<HashSet<Integer>, Predicate[]>> predsQueue = prioritisePreds(andPreds, tableSchema, finalSchema);
		// iterators
		Iterator[] tableIter = new Iterator[tableName.length];
		for (int tindex = 0; tindex < tableName.length; tindex++)
			tableIter[tindex] = new FileScan(tableSchema[tindex], new HeapFile(tableName[tindex]));
		// join & select
		while (!predsQueue.isEmpty()) {
			Pair<HashSet<Integer>, Predicate[]> p = predsQueue.poll();
			Iterator iter = join(p.k, tableIter);
			iter = select(iter, p.v);
			replace(tableIter, p.k, iter);
		}
		// assert correct
		for (int i = 1; i < tableIter.length; i++)
			if (tableIter[i] != tableIter[i - 1])
				throw new QueryException("Unexpected Error!");
		// PROJECT
		selectIter = tableIter[0];
		if (selectColName.length > 0)
			selectIter = new Projection(selectIter, colids);
		isExplain = tree.isExplain;
		System.out.println(selectIter);
	} // public Select(AST_Select tree) throws QueryException

	private Iterator select(Iterator iter, Predicate[] preds) {
		System.out.println("selecting " + iter + Arrays.toString(preds));
		return new Selection(iter, preds);
	}

	private void replace(Iterator[] tableIter, HashSet<Integer> k, Iterator iter) {
		for (int i = 0; i < tableIter.length; i++)
			if (k.contains(i))
				tableIter[i] = iter;
	}

	private Iterator join(HashSet<Integer> set, Iterator[] tableIter) {
		// remove repetition
		HashSet<Integer> noRepeatSet = new HashSet<Integer>(set);
		Integer[] noRepeatSetArray = new Integer[noRepeatSet.size()];
		noRepeatSet.toArray(noRepeatSetArray);
		for (int i = 0; i < noRepeatSetArray.length; i++)
			for (int j = i + 1; j < noRepeatSetArray.length; j++)
				if (tableIter[noRepeatSetArray[i]] == tableIter[noRepeatSetArray[j]])
					noRepeatSet.remove(j);
		// join
		Integer[] setArray = new Integer[noRepeatSet.size()];
		noRepeatSet.toArray(setArray);
		Iterator iter = tableIter[setArray[0]];
		if (setArray.length > 1) {
			System.out.print("joining " + iter);
			for (int i = 1; i < setArray.length; i++) {
				System.out.print(" " + tableIter[setArray[i]]);
				iter = new SimpleJoin(iter, tableIter[setArray[i]]);
			}
			System.out.println();
		}
		return iter;
	}

	private PriorityQueue<Pair<HashSet<Integer>, Predicate[]>> prioritisePreds(Predicate[][] preds, Schema[] schema, Schema finalSchem) {
		if (preds.length == 0)
			return new PriorityQueue<Pair<HashSet<Integer>, Predicate[]>>();
		PriorityQueue<Pair<HashSet<Integer>, Predicate[]>> q = new PriorityQueue<Pair<HashSet<Integer>, Predicate[]>>(preds.length, new CompareMyPair());
		for (int i = 0; i < preds.length; i++) {
			HashSet<String> cols = getCols(preds[i], finalSchem);
			HashSet<Integer> tables = new HashSet<Integer>();
			for (int j = 0; j < schema.length; j++)
				if (contains(schema[j].fieldNames(), cols))
					tables.add(j);
			q.add(new Pair<HashSet<Integer>, Predicate[]>(tables, preds[i]));
		}
		return q;
	}

	private boolean contains(String[] a, String s) {
		for (int i = 0; i < a.length; i++)
			if (s.equals(a[i]))
				return true;
		return false;
	}

	private boolean contains(String[] a, HashSet<String> s) {
		for (int i = 0; i < a.length; i++)
			if (s.contains(a[i]))
				return true;
		return false;
	}

	private HashSet<String> getCols(Predicate[] predicates, Schema schema) {
		HashSet<String> set = new HashSet<String>();
		for (int i = 0; i < predicates.length; i++) {
			String[] tok = predicates[i].toString().split(" ");
			for (int j = 0; j < tok.length; j++)
				if (tok[j].length() > 0 && contains(schema.fieldNames(), tok[j]))
					set.add(tok[j]);
		}
		return set;
	}

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		int retval = 0;

		if (selectIter != null) {
			if (isExplain) {
				selectIter.explain(0);
				System.out.println("SELECT Query Explained.");
			}
			else {
				retval = selectIter.execute();
				System.out.println(retval + " row(s) selected.");
			}
		}
	} // public void execute()

	protected static class Pair<K, V> {
		public K k;
		public V v;

		public Pair(K _k, V _v) {
			k = _k;
			v = _v;
		}

		@Override public String toString() {
			return "<" + k + "," + v + ">";
		}
	}

	protected static class CompareMyPair implements Comparator<Pair<HashSet<Integer>, Predicate[]>> {
		@Override public int compare(Pair<HashSet<Integer>, Predicate[]> a, Pair<HashSet<Integer>, Predicate[]> b) {
			return a.k.size() - b.k.size();
		}
	}

} // class Select implements Plan
