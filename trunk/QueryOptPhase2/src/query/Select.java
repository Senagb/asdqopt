package query;

import heap.HeapFile;

import java.util.Arrays;

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
		String[] tables = tree.getTables();
		String[] columns = tree.getColumns();
		Predicate[][] predicates = tree.getPredicates();
		Iterator iter = null;
		Integer[] colids = null;
for(int i=0;i<predicates.length;i++)
	System.out.println(Arrays.toString(predicates[i]));
		if (tables == null)
			throw new QueryException("Tables array invalid in SELECT");

		if (tables.length == 0)
			throw new QueryException("At least one table is required in SELECT");

		if (columns == null)
			throw new QueryException("Columns array invalid in SELECT");

		// build the final schema
		Schema[] tableSchemas = new Schema[tables.length];
		Schema schema = null;
		for (int tindex = 0; tindex < tables.length; tindex++) {
			if (schema == null) {
				schema = QueryCheck.tableExists(tables[tindex]);
				tableSchemas[tindex] = schema;
			} else {
				tableSchemas[tindex] = QueryCheck.tableExists(tables[tindex]);
				Schema tmpschema = Schema.join(schema, tableSchemas[tindex]);
				schema = tmpschema;
			}
		}

		// Check validty of columns
		if (columns.length > 0) {
			colids = new Integer[columns.length];

			for (int cindex = 0; cindex < columns.length; cindex++) {
				colids[cindex] = QueryCheck.columnExists(schema,
						columns[cindex]);
			}
		}

		// Check validty of predicates
		QueryCheck.predicates(schema, predicates);

		// Do all the hard work
		// FileScan and SimpleJoin
		for (int tindex = 0; tindex < tables.length; tindex++) {
			if (iter == null) {
				HeapFile hf = new HeapFile(tables[tindex]);
				iter = new FileScan(tableSchemas[tindex], hf);
			} else {
				HeapFile hf = new HeapFile(tables[tindex]);
				Iterator tmpfs = new FileScan(tableSchemas[tindex], hf);
				Iterator tmp = new SimpleJoin(iter, tmpfs);
				iter = tmp;
			}
		}
//iter.execute();
		// Selection
		int count=0;
		for (int pindex = 0; pindex < predicates.length; pindex++) {
			System.out.println(Arrays.toString(predicates[pindex]));
			Iterator tmpsel = new Selection(iter, predicates[pindex]);
			count++;
//			if(count==4)
//				tmpsel.execute();
			iter = tmpsel;
		}
		System.out.println();
//		if(iter!=null)
		//	iter.execute();
		System.out.println(".............................");
		// Projection
		if (columns.length > 0) {
			Iterator tmpproj = new Projection(iter, colids);
			iter = tmpproj;
		}

		selectIter = iter;
		isExplain = tree.isExplain;
	} // public Select(AST_Select tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		int retval = 0;
        
        if(selectIter != null) {
                if(isExplain) {
                        selectIter.explain(0);
                        System.out.println("SELECT Query Explained.");
                }
                else {
                       retval = selectIter.execute();
                        System.out.println(retval + "row(s) selected.");
                }
        }
		// print the output message
		System.out.println("0 rows affected. (Not implemented)");

	} // public void execute()

} // class Select implements Plan
