package query;

import global.Minibase;
import heap.HeapFile;
import parser.AST_Insert;
import relop.Schema;
import relop.Tuple;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

	protected String fileName;
	protected Object[] values;

	/**
	 * Optimizes the plan, given the parsed query.
	 * 
	 * @throws QueryException
	 *             if table doesn't exists or values are invalid
	 */
	public Insert(AST_Insert tree) throws QueryException {

		fileName = tree.getFileName();

		values = tree.getValues();
		 QueryCheck.tableExists(fileName);
	} // public Insert(AST_Insert tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		//TODO 
		Schema s=Minibase.SystemCatalog.getSchema(fileName);

		Tuple tuple=new Tuple(s);
		
		tuple.setAllFields(values);
		HeapFile file=new HeapFile(fileName);
		file.insertRecord(tuple.getData());
		
		
		//TODO if it have index
		// print the output message
		// System.out.println("0 rows affected. (Not implemented)");

	} // public void execute()

} // class Insert implements Plan
