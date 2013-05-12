package query;

import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
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
	protected Schema schema;
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
		 
		 
		 
		 
         schema = QueryCheck.tableExists(fileName);
         
         QueryCheck.insertValues(schema, values);
         
         // We need to update index it they exists, so check their validity here
         IndexDesc[] ixDescs = Minibase.SystemCatalog.getIndexes(fileName);
         for(IndexDesc ixDesc : ixDescs) {
                 QueryCheck.indexExists(ixDesc.indexName);
         }
	} // public Insert(AST_Insert tree) throws QueryException

	/**
	 * Executes the plan and prints applicable output.
	 */
	public void execute() {
		//TODO 
//		Schema s=Minibase.SystemCatalog.getSchema(fileName);
//		Tuple tuple=new Tuple(s);
//		
//		tuple.setAllFields(values);
//		HeapFile file=new HeapFile(fileName);
//		file.insertRecord(tuple.getData());

		HeapFile hf = new HeapFile(fileName);
        Tuple tuple = new Tuple(schema, values);
        RID rid = tuple.insertIntoFile(hf);
        
        // If there is index for the table, also update the index
        IndexDesc[] ixDescs = Minibase.SystemCatalog.getIndexes(fileName);
        for(IndexDesc ixDesc : ixDescs) {
                HashIndex indexFile = new HashIndex(ixDesc.indexName);
                indexFile.insertEntry(new SearchKey(tuple.getField(ixDesc.columnName)), rid);
        }
		
		//TODO if it have index
		// print the output message
		// System.out.println("0 rows affected. (Not implemented)");

	} // public void execute()

} // class Insert implements Plan
