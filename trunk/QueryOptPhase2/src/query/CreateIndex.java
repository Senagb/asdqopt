package query;

import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import global.Minibase;
import global.RID;
import global.SearchKey;
import parser.AST_CreateIndex;
import relop.Schema;
import relop.Tuple;



/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {
	
	/** Name of the table to create. */
	  protected String fileName;

	  /** table to set index on it */
	  protected String TableIndex;
	  
	  /** column to set index on it */
	  protected String ColumnIndex;
	  protected Schema schema;
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
	  
	  // get and validate the requested schema

	  TableIndex = tree.getIxTable();
	 schema= QueryCheck.tableExists(TableIndex);
//	  schema = Minibase.SystemCatalog.getSchema(TableIndex);
	  ColumnIndex = tree.getIxColumn();
	  QueryCheck.columnExists(schema, ColumnIndex);

//	  ixColumn = tree.getIxColumn();
//      QueryCheck.columnExists(schema, ixColumn);
      fileName = tree.getFileName();
      QueryCheck.fileNotExists(fileName);
	  
  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	 
	  //TODO i supposed that the hash file is not exists 
//	  new HashIndex(fileName);
//	  
//	  Minibase.SystemCatalog.createIndex(fileName,TableIndex, ColumnIndex);
//    // print the output message
//    System.out.println("(Not implemented)");
    
    
    
    HeapFile hp = new HeapFile(TableIndex);
    HashIndex hi = new HashIndex(fileName);
    HeapScan scan = hp.openScan(); 
    RID rid = new RID();
    
    while(scan.hasNext()){
            Tuple t = new Tuple(schema, scan.getNext(rid));
            hi.insertEntry(new SearchKey(t.getField(ColumnIndex)), rid);
    }
    
    scan.close();
    Minibase.SystemCatalog.createIndex(fileName, TableIndex, ColumnIndex);
    // print the output message
    System.out.println("Create index for column" + ColumnIndex + " in table " + TableIndex +" for file " + fileName);

  } // public void execute()

} // class CreateIndex implements Plan
