package query;

import global.Minibase;
import parser.AST_CreateIndex;
import relop.Schema;



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
  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
	  
	  fileName=tree.getFileName();
	  TableIndex=tree.getIxTable();
	  ColumnIndex=tree.getIxColumn();
	  
  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
	 
	  //TODO i supposed that the file is already exicts so no need to make hash file
	  Minibase.SystemCatalog.createIndex(fileName,TableIndex, ColumnIndex);
    // print the output message
    System.out.println("(Not implemented)");

  } // public void execute()

} // class CreateIndex implements Plan
