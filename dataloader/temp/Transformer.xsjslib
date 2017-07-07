//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  I M P O R T S
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________*/
var DB_UTIL		= $.import("dk.it.dqg.util", "database");
var MDB_UTIL    = $.import("dk.it.dqg.dataTransformer.job.util.masterData", "MasterDataBuilder");
var LOCK_UTIL 	= $.import("dk.it.dqg.util", "lock");


//_______________________________________________________________________________
//---------------------------------------------------------------------------
//               C L A S S   A T T R .   &   C O N S T R U C T O R
//---------------------------------------------------------------------------
//_______________________________________________________________________________
/**
 * Class constructor.
 * @Constructor
 * @param {$.hdb.Connection}     connection     Database connection.
 * @return {Object}                             Class instance
 */
function Transformer() {
	/*---------------------------------------------------
	 *--- Attributes
	 *---------------------------------------------------*/
    this.conn           	= undefined;        // Database connection
    this.basicConf			= undefined;
    this.masterDataBuilder 	= undefined;

    /*---------------------------------------------------
	 *--- "Constructor" logic
	 *---------------------------------------------------*/
    this.conn 		= DB_UTIL.getConnection();
    this.basicConf 	= this.getBasicConf();
	return this;
}



//_______________________________________________________________________________
//---------------------------------------------------------------------------
//             C L A S S:   M E T H O D S
//---------------------------------------------------------------------------
//_______________________________________________________________________________
Transformer.prototype = {

	/**
	 * Returns all tables related to a Replication Task.
	 * @param {String}				repTaskId		Replication Task id
	 * @return {$.hdb.ResultSet}                  	ResultSet from query  
	 */
	getRepTaskTables : function(repTaskId){
		var query	= 'select "native_table", "source_system_id", "adapter_type" '
					+ 'from "DQG".""dk.it.dqg.dataTransformer.model::customizing.CONF_BASIC_BINDINGS" '
					+ 'where "rep_task" = ?';
		var resultSet = this.conn.executeQuery(query, repTaskId);
		return resultSet;
	},
		
		
	/**
	 * Process a single Replication Task.
	 * @param {String}		repTaskId		Replication Task ID
	 */
	processRepTask : function(repTaskId) {
	    $.trace.debug("Now processing " + repTaskId + "...");
		var repTaskTables = this.getRepTaskTables(repTaskId);
		var repTaskTable;
		
		var iterator = repTaskTables.getIterator();
			while(iterator.next()){
			repTaskTable = iterator.value();
			
			//var tableProcessor = new TableProcessor();
			//tableProcessor = processTable(repTaskTable);
		}
	},
		
	/**
	 * Check if Replication Task is locked or not.
	 * @param {object}               Replication Task row
	 */
	checkForRepTaskLock : function(repTaskRow) {
		$.trace.debug("Checking " + repTaskRow.rep_task + " for lock...");
		var repTaskId = repTaskRow.rep_task;
			
		// Try to set lock on current Replication Task.
		var isLocked = LOCK_UTIL.lockHandlingStart(this.conn, repTaskId, "transformer");	
			
		// Check if lock i set or not.
		if (!isLocked){
			this.processRepTask(repTaskId);
			
			// Remove lock on current Replication Task after processing.
			LOCK_UTIL.lockHandlingEnd(this.conn, repTaskId, "transformer");
		} else{
			$.trace.debug("Rep Task " + repTaskId + " is locked, any further processing is aborted!");
		}
	},
		
		
	/**
	 * Process Replication Tasks found in "Basic Configuration" table.
	 * @param {$.hdb.ResultSet}       basicConf       Unique Replication Tasks
	 */
	processRepTasks : function(basicConf) {
		var iterator = basicConf.getIterator();
		var repTaskRow;

		while(iterator.next()){
			repTaskRow = iterator.value();
			this.checkForRepTaskLock(repTaskRow);
		}
	},
		
		
	/**
	 * Query Basic Configuration table and get number of unique Replication Tasks to process.
	 * @return {$.hdb.ResultSet}                     ResultSet from query  
	 */
	getBasicConf : function() {
		$.trace.debug("Getting Replication Tasks for processing...");
		var query = 'select distinct "rep_task" '
				  + '"from "DQG"."dk.it.dqg.dataTransformer.model::customizing.CONF_BASIC_BINDINGS"';
		var resultSet = this.conn.executeQuery(query);
		$.trace.debug("Replication Tasks found: " + resultSet.length);
		return resultSet;
	},


	execute : function() {
		if (this.basicConf.length > 0) {
			// Create master data builder to handle interaction to MASTER_DATA table.
			this.masterDataBuilder = new MDB_UTIL.MasterDataBuilder(this.conn);
			
			// Process Rep. Tasks.
			this.processRepTasks(this.basicConf);
		} else {
			$.trace.debug("No replication tasks to process.");
		}
		
		// Close DB connection when done.
		DB_UTIL.closeConnection(this.conn);
	}

		
};