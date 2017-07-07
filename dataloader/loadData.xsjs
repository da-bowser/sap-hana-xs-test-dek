/*_______________________________________________________________________________
 *  ---------------------------------------------------------------------------
 *                  G L O B A L   V A R I A B L E S
 *  ---------------------------------------------------------------------------
 *_______________________________________________________________________________*/
var CONN;




/*_______________________________________________________________________________
 *  ---------------------------------------------------------------------------
 *                  U T I L I T Y   F U N C T I O N S
 *  ---------------------------------------------------------------------------
 *_______________________________________________________________________________*/

/**
 * Connect to database
 */
function connectToDatabase() {
    var connection;

    try {
    	// Get connection to database
    	connection = $.hdb.getConnection();
	
    	$.trace.debug("Connected to database.");
    } catch (exception) {
    	$.trace.fatal("Error connecting to database.\n" + exception);
    }
    
    return connection;
}


/**
 * Disconnect from database
 */
function disconnectFromDatabase(connection) {
	try {
	    connection.commit();
	    connection.close();
	    $.trace.debug("Disconnected from database.");
    } catch (exception) {
		// Too bad
    	$.trace.warning("Error committing and closing connection to database.\n" + exception);
	}
}



/**
 * Looking which target field to map source field to.
 */
function lookupMapping(sourceFieldName) {
	var query 		= 'select * from "S0016140232"."DQG_DATA_MAPPING" where source_system_id';
	var resultSet 	= CONN.executeQuery(query);
	
	
}






/**
 * Process single, source database column of a specific row
 */
function processColumn(columnName, columnValue) {
	// Perform lookup in Mapping table, in order to
	// determine which target field to map to.
	// TO DO
	
	// Perform lookup in Master data table, in order to 
	// determine if record is an INSERT or UPDATE
	// TO DO
	
	// Update/insert current record into Master data table
	// TO DO
}




/**
 * Process single, source database row
 */
function processRow(row, metadata) {
	var counter 	= 0;
	var columnValue = "";
	var columnName 	= "";

	$.trace.debug("======= Row =======");
	for (counter=0; counter < metadata.columns.length; counter++) {
		columnValue = row[counter];
		columnName 	= metadata.columns[counter].name;
		$.trace.debug("--> " + counter +  ", Column name: " + columnName + ", Value: " + columnValue);
	}
}


/**
 * Process contents of source table
 */
function processTableContent(resultSet) {
	var metadata = resultSet.metadata;
	$.trace.debug("Number of rows: " + resultSet.length);
	$.trace.debug("Number of columns in metadata: " + metadata.columns.length);

	// Iterate all rows of current result set
	var row;
	var iter = resultSet.getIterator();
	while(iter.next()) {
		row = iter.value();
		processRow(row, metadata);
	}
	$.trace.debug("DONE!!");
}



/**
 * Get source data to be processed by data loader.
 */
function getSourceData(tableName) {
	var query 		= 'select * from "S0016140232"."' + tableName + '"';
	var resultSet 	= CONN.executeQuery(query);
	return resultSet;
}


/**
 * Process single, configured source table
 */
function processConfiguredTable(row) {
	// Get value of column TABLE_NAME
	var tableName = row['table_name'];
	
	// Get source data
	var sourceData = getSourceData(tableName);
	
	// Exit method if no data is found
	if (sourceData.length === 0 ) { 
		$.trace.debug("*processConfiguredTable* No data found in source table " + tableName);
		return; 
	}
	
	// DUBLICATE CHECK: Get mandatory (for file adapter!!!) source file info
	var sourceFileName = sourceData[0]['NAME'];
	
	// DUBLICATE CHECK: Check for duplicates (determine if file has already been processed)
	//var custLastRead = row['last_read'];
	var custLastReadFileName = row['last_read_file_name'];
	
	// DUBLICATE CHECK: execute
	if ( custLastReadFileName === sourceFileName ) {
		$.trace.warning("*processConfiguredTable* File " + sourceFileName + " already processed for table " + tableName);
	}
	
	// Execute Data Loader processing logic
	processTableContent(sourceData);
}



/**
 * Process all configured source tables
 */
function processConfiguredTables() {
	// Get list of tables to be processed
	var query 		= 'select "source_system_id", "table_name", "last_read_file_name" from "S0016140232"."test.dek.dataloader.model::customizing.DATA_LOAD_ENTITY_CONFIG"';
	var resultSet 	= CONN.executeQuery(query);
	
	// Log number of tables configured to be processed
	$.trace.debug("*processConfiguredTables* Number of tables configured to be processed by data loader: " + resultSet.length);
	
	// Process each entry (1 row represents 1 source table with master data to be loaded)
	var row;
	var iter = resultSet.getIterator();
	while(iter.next()) {
		row = iter.value();
		processConfiguredTable(row);
	}
}




/*_______________________________________________________________________________
 *  ---------------------------------------------------------------------------
 *              S T A R T   P R O C E S S I N G   R E Q U E S T
 *  ---------------------------------------------------------------------------
 *_______________________________________________________________________________*/

// Connect to database
CONN = connectToDatabase();

// Process all tables
processConfiguredTables();

// Commit and disconnect from database
disconnectFromDatabase(CONN);