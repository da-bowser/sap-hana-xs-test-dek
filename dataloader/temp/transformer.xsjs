//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  P R O G R A M   D E S C R I P T I O N
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________
//BLA BLA


//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  I M P O R T S
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________

var DB_UTIL = $.import("dk.it.dqg.util", "database");
var LOCK_UTIL = $.import("dk.it.dqg.util", "lock");
var CONVERSION_UTIL = $.import("dk.it.dqg.dataTransformer.job.util.conversionMethods", "converter");
var MASTER_DATA_UTIL =  $.import("dk.it.dqg.dataTransformer.job.util.masterData", "MasterDataBuilder");
var MASTER_DATA_DO_UTIL = $.import("dk.it.dqg.dataTransformer.job.util.masterData", "MasterDataDataObject");

//_______________________________________________________________________________
// ---------------------------------------------------------------------------
//                  G L O B A L   V A R I A B L E S
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________

var MASTER_DATA_BUILDER;
var CONN;
var SOURCE_SYSTEM_ID = "";
var SOURCE_OBJECT_ID_VALUE = "";
var ADAPTER_TYPE = "";

const CONF_BASIC_BINDINGS = "dk.it.dqg.dataTransformer.model::customizing.CONF_BASIC_BINDINGS";

//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  U T I L I T Y   F U N C T I O N S
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________

/**
 * Checks if mapping info rule is a match.
 * @param {object}				mappingInfoRow		Mapping rule (row)
 * @param {String}				sourceSystemId		Source system id to match
 * @param {String}				columnName			Column name to match
 * @return {boolean}								Result of match  
 */
function checkForMappingMatch(mappingInfoRow, sourceSystemId, columnName) {
	var mappingFound = false;
	
	if ((mappingInfoRow['source_system_id'] === sourceSystemId) && (mappingInfoRow['source_field'] === columnName)) {
		mappingFound = true;
	}
	
	return mappingFound;
}

/**
 * Gets mapping rule from list based on a column.
 * @param 	{$.hdb.ResultSet}		mappingInfo		Mapping rules related to a source system
 * @param 	{String}              	sourceSystemId	Source system id
 * @param 	{String}				columnName		Name of column to match
 * @return 	{object}								Matching rule
 */
function getMatchingMappingInfoRow(mappingInfo, sourceSystemId, columnName) {
	var mappingInfoRow = "";
	var counter = 0;
	var matchRow = "";
	var mappingFound = false;
	
	for (var counter = 0; counter < mappingInfo.length; counter++) {
		mappingInfoRow = mappingInfo[counter];
		
		mappingFound = checkForMappingMatch(mappingInfoRow, sourceSystemId, columnName);
		
		if (mappingFound) {
			matchRow = mappingInfoRow;
			break; // Leave loop when we have the row we want.
		}
	}
	
	if (matchRow === "") {
	    var noMappingMatchFound = "**** FATAL **** - no mapping information found for system: " + SOURCE_SYSTEM_ID + ", with column name: " + columnName + ", row will *not* be processed.";
		throw noMappingMatchFound;
	}
	
	return matchRow;
}

/**
 * Creates a master data record and adds it to "process queue".
 * @param {object}				masterDataObject		Object containing data relevant for master data row.
 */
function createMasterDataRecord(masterDataObject) {

	if (masterDataObject.isKeyColumn === "N") {
	    $.trace.debug("Creating master data record...");
		var dataObject = new MASTER_DATA_DO_UTIL.MasterDataDataObject();
		
		dataObject.setSourceSystem(masterDataObject.sourceSystemId);
		dataObject.setObjectId(masterDataObject.sourceObjectIdValue);
		dataObject.setFieldId(masterDataObject.fieldId);
		dataObject.setStartDate(masterDataObject.startDate); 	
		dataObject.setEndDate(masterDataObject.endDate);
		dataObject.setValue(masterDataObject.value);
		
		MASTER_DATA_BUILDER.addToQueue(dataObject);
	} else {
	    $.trace.debug("Key column is skipped, no master data record created...");
	}
}

function handleValueConversion(mappingInfoRow, columnName, columnValue) {
    var conversionMethod = mappingInfoRow['conversion_method.name'];
    var value = "";
    
    $.trace.debug("**********CONVERSION...." + conversionMethod);
	if(conversionMethod === null || conversionMethod === undefined) {
	    // Set value of current column.
	    $.trace.debug("No conversion needed for: " + columnName);
	    value = columnValue;
	} else {
	    // Conversion of value needed.
	    $.trace.debug("Conversion " + conversionMethod + " needed for: " + columnName);
	    value = CONVERSION_UTIL.handleConversionMethod(CONN, conversionMethod, SOURCE_SYSTEM_ID, columnName, columnValue);
	}

    return value;
}

/**
 * Process and prepare input data for master data.
 * @param {object}				mappingInfoRow		Mapping rule match
 * @param {String}				columnName			Name of column
 * @param {String}              columnValue			Value of column
 */
function processMasterDatarecord(mappingInfoRow, columnName, columnValue) {
	var masterData = {};
	
	// Check if current column is a "key" column.
	masterData.isKeyColumn = mappingInfoRow['is_key'];
	
	// Set source system id.
	masterData.sourceSystemId = mappingInfoRow['source_system_id'];
	
	// Set object id.
	masterData.sourceObjectIdValue = SOURCE_OBJECT_ID_VALUE;
	
	// Set field id.
	masterData.fieldId = mappingInfoRow['target_field_ref.field_id'];
	
	// Set start date.
	masterData.startDate = "2016-12-28"; //TODO! find a way to get this data. 
	
	// Set end date.
	masterData.endDate = "9999-12-31";	//TODO! find a way to get this data.
	
	// Set value of current column.
	masterData.value = handleValueConversion(mappingInfoRow, columnName, columnValue);
	
	// Create master data record.
	createMasterDataRecord(masterData);
}

/**
 * Process mapping rules for a column.
 * @param {$.hdb.ResultSet}		mappingInfo		Mapping rules related to a source system
 * @param {String}              columnName		Column name
 * @param {String}              columnValue		Column value
 */
function processMappingInfo(mappingInfo, columnName, columnValue){
	var mappingInfoRow = "";
	
	try {	
	    mappingInfoRow = getMatchingMappingInfoRow(mappingInfo, SOURCE_SYSTEM_ID, columnName);
	    
	    processMasterDatarecord(mappingInfoRow, columnName, columnValue);
	     
	} catch (noMappingMatchFound) {
	    $.trace.fatal(noMappingMatchFound);
	    // TODO! Add log to inform end user of error.
	}
	
	
}

/**
 * Process a single column from a "native" table row.
 * @param {String}				columnName		Name of column
 * @param {String}              columnValue		Value of column
 * @param {$.hdb.ResultSet}		mappingInfo		Mapping rules related to a source system  
 */
function processColumn(columnName, columnValue, mappingInfo) {
    $.trace.debug("Processing column: " + columnName + "...");
	var dataObject = {};
	
	// Process mapping info.
	dataObject = processMappingInfo(mappingInfo, columnName, columnValue);
}

/**
 * Check if mapping rule contains "key" column name.
 * @param {object}				mappingInfoRow		Single mapping rule
 * @returns {boolean}     		            		Result of check
 */
function checkForKeyColumn(mappingInfoRow) {
    var isKey = false;
        
        if(mappingInfoRow['is_key'] === "Y") {
            isKey = true;    
        }
        
    return isKey;
}

/**
 * Get name of "key" column from mapping rules.
 * @param {$.hdb.ResultSet}             mappingInfoRow		Mapping rules related to a source system
 * @returns {String}     		                    		Name of "key" column
 */
function getKeyColumnName(mappingInfo) {
    var keyColumnName = "";
    var iterator;
    var mappingInfoRow = "";
    var isKey = false;
    
    iterator = mappingInfo.getIterator();
    
    while(iterator.next()) {
        mappingInfoRow = iterator.value();
        isKey = checkForKeyColumn(mappingInfoRow);
        
        if(isKey) {
            keyColumnName = mappingInfoRow['source_field'];
        }
    }
    
    return keyColumnName;
}

/**
 * Process single "native" table row, taking "skip" columns into account.
 * @param {object}				nativeTableRow		Row of data from "native" table
 * @param {array}               dontSkipColumns		Positive list of columns we wan't to read from row
 * @param {$.hdb.ResultSet}		mappingInfo			Mapping rules related to a source system
 */
function processNativeTableRow(nativeTableRow, dontSkipColumns, mappingInfo){
	var counter = 0;
	var columnValue = "";
	var columnName = "";
	var rowError = "";

    // Check for import errors if adapter type is file.
	if(ADAPTER_TYPE === "FILE") {
	    $.trace.debug("Checking for import errors on row for adatper type: " + ADAPTER_TYPE + "...");
	    // Get error value for current row.
	    rowError = nativeTableRow['ERROR'];
	}
	
	// Only handle row if no import errors are found.
	if (rowError === null) {
	    $.trace.debug("Processing row...");
		for (counter = 0; counter < dontSkipColumns.length; counter++) {
			columnName = dontSkipColumns[counter];
			columnValue = nativeTableRow[dontSkipColumns[counter]];
			
			// Get "key" column name for source system.
			var keyColumnName = getKeyColumnName(mappingInfo);
			
			// Get key value for current row.
			SOURCE_OBJECT_ID_VALUE = nativeTableRow[keyColumnName];

			// Process current column.
			processColumn(columnName, columnValue, mappingInfo);
		}
	} else {
		$.trace.error("Row has Import errors: " + rowError + " - row *not* processed.");
		// TODO! Create log entry informing end user of error in input data.
	}
	
}

/**
 * Process table data from a "native" table.
 * @param {$.hdb.ResultSet}		nativeTableRows		"Native" table data
 * @param {array}               dontSkipColumns		Positive list of columns we wan't from native table
 * @param {$.hdb.ResultSet}     mappingInfo        	Mapping rules related to a source system
 */
function processNativeTableRows(nativeTableRows, dontSkipColumns, mappingInfo){
	var row;
	var iterator = nativeTableRows.getIterator();
	while(iterator.next()){
		row = iterator.value();
		processNativeTableRow(row, dontSkipColumns, mappingInfo);
	}
}

/**
 * Query "SKIP COLUMN" table to get a list of columns to skip for a given source system.
 * This is mostly relevant when using "File Adapter" to load data.
 * @param {String}				sourceSystemId		Source system id
 * @return {$.hdb.ResultSet}                     	Resultset from query  
 */
function getSkipColumns(sourceSystemId){
    $.trace.debug("Getting names of columns to skip for system " + sourceSystemId + "...");
	var resultSet = CONN.executeQuery('select "source_field_name" from "DQG"."dk.it.dqg.dataTransformer.model::customizing.DATA_LOAD_COLUMN_SKIP" where "source_system_id" = \'' + sourceSystemId + '\'');
	return resultSet;
}

/**
 * Query dynamic Hana "native" table related to Replication Task.
 * @param {String}				nativeTableName		Name of query table
 * @return {$.hdb.ResultSet}                     	Resultset from query  
 */
function getNativeTableRows(nativeTableName){
	try {
	    $.trace.debug("Get data from table " + nativeTableName + "...");
		var resultSet = CONN.executeQuery('select * from "DQG"."' + nativeTableName + '"');
	} catch (sqlException) {
		throw sqlException;
	}
	
	return resultSet;
}

/**
 * Checks if a column name is found in "skip" array.
 * @param {array}				skipColumns			Array of column names we want to skip while reading native tables
 * @param {String}              checkValue    	    Column name we want to check against skip array
 * @return {boolean}            					Result of check
 */
function checkForSkipValue(skipColumns, checkValue){
	var valueFound = false;
	var iterator;
	var row;
	
	iterator = skipColumns.getIterator();
	while (iterator.next()) {
		row = iterator.value();
		
		if (checkValue === row[0]) {
			valueFound = true;
			break;
		} else {
			valueFound = false;
		}
	}
	
	return valueFound;
}

/**
 * Creates an array of column names we want to read from "native" table.
 * This is mostly relevant for File Adapter loads where we have a lot of "adapter specific" fields.
 * @param {$.hdb.ResultSet}			skipColumns				Names of columns we don't want to read
 * @param {$.hdb.ColumnMetadata}	nativeTableMetadata     Metadata of dynamically read "native" table
 * @return {array}                     						Positive list of column names
 */
function processSkipColumns(skipColumns, nativeTableMetadata){
	var counter = 0;
	var positiveList = [];
	var valueFound;
	var columnName;
	
	for (counter = 0; counter < nativeTableMetadata.columns.length; counter++) {
		
		columnName = nativeTableMetadata.columns[counter].name;
		valueFound = checkForSkipValue(skipColumns, columnName);
		
		if (!valueFound) {
			positiveList.push(columnName);
		}
		
	}
	
	return positiveList; 
}

/**
 * Deletes all data for "Native Table" where transformation is processed correctly.
 * @param {String}				nativeTableName		Name of Hana table
 * @return {number}             					Number of rows deleted
 * @throws {$.hdb.SQLException}  
 */
function deleteRepTaskTableRows(nativeTableName) {
	var rowsDeleted = 0;
	
	$.trace.debug("Starting cleaup of " + nativeTableName + "...");
	try {
		
		rowsDeleted = CONN.executeUpdate('delete from "DQG"."' + nativeTableName + '"');
		
		if (rowsDeleted > 0) {
			CONN.commit();
			$.trace.debug("Cleanup of " + nativeTableName + " complete " + rowsDeleted + " rows deleted.");	
		}
		
	} catch (sqlException) {
		throw sqlException;
	}
	
	return rowsDeleted;
}

/**
 * Query Mapping Data table to get all mapping rules matching source system.
 * @param {String}				sourceSystemId		Source system id
 * @return {$.hdb.ResultSet}                     	Resultset from query  
 */
function getMappingInfo(sourceSystemId){
    $.trace.debug("Getting mapping rules for system " + sourceSystemId + "...");
	var resultSet = CONN.executeQuery('SELECT * ' +
			'FROM "DQG"."dk.it.dqg.dataTransformer.model::customizing.DATA_MAPPING"' +
			'WHERE "source_system_id" = \'' + sourceSystemId + '\'');
	
	if (resultSet.length === 0) {
	    var noMappingFound = "**** FATAL **** customizing fault, no mapping values found for source_system_id: " + sourceSystemId;
		throw noMappingFound;
	}
	
	return resultSet;
}

/**
 * Process "Native Table" related to a Replication Task.
 * @param {object}				repTaskRow		 Replication Task row
 * @return {$.hdb.ResultSet}                     Resultset from query  
 */
function processRepTaskTable(repTaskRow){
	var nativeTableName = repTaskRow['native_table'];
	var nativeTableRows = "";
	try {
		nativeTableRows = getNativeTableRows(nativeTableName);
		
		if (nativeTableRows.length > 0) {
			var nativeTableMetadata = nativeTableRows.metadata;
			var mappingInfo;
			
			// Set source system id as global variable
			SOURCE_SYSTEM_ID = repTaskRow['source_system_id'];
	        
	        // Set adapter type as global varable.	
	        ADAPTER_TYPE = repTaskRow['adapter_type'];
	        
			// Get mapping info related to source system.
			try {
			    mappingInfo = getMappingInfo(SOURCE_SYSTEM_ID);
			} catch (noMappingFound) {
			    	$.trace.fatal(noMappingFound);
		            // TODO! Add log entry, informing end user of customizing error.
			}
						
			// Get what columns to skip for current table when reading data.
			var skipColumns = getSkipColumns(SOURCE_SYSTEM_ID);
			
			// Get "positive" list of columns we want to get.
			var dontSkipColumns = processSkipColumns(skipColumns, nativeTableMetadata);

			processNativeTableRows(nativeTableRows, dontSkipColumns, mappingInfo);
			
			// Push master data records to table.
			MASTER_DATA_BUILDER.writeQueuesToDatabase();
			
			// Do cleanup after processing of rows.
			try {
				//deleteRepTaskTableRows(nativeTableName);
			} catch (sqlException) {
			    //TODO! Log error in table.
				$.trace.fatal("**** FATAL **** - cleanup of table: " + nativeTableName + " failed with error: " + sqlException);
			}

		} else {
			$.trace.error("Error: No data to process for table: " + nativeTableName);
		}
		
	} catch (sqlException) {
		$.trace.error("Error during processing of rows for table " + nativeTableName + " with error: " + sqlException);
	}
	
}

/**
  * Returns all tables related to a Replication Task.
  * @param {String}				repTaskId		Replication Task id
  * @return {$.hdb.ResultSet}                   Resultset from query  
  */
function getRepTaskTables(repTaskId){
	var resultSet = CONN.executeQuery('select "native_table", "source_system_id", "adapter_type" from "DQG"."' + CONF_BASIC_BINDINGS + '" where "rep_task"=\'' + repTaskId + '\'');
	return resultSet;
}

/**
  * Process a single Replication Task.
  * @param {String}		repTaskId		Replication Task ID
  */
function processRepTask(repTaskId){
    $.trace.debug("Now processing " + repTaskId + "...");
	var repTaskTables = getRepTaskTables(repTaskId);

	var repTaskTable;
	var iterator = repTaskTables.getIterator();

	while(iterator.next()){
		repTaskTable = iterator.value();
		processRepTaskTable(repTaskTable);
	}
}

/**
  * Check if Replication Task is locked or not.
  * @param {object}               Replication Task row
  */
function checkForRepTaskLock(repTaskRow){
    $.trace.debug("Checking " + repTaskRow['rep_task'] + " for lock...");
	var repTaskId = repTaskRow['rep_task'];
	
	// Try to set lock on current Replication Task.
	var isLocked = LOCK_UTIL.lockHandlingStart(CONN, repTaskId, "transformer");	
	
	// Check if lock i set or not.
	if (!isLocked){
		processRepTask(repTaskId);
		
		// Remove lock on current Replication Task after processing.
		LOCK_UTIL.lockHandlingEnd(CONN, repTaskId, "transformer");
	} else{
		$.trace.debug("Rep Task " + repTaskId + " is locked, any further processing is aborted!");
	}
}

 /**
   * Process Replication Tasks found in "Basic Configuration" table.
   * @param {$.hdb.ResultSet}       basicConf       Unique Replication Tasks
   */
function processRepTasks(basicConf){
	var iterator = basicConf.getIterator();
	var repTaskRow;

	while(iterator.next()){
		repTaskRow = iterator.value();
		checkForRepTaskLock(repTaskRow);
	}
}

 /**
   * Query Basic Configuration table and get number unique Replikation Tasks to process.
   * @return {$.hdb.ResultSet}                     Resultset from query  
   */
function getBasicConf(){
    $.trace.debug("Getting Replication Tasks for processing...");
	var resultSet = CONN.executeQuery('select distinct "rep_task" from "DQG"."' + CONF_BASIC_BINDINGS + '"');
	$.trace.debug("Replication Tasks found: " + resultSet.length);
	return resultSet;
}

//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  M A I N   L O G I C
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________

 /**
   * Main function called from scheduled job, starts data transformation.
   */
function main() {
	
	// Get DB connection.
	CONN = DB_UTIL.getConnection();
	
	//Get basic configuration
	var basicConf = getBasicConf();

	if (basicConf.length > 0) {
		// Create master data builder to handle interaction to MASTER_DATA table.
		MASTER_DATA_BUILDER = new MASTER_DATA_UTIL.MasterDataBuilder(CONN);
		
		// Process Rep. Tasks.
		processRepTasks(basicConf);
	} else {
		$.trace.debug("No replication tasks to process.")
	}
	
	// Close DB connection when done.
	DB_UTIL.closeConnection(CONN);
}

$.trace.debug("======================= Transformer - Start =======================");
main();
$.trace.debug("======================= Transformer - End =======================");
