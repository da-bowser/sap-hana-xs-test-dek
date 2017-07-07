var body = '';

try {
    var conn            = $.hdb.getConnection();
    var tableName       = "dk.it.dqg.dataLoader.job::REP_dek_d.NT_ext_source_csv";
    var sourceSystemId  = 'sfsf_source_sys';
    
    // Get key field from Mapping Table
    var query1   = 'SELECT "source_field" ' 
	            + 'FROM "DQG"."dk.it.dqg.dataTransformer.model::customizing.DATA_MAPPING" '
	            + 'WHERE "is_key" = ? AND "source_system_id" = ? ';
	var resultSet1 = conn.executeQuery(query1, "Y", sourceSystemId);
	var sourceKeyFieldName = resultSet1[0].source_field;
    
    // Get data
    var query2  = 'SELECT * ' 
	            + 'FROM "DQG"."' + tableName + '" ';
	var resultSet2 = conn.executeQuery(query2);

    // Abort if there are no data in source table
    if (resultSet2.length === 0) {
        throw "No data in source table: " + tableName;
    }

    // Get metadata
    var metadata = resultSet2.metadata;

    // Determine index of key field in source table
    var index;
    for (var i = 0; i < metadata.columns.length; i++) {
        if (sourceKeyFieldName === metadata.columns[i].name) {
            index = i;
            break;
        }
    }
    
    // For every row, add value of key field to dynamic where clause
    var whereClause = '';
    var counter = 0;
    var currentColumnValue;
    var valueOfKey;
    var str;
    var iter = resultSet2.getIterator();
    while(iter.next())  {
        currentColumnValue  = iter.value();
        valueOfKey          = currentColumnValue[index];
        
        str = ' "' + sourceKeyFieldName + '" = ' + '\'' + valueOfKey + '\'';
        if (counter ===  resultSet2.length - 1) {
            // Last column
            whereClause += str;
        } else {
            // Not last column
            whereClause += str + ' OR ';
        }
        counter++;
    }
    
    // Build delete query
    var deleteSql   = 'DELETE '
                    + 'FROM "DQG"."' + tableName + '" '
                    + 'WHERE ' + whereClause;
    
	// Execute query
	var rowsAffected = conn.executeUpdate(deleteSql); 
    body = 'Number of rows deleted: ' + rowsAffected;
    //conn.commit();
    conn.close();

} catch (exception) {
    body = 'Error replicating data: \n' + exception;
}

$.response.setBody(body);