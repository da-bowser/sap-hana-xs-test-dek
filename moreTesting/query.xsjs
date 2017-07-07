function addPlingsToValue(value) {
        return '\'' + value + '\'';    
}


function buildParamForWhereClause (someArray) {
    var result = '';
    
    for (var i = 0; i < someArray.length; i++) {
        var value = someArray[i];

        if (i + 1 === someArray.length) {
            // Last entry - do not add comma
            result += addPlingsToValue(value);
        } else {
            // Entry followed by comma
            result   += addPlingsToValue(value)   + ",";
        }
    }
    return result;
}
    

 function buildCandidatesWhereClause(candidates) {
    // Initial check to prevent further processing if there are no candidates to process
    if (candidates.length === 0) {
        return undefined;
    }
    
    
    // Split candidates into components, only keeping unique entries
    var objectIdArray = [];
    var fieldIdArray = [];
    var index;
    candidates.forEach(function(candidate) {
        // Add Object Id IFF it is new
        index = objectIdArray.indexOf(candidate.objectId);
        if (index === -1) { objectIdArray.push(candidate.objectId); }
        
        // Add Field Id IFF it is new
        index = fieldIdArray.indexOf(candidate.fieldId);
        if (index === -1) { fieldIdArray.push(candidate.fieldId); }
    });
    
    // Sort param tables (ASC)
    objectIdArray.sort(function(a,b) {
        //sort values ascending
        if (a < b) { return -1; }  
        if (a > b) { return 1;  }   
        return 0;
    });
    fieldIdArray.sort(function(a,b) {
        //sort values ascending
        if (a < b) { return -1; }  
        if (a > b) { return 1;  }   
        return 0;
    });
    
    // Build parameters for WHERE clause
    var sourceSystemParam = addPlingsToValue(candidates[0].sourceSystem);
    var objectIdParam = buildParamForWhereClause(objectIdArray);
    var fieldIdParam = buildParamForWhereClause(fieldIdArray);

    // Add parentheses as required by 'IN' operator
    objectIdParam   = '(' + objectIdParam   + ')';
    fieldIdParam    = '(' + fieldIdParam    + ')';
    
    // Create the full content for SQL WHERE clause
    var whereClause = '    "source_system_id"       = '  + sourceSystemParam    + ' '
                    + 'AND "object_id"              in ' + objectIdParam        + ' '
                    + 'AND "field_id_ref.field_id"  in ' + fieldIdParam;
    
    // Return where clause
    $.trace.debug("Where clause: " + whereClause);
    return whereClause;
}





function getArrayOfQueries(conn, candidates) {
    var newArray = [];
    var record;
    var index;
    
    var query   = 'SELECT   "object_id", '
                + '         "field_id_ref.field_id", '
                + '         TO_VARCHAR(TO_DATE("start_date", \'YYYY-MM-DD\'))   as "start_date", '
    	        + '         TO_VARCHAR(TO_DATE("end_date", \'YYYY-MM-DD\'))     as "end_date", '
    	        + '         "value" ' 
    			+ 'FROM "DQG"."dk.it.dqg.dataTransformer.model::transaction.MASTER_DATA" '
    			+ 'WHERE "source_system_id"       = ? '
                + '  AND "object_id"              = ? '
                + '  AND "field_id_ref.field_id"  = ? '
    			+ 'ORDER BY "object_id", "field_id_ref.field_id", "start_date" ASC';
    			
    var sourceSys = candidates[0].sourceSystem;
    
    candidates.forEach(function(candidate) {
        // Build record to be added, if unique
        record = [query, candidate.objectId, candidate.fieldId];
        
        // Add record IFF new
        index = newArray.indexOf(record);
        if (index === -1) { newArray.push(record); }
    });
    

        $.trace.debug("MasterData query: " + query);
        var resultSet = conn.executeQuery.apply(conn, newArray);
}












var candidateArray = [];
candidateArray.push({sourceSystem: 'sfsf_source_sys', objectId: 'Row 1, Column value 48',   fieldId: 'INT_FIELD_1'});
candidateArray.push({sourceSystem: 'sfsf_source_sys', objectId: 'Row 110, Column value 48', fieldId: 'INT_FIELD_14'});
candidateArray.push({sourceSystem: 'sfsf_source_sys', objectId: 'Row 118, Column value 48', fieldId: 'INT_FIELD_44'});
candidateArray.push({sourceSystem: 'sfsf_source_sys', objectId: 'Row 128, Column value 48', fieldId: 'INT_FIELD_47'});
candidateArray.push({sourceSystem: 'sfsf_source_sys', objectId: 'Row 138, Column value 48', fieldId: 'INT_FIELD_50'});

var connection = $.hdb.getConnection();
var body = '';

try {
    buildCandidatesWhereClause(candidateArray);
    body += 'done';
    body += ' ' + (6000001 % 2000000 === 0);
} catch (exception) {
    body += exception;
} finally {
    connection.close();
    $.response.setBody(body);
}




