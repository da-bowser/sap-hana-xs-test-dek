var connection = $.hdb.getConnection();

//Batch Insert
var myArray = [];

// Current datetime
var currentDate = new Date();
$.trace.debug("date: " + currentDate);
var myDate = currentDate.getFullYear() + "-" + (currentDate.getMonth() + 1) + "-" + currentDate.getDate();  

// Max date
var maxDate = "9999-12-31";

// Build array
var row;
row = ["mySourceSys_1", "myObjId_1", "myFieldId_1", myDate, maxDate, "myFieldType_1", "myValue_1", "false"];
myArray.push(row);

row = ["mySourceSys_2", "myObjId_2", "myFieldId_2", myDate, maxDate, "myFieldType_2", "myValue_2", "true"];
myArray.push(row);

row = ["mySourceSys_3", "myObjId_3", "myFieldId_3", myDate, maxDate, "myFieldType_3", "myValue_3", "false"];
myArray.push(row);

// Perform batch insert
connection.executeUpdate('INSERT INTO "S0016140232"."test.dek.dataloader.model::transaction.MASTER_DATA" values(?,?,?,?,?,?,?,?)', myArray);
connection.commit();

connection.close();