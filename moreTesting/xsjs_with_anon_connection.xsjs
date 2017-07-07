//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  I M P O R T S
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________*/
var DATE_UTIL   = $.import("dk.it.dqg.util", "date");
var RESET_UTIL  = $.import("dk.it.dqg.tools.resetResults", "Resetter");
var DB_UTIL     = $.import("dk.it.dqg.util", "database");
var LOCK_UTIL   = $.import("dk.it.dqg.util", "lock");
var LOG_UTIL    = $.import("dk.it.dqg.util", "Logger");


var body = '';
var conn;

try {
    conn = $.db.getConnection("test.dek.moreTesting::default");
    body += "hello";
} catch (exception) {
    $.trace.fatal("Error during execution: \n" + exception.toString());
    body += exception;
} finally {
    conn.close();
    $.response.contentType = "text/html";
    $.response.setBody(body);
}