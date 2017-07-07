//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  I M P O R T S
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________*/



//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  M E T H O D S
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________*/
function sleep(milliseconds) {
    var start = new Date().getTime();
    for (var i = 0; i < 1e7; i++) {
        if ((new Date().getTime() - start) > milliseconds) {
            break;
        }
    }
}



//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  M A I N   L O G I C
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________*/
var body = '';

try {
    // Get start time
    var timeStart  = new Date().getTime();
    
    // Wait for some time
    this.sleep(1000);
    
    body += 'User name is: ' + $.session.getUsername() + "\n";
    
    // Check auth
    var isAuth = $.session.hasAppPrivilege('test.dek.xsjs_with_roles::dek_priv_B');
    if (isAuth) {
        body += 'User is authorized'; 
    } else {
        body += 'User is NOT authorized'; 
    }
    
    // Calc time taken
    var timeEnd = new Date().getTime();
    var timeTaken = timeEnd - this.timeStart;
    
    // Write time taken to resp
    //this.logger.addLine(LOG_TYP_INFO, LOG_CAT_GENERAL, "Processing time (ms): " + timeTaken);
    body += '\nProcessing time (ms): ' + timeTaken;
    body += '\nProcessing time (s): ' + timeTaken / 1000;
    body += '\nProcessing time (m): ' + (timeTaken / 1000) / 60;
} catch (exception) {
    body += exception;
} finally {
    $.response.contentType = "text/html";
    $.response.setBody(body);
}
