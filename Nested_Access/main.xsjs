//_______________________________________________________________________________
//---------------------------------------------------------------------------
//                              I M P O R T S
//---------------------------------------------------------------------------
//_______________________________________________________________________________
var AAA_UTIL = $.import("test.dek.Nested_Access", "a");
var BBB_UTIL = $.import("test.dek.Nested_Access", "b");




/*
 * MAIN
 */ 
var body = ''; 

try {
    
    body += 'creating A\n';
    var A = new AAA_UTIL.AAA();
    body += 'A created\n';
    body += 'Value of prop A: ' + A.propA + "\n";
    body += 'Value of prop B: ' + A.refToB.propB + "\n";
    
    body += 'Value of prop A - prototype: ' + AAA_UTIL.AAA.prototype.propA + "\n";
    body += 'Value of prop B - prototype: ' + BBB_UTIL.BBB.prototype.propB + "\n";
    
} catch (exception) {
    body += exception;
} finally {
    $.response.setBody(body);
}