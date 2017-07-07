//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  I M P O R T S
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________*/
var TRFM_UTIL = $.import("dk.it.dqg.test.dek.dataloader.temp", "Transformer");



//_______________________________________________________________________________
//  ---------------------------------------------------------------------------
//                  T E S T   L O G I C
//  ---------------------------------------------------------------------------
//_______________________________________________________________________________*/
$.trace.debug("********************* TEST: Transformer.xsjslib, start ********************");
var body = '';

try {
    // Define input parameter: Replication Task Id
    var input = { repTaskId : 'REP_dek_d' };

    // Execute data loader
    var transformer = new TRFM_UTIL.Transformer();
    

    // Set positive response body
    body = 'Data replicated from virtual tables to native hana tables';
} catch (exception) {
    // Set negative response body
    body = 'Error replicating data: \n' + exception;
}

$.response.setBody(body);
$.trace.debug("********************* TEST: Transformer.xsjslib, end ********************");