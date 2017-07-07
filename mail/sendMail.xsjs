//_______________________________________________________________________________
//---------------------------------------------------------------------------
//                              I M P O R T S
//---------------------------------------------------------------------------
//_______________________________________________________________________________


function sendMail_A() {
    //create email from JS Object and send
    var mail = new $.net.Mail({
       sender: {address: "dek_hcp@sap.com"},
       to: [{ name: "D-BOI", address: "dek@itelligence.dk", nameEncoding: "UTF-8"}],
       subject: "mail from hcp",
       subjectEncoding: "UTF-8",
       parts: [ new $.net.Mail.Part({
           type: $.net.Mail.Part.TYPE_TEXT,
           text: "The body of the mail.",
           contentType: "text/plain",
           encoding: "UTF-8"
       })]
    });

    var returnValue;
    var response = "";
    try {
        returnValue = mail.send();
        response = "MessageId = " + returnValue.messageId + ", final reply = " + returnValue.finalReply;
        $.response.status = $.net.http.OK;
    } catch(exception) {
        response = "Error occurred:" + exception.message;
    }

    return response;
}


function sendMail_B() {
    var subscribers = ["dek@itelligence.dk"];
    var smtpConnection = new $.net.SMTPConnection();
    var mail = new $.net.Mail({ sender: "dek_hcp@sap.com",
        subject: "mail test",
        subjectEncoding: "UTF-8",
        parts: [new $.net.Mail.Part({
                    type: $.net.Mail.Part.TYPE_TEXT,
                    contentType: "text/html",
                    encoding: "UTF-8"
               })]
        });
    
    var returnValue;
    var response = "";
    try {
        for (var i = 0; i < subscribers.length; ++i) {
            mail.to = subscribers[i];
            mail.parts[0].text = "Awesome text";
            returnValue = smtpConnection.send(mail);
            response = response + "\nMessageId = " + returnValue.messageId + ", final reply = " + returnValue.finalReply;
        }
        $.response.status = $.net.http.OK;
    } catch(exception) {
        response = "Error occurred:" + exception.message;
    }
    smtpConnection.close();
    return response;
}



/*
 * MAIN
 */ 
var body = ''; 

try {
    body += sendMail_A() + "\n";
    body += sendMail_B();
} catch (exception) {
    body += exception;
} finally {
    $.response.contentType = "text/html";
    $.response.setBody(body);
}