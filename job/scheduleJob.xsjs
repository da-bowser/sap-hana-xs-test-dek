$.trace.debug("********************* DUDE! Start now ********************");
var myjob = new $.jobs.Job({uri:"/test/dek/job/myJob.xsjob", sqlcc:"/test/dek/job/anonuser.xssqlcc"});

// Schedule job
var id = myjob.schedules.add({
    description: "Added at runtime - oh dear!",
    xscron: "* * * * * */5 20"
});

// Delete scheduling
//myjob.schedules.delete({id: id});

$.trace.debug("Scheduled job deleted. ID: " + id);

$.response.contentType = "text/plain";
$.response.setBody("Done");

$.trace.debug("********************* DUDE! End now********************");