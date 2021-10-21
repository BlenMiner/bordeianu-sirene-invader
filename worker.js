setInterval(() => {
    console.log("alive");
}, 10000);

var WORKING_FILE = "";
var MY_PID = -1;

function MarkAsCompleted() {
    process.send({
        type : 'process:msg',
        data : {
            FILE : WORKING_FILE,
            PID: MY_PID
        }
    });
}

process.on('message', function(packet) {
    WORKING_FILE = packet.data.FILE;
    MY_PID = packet.data.PID;

    MarkAsCompleted();
});