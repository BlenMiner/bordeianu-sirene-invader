var WORKING_FILE = "";

function MarkAsCompleted() {
    process.send({
        type : 'process:msg',
        data : {
            FILE : WORKING_FILE,
            PID: process.env.pm_id
        }
    });
}

function MarkAsReady() {
    process.send({
        type : 'process:msg',
        data : {
            READY: true,
            PID: process.env.pm_id
        }
    });
}

MarkAsReady();

process.on('message', function(packet) {
    WORKING_FILE = packet.data.FILE;

    console.log(WORKING_FILE);
    MarkAsCompleted();
});