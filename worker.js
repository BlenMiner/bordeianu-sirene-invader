import fs from 'fs';
import { csv_to_table } from './csv.js';

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
    const filter = packet.data.FILTER;
    const csvStr = fs.readFileSync(WORKING_FILE);

    console.log(csv_to_table(csvStr));

    setTimeout(MarkAsCompleted, 5000);
});