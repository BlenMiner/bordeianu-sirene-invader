import fs from 'fs';
import { csv_to_table } from './csv.js';
import { MongoClient } from 'mongodb';

var WORKING_FILE = "";
const PASSWORD = "epita";

//const dbUri = `mongodb://127.0.0.1:27017/?retryWrites=true&w=majority`;
const dbUri = `mongodb+srv://epita:${PASSWORD}@cluster0.5owox.mongodb.net/myFirstDatabase?retryWrites=true&w=majority`;
const mongoClient = new MongoClient(dbUri, { useNewUrlParser: true, useUnifiedTopology: true , keepAlive: 1});

mongoClient.connect(err => {
    MarkAsReady();
});

// TODO: OnProcessQuit: mongoClient.close();

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

process.on('message', function(packet) {
    WORKING_FILE = packet.data.FILE;
    const filter = packet.data.FILTER;
    const csvStr = fs.readFileSync(WORKING_FILE, 'utf8');
    const table = csv_to_table(csvStr);

    const collection = mongoClient.db("SIRENE-INVADER").collection("SIRENES");
    const bulkWrite = collection.initializeUnorderedBulkOp();

    for (let i = 0; i < table.length; ++i)
    {
        let insertValue = {};

        for (const [key, value] of Object.entries(filter))
        {
            const v = table[i][value];
            if (v) insertValue[key] = v;
        }

        bulkWrite.insert(insertValue);
    }

    bulkWrite.execute().finally(() => {
        console.log("Processed " + table.length + "rows.");
        MarkAsCompleted();
    });
});