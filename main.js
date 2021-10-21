import fs from 'fs';
import pm2 from 'pm2';
import { performance } from 'perf_hooks';
import { csv_to_table } from './csv.js';

const PATH = './data/StockEtablissement_utf8.csv';

const WORK_IN_PROGRESS = {};
const QUEUED_WORK = [];
const FREE_INSTANCES = [];

function UpdateWorkers(packet = null) {
    if (packet !== null) {
        // worker finished something
        FREE_INSTANCES.push(packet.data.PID);
        const delay = performance.now() - WORK_IN_PROGRESS[packet.data.FILE];
        console.log("Took " + delay + " ms for " + packet.data.FILE);
    }

    if (QUEUED_WORK.length > 0 && FREE_INSTANCES.length > 0) {
        let instanceReadyCount = Math.min(QUEUED_WORK.length, FREE_INSTANCES.length);

        for (let i = 0; i < instanceReadyCount; ++i) {
            const PID = FREE_INSTANCES[i];

            // Remove it from the FREE list
            FREE_INSTANCES.splice(i, 1);
            
            i -= 1;
            instanceReadyCount -= 1;

            const work = QUEUED_WORK.shift();

            WORK_IN_PROGRESS[work] = performance.now();

            // Send it the work it will handle
            pm2.sendDataToProcessId(PID, {
                type : 'process:msg',
                data : {
                    PID: PID,
                    FILE: work
                },
                topic: "SIRENE-INVADER"
            }
            , (error, result) => {
                if (error) console.error(error);
            });
        }
    }
}

function StartCluster() {
    pm2.start({
        script  : 'worker.js',
        name    : `worker`,
        instances : "max",
        exec_mode : "cluster"
    }, (err, proc) => {
        if (err) console.error(err);
        else {
            for (let i = 0; i < proc.length; ++i) {
                FREE_INSTANCES.push(proc[i].pm_id);
            }
            UpdateWorkers();
        }
    });
}

function FilterHeaders(header) {
    let csv = csv_to_table(header);
    let res = [];

    if (csv.length != 1) throw "Header doesn't exist.";

    let table_header = csv[0];

    const headers = [
        "siren",
        "nic",
        "siret",
        "dateCreationEtablissement",
        "dateDernierTraitementEtablissement",
        "typeVoieEtablissement",
        "libelleVoieEtablissement",
        "codePostalEtablissement",
        "libelleCommuneEtablissement",
        "codeCommuneEtablissement",
        "dateDebut",
        "etatAdministratifEtablissement"
    ];

    for (let i = 0; i < headers.length; ++i) {
        let idx = table_header.indexOf(headers[i]);
        if (idx < 0) throw "Header not found: " + headers[i];
        res.push(idx);
    }
    console.table(res);
    return res;
}

function SaveCSV(buffer, start, end, savePath) {
    fs.writeFileSync(savePath, buffer.slice(start, end));
    QUEUED_WORK.push(savePath);
    UpdateWorkers();
}

function FindFirstCharacter(character, buffer, bufferSize) {
    for (let i = 0; i < bufferSize; ++i) {
        if (String.fromCharCode(buffer[i]) === character) {
            return i;
        }
    }
    return -1;
}

function FindLastCharacter(character, buffer, bufferSize) {
    for (let i = bufferSize - 1; i >= 0; --i) {
        if (String.fromCharCode(buffer[i]) === character) {
            return i;
        }
    }
    return -1;
}

function SplitCSV() {
    const splitSizeInMB = 30;
    const chunkSize = 1024 * 1024 * splitSizeInMB;
    const chunkBuffer = Buffer.alloc(chunkSize);

    let bytesRead = 0;
    let offset = 0;
    let csvId = 0;
    let filter = null;

    const fp = fs.openSync(PATH, 'r');

    while(bytesRead = fs.readSync(fp, chunkBuffer, 0, chunkSize, offset)) {

        let streamEnd = FindLastCharacter("\n", chunkBuffer, bytesRead);
        let streamStart = 0; // Will be used to skip header

        if (streamEnd < 0) {
            throw "The buffer is too small or the file isn't an CSV.";
        } else {
            offset += streamEnd + 1;
        }

        // For the first pass, read the header & assign the filter
        if (filter === null) {
            streamStart = FindFirstCharacter("\n", chunkBuffer, bytesRead);
            filter = FilterHeaders(chunkBuffer.slice(0, streamStart++).toString());
        }

        SaveCSV(chunkBuffer, streamStart, streamEnd, `./data/generated/CSV-${csvId++}.csv`);

        if (csvId > 9) break; // FOR DEBUGGING
    }

    console.log("Done.");
}

pm2.connect(function(err) {
    if (err) {
      console.error(err)
      process.exit(2)
    }

    pm2.launchBus(function(err, pm2_bus) {
        pm2_bus.on('process:msg', function(packet) {
            UpdateWorkers(packet);
        })
    });

    try {
        if (fs.existsSync(PATH)) {
            StartCluster();
            SplitCSV();
        } else {
            console.error(`File doesn't exist: ${PATH}`);
        }
    } catch(err) {
        console.error(err)
    }
  }
);