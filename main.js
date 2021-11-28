import fs from 'fs';
import pm2 from 'pm2';
import { performance } from 'perf_hooks';
import { csv_to_table } from './csv.js';

const PATH = './data/StockEtablissement_utf8.csv';

const WORK_IN_PROGRESS = {};
const QUEUED_WORK = [];
const FREE_INSTANCES = [];
let TIMES_TOTAL = 0;
let TIMES_COUNT = 0;
let BUSY_INSTANCES = 0;
let FINISHED_SPLITTING = false;
var FILTER = undefined;
const PROCESS_START = performance.now();

function UpdateWorkers(packet = null) {
    if (packet !== null) {
        if (packet.data.READY) {
            FREE_INSTANCES.push(packet.data.PID);
        } else {
            // worker finished working
            FREE_INSTANCES.push(packet.data.PID);
            BUSY_INSTANCES -= 1;

            const delay = performance.now() - WORK_IN_PROGRESS[packet.data.FILE];
            TIMES_TOTAL += delay;
            TIMES_COUNT += 1;
            console.log("Took " + delay + " ms for " + packet.data.FILE);
        }
    }

    if (QUEUED_WORK.length > 0 && FREE_INSTANCES.length > 0 && FILTER !== undefined) {
        let instanceReadyCount = Math.min(QUEUED_WORK.length, FREE_INSTANCES.length);

        for (let i = 0; i < instanceReadyCount; ++i) {
            const PID = FREE_INSTANCES[i];

            // Remove it from the FREE list
            FREE_INSTANCES.splice(i, 1);
            BUSY_INSTANCES += 1;
            
            i -= 1;
            instanceReadyCount -= 1;

            const work = QUEUED_WORK.shift();

            WORK_IN_PROGRESS[work] = performance.now();

            // Send it the work it will handle
            pm2.sendDataToProcessId(PID, {
                type : 'process:msg',
                data : {
                    FILE: work,
                    FILTER: FILTER
                },
                topic: "SIRENE-INVADER"
            }
            , (error, result) => {
                if (error) console.error(error);
            });
        }
    } 
    else if (QUEUED_WORK.length == 0 && FINISHED_SPLITTING && BUSY_INSTANCES == 0)
    {
        console.log('\x1b[36m%s\x1b[0m', "Finished indexing ");

        let totalMs = (performance.now() - PROCESS_START) / 1000;
        let seconds = TIMES_TOTAL / 1000;

        console.log('\x1b[36m%s\x1b[0m', "Total of " + totalMs + " seconds to finish.");
        console.log('\x1b[36m%s\x1b[0m', "Total of " + seconds + " seconds on indexation.");
        console.log('\x1b[36m%s\x1b[0m', "Average of " + (seconds / TIMES_COUNT) + " seconds per worker.");
    }
}

function StartCluster() {
    pm2.start({
        script  : 'worker.js',
        name    : `sirene-worker`,
        instances : "max",
        exec_mode : "cluster"
    }, (err, _) => {
        if (err) console.error(err);
    });
}

function FilterHeaders(header) {
    let csv = csv_to_table(header);
    let res = {};

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
        res[headers[i]] = idx;
    }
    
    return res;
}

async function SaveCSV(buffer, start, end, savePath) {
    await fs.promises.writeFile(savePath, buffer.slice(start, end));
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

const splitSizeInMB = 30;
const chunkSize = 1024 * 1024 * splitSizeInMB;
const chunkBuffer = Buffer.alloc(chunkSize);

async function SplitCSV() {
    const splitStart = performance.now();
    let bytesRead = 0;
    let offset = 0;
    let csvId = 0;

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
        if (FILTER === undefined) {
            streamStart = FindFirstCharacter("\n", chunkBuffer, bytesRead);
            FILTER = FilterHeaders(chunkBuffer.slice(0, streamStart++).toString());
        }

        await SaveCSV(chunkBuffer, streamStart, streamEnd, `./data/generated/CSV-${csvId++}.csv`);

        UpdateWorkers();
    }

    
    FINISHED_SPLITTING = true;
    let totalSeconds = (performance.now() - splitStart) / 1000;
    console.log('\x1b[36m%s\x1b[0m', "Split CSV into " + csvId + " chunks of " + splitSizeInMB + "MB in " + totalSeconds + " seconds.");
    UpdateWorkers();
}

pm2.connect(async function(err) {
    if (err) {
      console.error(err)
      process.exit(2)
    }

    pm2.launchBus(function(_err, pm2_bus) {
        pm2_bus.on('process:msg', function(packet) {
            UpdateWorkers(packet);
        })
    });

    try {
        if (fs.existsSync(PATH)) {
            console.log("Starting clusters...");

            StartCluster();

            console.log("Starting to split CSV...");

            await SplitCSV();

        } else {
            console.error(`File doesn't exist: ${PATH}`);
        }
    } catch(err) {
        console.error(err)
    }
  }
);