import fs from 'fs';
import { csv_to_table } from './csv.js'

const PATH = './data/StockEtablissement_utf8.csv';

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
    console.log(res);
    return res;
}

function SaveCSV(buffer, start, end, filter, savePath) {
    fs.writeFileSync(savePath, buffer.slice(start, end));
    console.log(savePath + " " + end);
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

        SaveCSV(chunkBuffer, streamStart, streamEnd, filter, `./data/generated/CSV-${csvId++}.csv`);

        if (csvId > 9) break; // FOR DEBUGGING
    }

    console.log("Done.");
}

try {
    if (fs.existsSync(PATH)) {
        SplitCSV();
    } else {
        console.error(`File doesn't exist: ${PATH}`);
    }
} catch(err) {
    console.error(err)
}
