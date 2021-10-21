import fs from 'fs';

const PATH = './data/StockEtablissement_utf8.csv';

function FilterHeaders(header) {
    console.log(header);
    return [0];
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
