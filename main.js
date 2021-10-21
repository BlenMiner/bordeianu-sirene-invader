import fs from 'fs';

const PATH = './data/StockEtablissement_utf8.csv';

function FilterHeaders(header) {
    console.log(header);
    return [0];
}

function SaveCSV(buffer, size, filter, savePath) {
    fs.writeFileSync(savePath, buffer.slice(0, size));
    console.log(savePath + " " + size);
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
    const chunkSize = 1024 * 1024 * 35;
    const chunkBuffer = Buffer.alloc(chunkSize);

    let bytesRead = 0;
    let offset = 0;
    let csvId = 0;
    let filter = null;

    const fp = fs.openSync(PATH, 'r');

    while(bytesRead = fs.readSync(fp, chunkBuffer, 0, chunkSize, offset)) {

        let streamEnd = FindLastCharacter("\n", chunkBuffer, bytesRead);

        if (streamEnd < 0) {
            throw "The buffer is too small or the file isn't an CSV.";
        } else {
            offset += streamEnd + 1;
        }

        // For the first pass, read the header & assign the filter
        if (filter === null) {
            filter = FilterHeaders(chunkBuffer.slice(0, bytesRead).toString().split("\n")[0]);
        }

        SaveCSV(chunkBuffer, streamEnd, filter, `./data/generated/CSV-${csvId++}.csv`);

        if (csvId > 4) break;
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
