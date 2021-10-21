import fs from 'fs';
import { nextTick } from 'process';

const PATH = './data/StockEtablissement_utf8.csv';

function FilterHeaders(header) {
    console.log(header);
    return [0];
}

function SaveCSV(lines, filter, savePath) {
    let data = lines.join("\n");
    //fs.writeFileSync(savePath, data);
    console.log(savePath);
}

function SplitCSV() {
    let lines = [];
    const chunkSize = 1024 * 1024;
    const chunkBuffer = Buffer.alloc(chunkSize);

    let currentLine = null;
    let totalLineCount = 0;
    let bytesRead = 0;
    let offset = 0;
    let csvId = 0;
    let filter = null;

    const fp = fs.openSync(PATH, 'r');

    while(bytesRead = fs.readSync(fp, chunkBuffer, 0, chunkSize, offset)) {
        offset += bytesRead;
        console.log(chunkBuffer[0] + " " + String.fromCharCode(chunkBuffer[0]));

        const rawData = chunkBuffer.slice(0, bytesRead);
        const strData = rawData.toString();

        let lineStart = 0;

        // Split all the lines (account for when we don't read all the line)
        while (true) {
            let r = strData.indexOf("\n", lineStart);
            if (r < 0) break;

            const line = chunkBuffer.slice(lineStart, r - 1).toString();
            totalLineCount += 1;

            if (currentLine !== null) {
                lines.push(`${currentLine}${line}`);
                currentLine = null;
            } else {
                lines.push(line);
            }

            lineStart = r + 1;
        }

        // Trailling data, we will catch this next read
        if (lineStart < bytesRead) {
            const remainder = chunkBuffer.slice(lineStart, bytesRead).toString();
            if (currentLine === null) {
                currentLine = remainder;
            } else {
                currentLine += remainder;
            }
        }

        if (lines.length > 250000) {
            // For the first pass, read the header & assign the filter
            if (filter === null) {
                filter = FilterHeaders(lines[0]);
                lines.shift();
            }
            SaveCSV(lines, filter, `./data/generated/CSV-${csvId++}.csv`);
            lines = [];
        }
    }

    if (currentLine !== null) {
        totalLineCount += 1;
        lines.push(currentLine);
    }

    if (lines.length > 0) {
        SaveCSV(lines, filter, `./data/generated/CSV-${csvId++}.csv`);
        lines = [];
    }

    if (totalLineCount != 31814267) {
        console.error("Something is wrong, read " + totalLineCount + " lines, but should be 31814267");
    }
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
