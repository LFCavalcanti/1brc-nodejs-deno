import { workerData, parentPort } from "worker_threads";
import fs from "node:fs";

// const SEMICOLON = ";".charCodeAt(); //59
// const POINT = ".".charCodeAt(); //46
// const LINEEND = "\n".charCodeAt(); //10
const SEMICOLON = 59; //;
const POINT = 46; //.
// const LINEEND = 10; //\n
const processedLines = new Map();

let stationName = Buffer.allocUnsafe(100);
let stationNameLen = 0;
let temperature = Buffer.allocUnsafe(5);
let temperatureLen = 0;
let segmentBeingRead = 0; // 0 = Station Name, 1 = temp, 2 = LineEnd

// -999 -99 99 999
function ConvertBufferToInt(buffer, numberLen) {
  if (buffer[0] === 45) {
    if (numberLen === 4) {
      return (
        -((buffer[1] - 0x30) * 100) +
        (buffer[2] - 0x30) * 10 +
        (buffer[3] - 0x30)
      );
    } else {
      return -((buffer[2] - 0x30) * 10) + (buffer[3] - 0x30);
    }
  } else {
    if (numberLen === 3) {
      return (
        (buffer[0] - 0x30) * 100 + (buffer[1] - 0x30) * 10 + (buffer[2] - 0x30)
      );
    } else {
      return (buffer[0] - 0x30) * 10 + (buffer[1] - 0x30);
    }
  }
}

const processChunk = async (chunk) => {
  let chunkBuffer = Buffer.from(chunk);
  let currChunkLen = chunkBuffer.length;
  for (let index = 0; index < currChunkLen; index++) {
    //STATION NAME
    if (segmentBeingRead === 0) {
      if (chunkBuffer[index] === SEMICOLON) {
        segmentBeingRead = 1;
        continue;
      }
      stationName[stationNameLen] = chunkBuffer[index];
      stationNameLen++;
    } else if (segmentBeingRead === 1) {
      if (chunkBuffer[index] === POINT) {
        temperature[temperatureLen] = chunkBuffer[index + 1];
        temperatureLen++;
        index++;
        segmentBeingRead = 2;
        continue;
      }
      temperature[temperatureLen] = chunkBuffer[index];
      temperatureLen++;
      //LINE END
    } else {
      let stationNameStr = stationName.toString("utf8", 0, stationNameLen);
      let temp = ConvertBufferToInt(temperature, temperatureLen);
      let currData = processedLines.get(stationNameStr);
      if (!currData) {
        processedLines.set(stationNameStr, {
          min: temp,
          max: temp,
          sum: temp,
          count: 1,
        });
      } else {
        if (temp < currData.min) currData.min = temp;
        if (temp > currData.max) currData.max = temp;
        currData.sum += temp;
        currData.count++;
      }
      stationNameLen = 0;
      temperatureLen = 0;
      segmentBeingRead = 0;
    }
  }
};

const fileReadStream = fs.createReadStream(workerData.filePath, {
  start: workerData.start,
  end: workerData.end,
});
fileReadStream.on("data", processChunk);
fileReadStream.on("end", () => {
  parentPort.postMessage(processedLines);
});
