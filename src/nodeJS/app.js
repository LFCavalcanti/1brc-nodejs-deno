import fs from "node:fs";
import os from "node:os";
import { Worker } from "node:worker_threads";

console.time("Execution Time");
const filePath = process.argv[2];

const MAX_LINE_LENGTH = 106;
const file = await fs.promises.open(filePath);
const FILE_SIZE = (await file.stat()).size;
const MAX_WORKERS = os.availableParallelism();

const SEGMENT_SIZE = Math.floor(FILE_SIZE / MAX_WORKERS);
const offsets = [];
const bufferToFindOffsets = Buffer.alloc(MAX_LINE_LENGTH);
const processedStations = new Map();
let offset = 0;
let finishedWorkers = 0;

const processResult = () => {
  const sortedStationNames = Array.from(processedStations.keys()).sort();
  const len = sortedStationNames.length;
  process.stdout.write("{");
  for (let idx = 0; idx < len; idx++) {
    if (idx > 0) {
      process.stdout.write(", ");
    }
    const stationData = processedStations.get(sortedStationNames[idx]);
    process.stdout.write(sortedStationNames[idx]);
    process.stdout.write("=");
    process.stdout.write(
      `${stationData.min / 10}/${(
        stationData.sum /
        10 /
        stationData.count
      ).toFixed(1)}/${stationData.max / 10}`
    );
  }

  process.stdout.write("}\n");
  console.timeEnd("Execution Time");
};

while (true) {
  offset += SEGMENT_SIZE;

  if (offset >= FILE_SIZE) {
    offsets.push(FILE_SIZE);
    break;
  }
  await file.read(bufferToFindOffsets, 0, MAX_LINE_LENGTH, offset);
  const lineEndPos = bufferToFindOffsets.indexOf(10);
  if (lineEndPos === -1) {
    chunkOffsets.push(FILE_SIZE);
    break;
  } else {
    offset += lineEndPos + 1;
    offsets.push(offset);
  }
}

console.log(offsets);

await file.close();

// const workersToFire = offsets.length;
for (let workerNum = 0; workerNum < offsets.length; workerNum++) {
  const lineWorker = new Worker("./workerLines.js", {
    workerData: {
      filePath,
      start: workerNum === 0 ? 0 : offsets[workerNum - 1],
      end: offsets[workerNum] - 1,
    },
  });
  lineWorker.on("message", (returnedResult) => {
    for (let [stationName, stationData] of returnedResult.entries()) {
      let currData = processedStations.get(stationName);
      if (!currData) {
        processedStations.set(stationName, stationData);
      } else {
        if (stationData.min < currData.min) currData.min = stationData.min;
        if (stationData.max > currData.max) currData.max = stationData.max;
        currData.sum += stationData.sum;
        currData.count += stationData.count;
      }
    }
  });
  lineWorker.on("error", (result) => {
    console.error(result.reason);
    throw new Error("Worker threads failed to process lines");
  });
  lineWorker.on("exit", (data) => {
    // if (data !== 0) {
    //   console.error(data);
    //   throw new Error("Worker threads failed to process lines");
    // }
    finishedWorkers++;
    if (finishedWorkers === offsets.length) {
      processResult();
    }
  });
}
