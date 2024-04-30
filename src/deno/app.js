import * as mod from "https://deno.land/std@0.177.0/node/os.ts";

console.time("Execution Time");
const filePath = Deno.args[0];
// console.log(filePath);

const MAX_LINE_LENGTH = 106;
const file = await Deno.open(filePath);
const fileStats = await Deno.stat(filePath);
const FILE_SIZE = fileStats.size;
const MAX_WORKERS = mod.cpus().length;
const SEGMENT_SIZE = Math.floor(FILE_SIZE / MAX_WORKERS);
const offsets = [];
const bufferToFindOffsets = new Uint8Array(MAX_LINE_LENGTH);
const processedStations = new Map();
const textEnconder = new TextEncoder();
let offset = 0;
let finishedWorkers = 0;

const processResult = () => {
  const sortedStationNames = Array.from(processedStations.keys()).sort();
  const len = sortedStationNames.length;
  console.log("STATIONS NUM: ", len);
  Deno.writeAllSync(Deno.stdout, textEnconder.encode("{"));
  for (let idx = 0; idx < len; idx++) {
    if (idx > 0) {
      Deno.writeAllSync(Deno.stdout, textEnconder.encode(", "));
    }
    const stationData = processedStations.get(sortedStationNames[idx]);
    Deno.writeAllSync(
      Deno.stdout,
      textEnconder.encode(sortedStationNames[idx])
    );
    Deno.writeAllSync(Deno.stdout, textEnconder.encode("="));
    Deno.writeAllSync(
      Deno.stdout,
      textEnconder.encode(
        `${stationData.min / 10}/${(
          stationData.sum /
          10 /
          stationData.count
        ).toFixed(1)}/${stationData.max / 10}`
      )
    );
  }

  // Deno.stdout.write("}\n");
  Deno.writeAllSync(Deno.stdout, textEnconder.encode("}\n"));
  console.timeEnd("Execution Time");
  return;
};

while (true) {
  offset += SEGMENT_SIZE;

  if (offset >= FILE_SIZE) {
    offsets.push(FILE_SIZE);
    break;
  }
  await file.seek(offset, Deno.SeekMode.Start);
  await file.read(bufferToFindOffsets);

  const lineEndPos = bufferToFindOffsets.indexOf(10);
  if (lineEndPos === -1) {
    chunkOffsets.push(FILE_SIZE);
    break;
  } else {
    offset += lineEndPos + 1;
    offsets.push(offset);
  }
}

// console.log("OFFSETS: ", offsets);

await file.close();

// const workersToFire = offsets.length;
for (let workerNum = 0; workerNum < offsets.length; workerNum++) {
  const lineWorker = new Worker(import.meta.resolve("./workerLines.js"), {
    type: "module",
  });
  lineWorker.postMessage({
    filePath,
    start: workerNum === 0 ? 0 : offsets[workerNum - 1],
    end: offsets[workerNum] - 1,
  });

  lineWorker.addEventListener("message", (returnedResult) => {
    const returnedMap = returnedResult.data;
    for (let [stationName, stationData] of returnedMap.entries()) {
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
    finishedWorkers++;
    if (finishedWorkers === offsets.length) {
      processResult();
    }
    return;
  });
  lineWorker.addEventListener("error", (result) => {
    console.error(result.reason);
    throw new Error("Worker threads failed to process lines");
  });
}
