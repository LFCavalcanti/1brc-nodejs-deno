import { Buffer } from "https://deno.land/std@0.139.0/node/buffer.ts";
import { ByteSliceStream } from "https://deno.land/std/streams/byte_slice_stream.ts";
import { readerFromStreamReader } from "https://deno.land/std/io/mod.ts";

const SEMICOLON = 59; //;
const POINT = 46; //.
const LINEEND = 10; //\n
const processedLines = new Map();
// const textDecoder = new TextDecoder();

// let stationName = new Uint8Array(100);
let stationName = new Buffer.allocUnsafe(100);
let stationNameLen = 0;
let temperature = new Uint8Array(5);
let temperatureLen = 0;
let segmentBeingRead = 0; // 0 = Station Name, 1 = temp, 2 = LineEnd
// let readBuffer = new Uint8Array(26500);
// let readBuffer = new Uint8Array(65536);
let readBuffer = new Uint8Array(1048576);

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

// function ConvertBufferToString(buffer, nameLen) {
//   let decodedString = "";
//   for (let idx = 0; idx < nameLen; idx++) {
//     if (buffer[idx] === LINEEND) break;
//     decodedString += String.fromCharCode(buffer[idx]);
//   }
//   return decodedString;
// }

const processChunk = async (chunkBuffer, currChunkLen) => {
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
      let stationNameStr = stationName.toString(
        "utf8",
        stationName[0] === LINEEND ? 1 : 0,
        // 0,
        stationNameLen
      );
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

self.onmessage = async (messageData) => {
  const file = await Deno.open(messageData.data.filePath);
  await file.seek(messageData.data.start, Deno.SeekMode.Start);
  const slicestart = 0;
  const sliceend = messageData.data.end - messageData.data.start;
  const slice = file.readable.pipeThrough(
    new ByteSliceStream(slicestart, sliceend)
  );
  const fileReaderOri = slice.getReader();

  if (fileReaderOri) {
    const fileReader = readerFromStreamReader(fileReaderOri);
    let numberRead = 0;
    do {
      numberRead = (await fileReader.read(readBuffer)) || 0;
      if (numberRead == 0) break;
      await processChunk(readBuffer, numberRead);
    } while (true);
    self.postMessage(processedLines);
    self.close();
  }
};
