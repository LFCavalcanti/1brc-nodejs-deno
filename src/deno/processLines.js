import { Worker } from "node:worker_threads";
export default async function ProcessLines(filePath, start, end) {
  return new Promise((resolve, reject) => {
    const lineWorker = new Worker("./workerLines.js", {
      workerData: { filePath, start, end },
    });
    lineWorker.on("message", (data) => {
      resolve(data);
    });
    lineWorker.on("error", (data) => {
      reject(data);
    });
  });
}
