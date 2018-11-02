"use strict";

const fs = require("fs");
const readline = require("readline");
const zlib = require("zlib");

/**
 * Обрабатывает файл лога - собирает отчет
 * 
 * @param {String} gzippedLogFile путь к файлу
 * @param {Object} period период {begin, end}
 */
async function collectReport(gzippedLogFile, period) {
  const { begin, end } = period;
  const data = await new Promise((resolve, reject) => {
    const report = {};
    const reader = readline.createInterface({
      input: fs
        .createReadStream(gzippedLogFile)
        .pipe(zlib.createGunzip())
        .on("error", reject)
    });

    reader.on("line", line => {
      let data;
      try {
        data = JSON.parse(line);
      } catch (err) {        
        process.stderr.write(
          `ERROR in ${gzippedLogFile}: Cannot convert to JSON line: "${line}"\n`
        );
        return;
      }      
      const { event_type, campaign_id, time } = data;
      const eventTime = new Date(time);
      const periodIncludes = begin <= eventTime && eventTime < end;
      if (event_type === "show" && periodIncludes) {
        report[campaign_id] = report[campaign_id] || 0;
        report[campaign_id]++;
      }
    });

    reader.on("close", () => {
      resolve(report);
    });
  });

  return data;
}

module.exports = collectReport;
