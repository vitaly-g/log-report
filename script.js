"use strict";

const path = require("path");
const { promisify } = require("util");
const { readdir } = require("fs");

const collectReport = require("./collect-report");

const fsReadDir = promisify(readdir);

let periodBegin;
let periodEnd;

/**
 * Если является действительной датой, то возвращает true, иначе false
 * @param {any} date 
 */
function isValidDate(date) {
  return date instanceof Date && !isNaN(date);
}

try {
  const args = process.argv;
  periodBegin = new Date(args[2]);
  periodEnd = new Date(args[3]);
  if (!isValidDate(periodBegin)) {
    throw new Error(`${args[2]} not a valid ISO date`);
  }
  if (!isValidDate(periodEnd)) {
    throw new Error(`${args[3]} not a valid ISO date`);
  }
  process.stdout.write(
    `Start process for period: ${periodBegin.toISOString()} - ${periodEnd.toISOString()}\n`
  );
} catch (err) {
  process.stderr.write(`${err}\n`);
  process.stdout.write(
    "Use:\n\tnode script.js <Period begin - ISO Date, ex: 2018-11-01> <Period end - ISO Date, ex: 2018-11-30>"
  );
  process.exit(1);
}

const dataDir = "./data";

/**
 * Выполняет обработку файлов и собирает отчет
 */
async function start() {
  const dir = await fsReadDir(dataDir);
  const reports = await Promise.all(
    dir.filter(fileName => fileName.endsWith(".gz")).map(async fileName => {
      process.stdout.write(`Processing file ${fileName}...\n`);
      const report = await collectReport(path.join(dataDir, fileName), {
        begin: periodBegin,
        end: periodEnd
      });
      process.stdout.write(`File ${fileName} processed\n`);

      return report;
    })
  );

  const totalReport = reports.reduce((p, c) => {
    Object.keys(c).forEach(key => {
      p[key] = (p[key] || 0) + c[key];
    });

    return p;
  }, {});

  process.stdout.write("Report: \n");
  Object.keys(totalReport).forEach(campaignId => {
    process.stdout.write(`${campaignId}: ${totalReport[campaignId]}\n`);
  });
  process.stdout.write("\n...done\n");
}

start();
