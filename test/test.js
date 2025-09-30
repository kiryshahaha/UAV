// process_excel.js - FIXED VERSION
const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");

const EXCEL_FILE_PATH =
  process.argv[2] ||
  "C:\\Users\\soapd\\Documents\\VS\\React\\UAV\\test\\data.xlsx";
const OUTPUT_JSON_PATH = path.join(
  path.dirname(EXCEL_FILE_PATH),
  "processed_data.json"
);

async function main() {
  console.log(`Processing Excel file: ${EXCEL_FILE_PATH}`);

  let workbook;
  try {
    workbook = XLSX.readFile(EXCEL_FILE_PATH);
  } catch (error) {
    console.error(`Error reading Excel file: ${error}`);
    return;
  }

  const sheetNames = workbook.SheetNames;
  console.log(`Found sheets: ${sheetNames.join(", ")}`);

  let combinedData = [];
  let idCounter = 1;

  for (const sheetName of sheetNames) {
    const worksheet = workbook.Sheets[sheetName];
    let data = XLSX.utils.sheet_to_json(worksheet, {
      defval: null,
      blankrows: false,
    });

    if (!data.length) {
      console.log(`Sheet '${sheetName}' is empty, skipping`);
      continue;
    }

    const originalHeaders = Object.keys(data[0]);
    const cleanedHeaders = cleanColumnNames(originalHeaders);

    data = data.map((row) => {
      const newRow = {};
      originalHeaders.forEach((orig, idx) => {
        newRow[cleanedHeaders[idx]] = row[orig];
      });
      newRow.source_sheet = sheetName;
      return newRow;
    });

    data = cleanDataframe(data, cleanedHeaders);
    data = decodeFlightPlanFields(data, idCounter);
    idCounter += data.length;

    combinedData.push(...data);
    console.log(`Processed sheet '${sheetName}': ${data.length} rows`);
  }

  if (combinedData.length) {
    console.log(`Combined total: ${combinedData.length} rows`);
    try {
      fs.writeFileSync(
        OUTPUT_JSON_PATH,
        JSON.stringify(combinedData, null, 2),
        "utf-8"
      );
      console.log(`Output saved to: ${OUTPUT_JSON_PATH}`);
    } catch (error) {
      console.error(`Error writing JSON file: ${error}`);
    }
  } else {
    console.log("No data to save");
  }

  console.log("Processing complete!");
}

function cleanColumnNames(columns) {
  const cyrillicToLatin = {
    а: "a",
    б: "b",
    в: "v",
    г: "g",
    д: "d",
    е: "e",
    ё: "yo",
    ж: "zh",
    з: "z",
    и: "i",
    й: "y",
    к: "k",
    л: "l",
    м: "m",
    н: "n",
    о: "o",
    п: "p",
    р: "r",
    с: "s",
    т: "t",
    у: "u",
    ф: "f",
    х: "h",
    ц: "ts",
    ч: "ch",
    ш: "sh",
    щ: "sch",
    ъ: "",
    ы: "y",
    ь: "",
    э: "e",
    ю: "yu",
    я: "ya",
    А: "A",
    Б: "B",
    В: "V",
    Г: "G",
    Д: "D",
    Е: "E",
    Ё: "YO",
    Ж: "ZH",
    З: "Z",
    И: "I",
    Й: "Y",
    К: "K",
    Л: "L",
    М: "M",
    Н: "N",
    О: "O",
    П: "P",
    Р: "R",
    С: "S",
    Т: "T",
    У: "U",
    Ф: "F",
    Х: "H",
    Ц: "TS",
    Ч: "CH",
    Ш: "SH",
    Щ: "SCH",
    Ъ: "",
    Ы: "Y",
    Ь: "",
    Э: "E",
    Ю: "YU",
    Я: "YA",
  };

  return columns.map((col, idx) => {
    if (typeof col !== "string") col = String(col);
    col = col.normalize("NFKD");
    for (const [cyr, lat] of Object.entries(cyrillicToLatin)) {
      col = col.replace(new RegExp(cyr, "g"), lat);
    }
    col = col
      .toLowerCase()
      .replace(/[\s\-\.\/\\]+/g, "_")
      .replace(/[^a-z0-9_]/g, "")
      .replace(/_+/g, "_")
      .replace(/^_|_$/g, "");

    if (!col || /^\d/.test(col)) col = `column_${idx}`;
    return col;
  });
}

function cleanDataframe(data, headers) {
  return data.filter((row) =>
    headers.some((header) => {
      const value = row[header];
      return (
        value !== null &&
        value !== undefined &&
        value !== "" &&
        (!Array.isArray(value) || value.length > 0) &&
        (typeof value !== "object" || Object.keys(value).length > 0)
      );
    })
  );
}

function parseCoordinate(coord) {
  if (!coord || typeof coord !== "string") return null;

  coord = coord.replace(/\s+/g, "").toUpperCase();

  // Pattern 1: ddmmNdddmmE (e.g., 5957N02905E)
  let match = coord.match(/^(\d{2})(\d{2})([NS])(\d{3})(\d{2})([EW])$/);
  if (match) {
    const latDeg = parseInt(match[1], 10);
    const latMin = parseInt(match[2], 10);
    const lat = (match[3] === "N" ? 1 : -1) * (latDeg + latMin / 60);

    const lonDeg = parseInt(match[4], 10);
    const lonMin = parseInt(match[5], 10);
    const lon = (match[6] === "E" ? 1 : -1) * (lonDeg + lonMin / 60);

    return { latitude: +lat.toFixed(6), longitude: +lon.toFixed(6) };
  }

  // Pattern 2: ddmmssNdddmmssE (e.g., 440846N0430829E)
  match = coord.match(
    /^(\d{2})(\d{2})(\d{2})([NS])(\d{3})(\d{2})(\d{2})([EW])$/
  );
  if (match) {
    const latDeg = parseInt(match[1], 10);
    const latMin = parseInt(match[2], 10);
    const latSec = parseInt(match[3], 10);
    const lat =
      (match[4] === "N" ? 1 : -1) * (latDeg + latMin / 60 + latSec / 3600);

    const lonDeg = parseInt(match[5], 10);
    const lonMin = parseInt(match[6], 10);
    const lonSec = parseInt(match[7], 10);
    const lon =
      (match[8] === "E" ? 1 : -1) * (lonDeg + lonMin / 60 + lonSec / 3600);

    return { latitude: +lat.toFixed(6), longitude: +lon.toFixed(6) };
  }

  return null;
}

function normalizePhones(message) {
  if (!message) return [];

  const phoneRegex =
    /(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}|\b\d{10,11}\b/g;
  const rawPhones = message.match(phoneRegex) || [];

  return rawPhones
    .map((p) => p.replace(/[^\d\+]/g, ""))
    .map((p) => {
      if (p.startsWith("8") && p.length === 11) {
        return "+7" + p.slice(1);
      }
      if (p.length === 10) {
        return "+7" + p;
      }
      return p;
    })
    .filter((p) => p.length >= 11 && p.length <= 12)
    .filter((p) => !/^\+?7\d{10}$/.test(p) || !p.match(/7{3,}|\d{10}$/)) // Exclude SID-like numbers
    .filter((p, index, self) => self.indexOf(p) === index);
}

function decodeFlightPlanFields(data, idCounter) {
  const timePatternICAO = /(\d{4})/g;
  const coordPattern = /(\d{4,6}[NS]\d{5,7}[EW])/gi;
  const zonePattern = /ZONA\s+([^\/\r\n]+)/i;

  return data.map((row, index) => {
    let departureTime = null;
    let arrivalTime = null;

    const sources = [row.shr || "", row.dep || "", row.arr || ""]
      .filter(Boolean)
      .map((s) => s.toString().replace(/\r\n/g, " "));

    const combinedMessage = sources.join(" ");

    // Parse times
    const depMatch = (row.dep || "").toString().match(/-ATD\s+(\d{4})/);
    if (depMatch) {
      const timeStr = depMatch[1];
      departureTime = `${timeStr.slice(0, 2)}:${timeStr.slice(2)}:00`;
    }

    const arrMatch = (row.arr || "").toString().match(/-ATA\s+(\d{4})/);
    if (arrMatch) {
      const timeStr = arrMatch[1];
      arrivalTime = `${timeStr.slice(0, 2)}:${timeStr.slice(2)}:00`;
    }

    // Fallback to SHR for times, excluding altitude codes
    if (!departureTime || (!arrMatch && !arrivalTime)) {
      const shrTimes = [...(row.shr || "").toString().matchAll(timePatternICAO)]
        .filter((m) => !/M\d{4}|K\d{4}/.test(m[0]))
        .map((m) => {
          const timeStr = m[1];
          return `${timeStr.slice(0, 2)}:${timeStr.slice(2)}:00`;
        })
        .filter((t) => t !== "00:00:00" && t !== "24:00:00");

      if (!departureTime && shrTimes[0]) departureTime = shrTimes[0];
      if (!arrMatch && shrTimes[1]) arrivalTime = shrTimes[1];
    }

    if (departureTime === "24:00:00") departureTime = "00:00:00";
    if (arrivalTime === "24:00:00") arrivalTime = "00:00:00";

    // Parse coordinates
    const depCoord =
      combinedMessage.match(/ADEPZ\s+(\d{4,6}[NS]\d{5,7}[EW])/i)?.[1] ||
      combinedMessage.match(/DEP\/(\d{4,6}[NS]\d{5,7}[EW])/i)?.[1] ||
      null;
    const destCoord =
      combinedMessage.match(/ADARRZ\s+(\d{4,6}[NS]\d{5,7}[EW])/i)?.[1] ||
      combinedMessage.match(/DEST\/(\d{4,6}[NS]\d{5,7}[EW])/i)?.[1] ||
      null;

    const coordinates = [...combinedMessage.matchAll(coordPattern)]
      .map((m) => parseCoordinate(m[1]))
      .filter(Boolean)
      .filter(
        (coord, index, self) =>
          self.findIndex(
            (c) =>
              c.latitude === coord.latitude && c.longitude === coord.longitude
          ) === index
      );

    // Parse ZONA
    let zone = null;
    const zoneMatch = combinedMessage.match(zonePattern);
    if (zoneMatch) {
      const zoneData = zoneMatch[1].trim();
      const zoneCoords = [...zoneData.matchAll(/\d{4,6}[NS]\d{5,7}[EW]/gi)]
        .map((m) => parseCoordinate(m[0]))
        .filter(Boolean);

      zone = zoneCoords.length > 0 ? zoneCoords : zoneData;
    }

    // Parse phones
    const phones = normalizePhones(combinedMessage);

    // Parse other fields
    const regMatch =
      combinedMessage.match(/REG\/([^\/\r\n]+?)(?=\/|$|\s+[A-Z]+\/)/i) ||
      combinedMessage.match(/-REG\s+([^ \)]+)/i);
    const oprMatch = combinedMessage.match(/OPR\/(.+?)(?=\s+[A-Z]{3}\/|$)/i);
    const rmkMatch = combinedMessage.match(/RMK\/(.+?)(?=\s+SID\/|$)/i);
    const stsMatch = combinedMessage.match(/STS\/([^ \)]+)/g);
    const dofMatch = combinedMessage.match(/DOF\/(\d{6})/i);
    const sidMatch =
      combinedMessage.match(/SID\/(\d+)/i) ||
      combinedMessage.match(/-SID\s+(\d+)/i);
    const typMatch = combinedMessage.match(/TYP\/([^ \)]+)/i);
    const eetMatch = combinedMessage.match(/EET\/([^ \)]+)/i);

    row.id = idCounter + index;
    row.center = row.tsentr_es_orvd || null;
    row.dof = dofMatch ? dofMatch[1] : null;
    row.sid = sidMatch ? sidMatch[1] : null;
    row.reg = regMatch
      ? regMatch[1]
          .trim()
          .split(/[\s,]+/)
          .filter(Boolean)
          .map((r) => r.replace(/,$/, ""))
          .join(", ")
      : null;
    row.typ = typMatch ? typMatch[1] : null;
    row.opr = oprMatch ? oprMatch[1].trim() : null;
    row.sts = stsMatch ? stsMatch.map((m) => m.replace("STS/", "")) : null;
    row.rmk = rmkMatch ? rmkMatch[1].trim() : null;
    row.eet = eetMatch ? eetMatch[1] : null;
    row.departure_time = departureTime;
    row.arrival_time = arrMatch ? arrivalTime : null;
    row.dep_coord = depCoord ? parseCoordinate(depCoord) : null;
    row.dest_coord = destCoord ? parseCoordinate(destCoord) : null;
    row.coordinates = coordinates.length > 0 ? coordinates : null;
    row.zone = zone;
    row.operator_phones = phones.length > 0 ? phones : null;

    return row;
  });
}

main().catch((error) => {
  console.error("Error:", error);
});
