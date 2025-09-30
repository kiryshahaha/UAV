const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const { cleanColumnNames } = require("./cleanColumnNames");
const { cleanDataframe } = require("./cleanDataframe");
const { parseFlightData, removeDuplicates } = require("./parseFlightData");

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
    data = parseFlightData(data, idCounter);
    idCounter += data.length;

    combinedData.push(...data);
    console.log(`Processed sheet '${sheetName}': ${data.length} rows`);
  }


  const uniqueData = removeDuplicates(combinedData);

  if (uniqueData.length) {
    console.log(
      `Combined total after deduplication: ${uniqueData.length} rows`
    );
    try {
      fs.writeFileSync(
        OUTPUT_JSON_PATH,
        JSON.stringify(uniqueData, null, 2),
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

main().catch((error) => {
  console.error("Error:", error);
});
