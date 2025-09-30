const { parseSHR } = require("./parseSHR");
const { parseDEP } = require("./parseDEP");
const { parseARR } = require("./parseARR");

function parseFlightData(data, idCounter) {
  return data.map((row, index) => {
    const shrData = parseSHR(row.shr || "");
    const depData = parseDEP(row.dep || "");
    const arrData = parseARR(row.arr || "");

    // Унифицируем reg: приоритет у SHR, затем DEP, затем ARR
    const reg = shrData.reg || depData.reg || arrData.reg || null;
    
    // Унифицируем sid: приоритет у SHR, затем DEP, затем ARR
    const sid = shrData.sid || depData.sid || arrData.sid || null;

    return {
      id: idCounter + index,
      center: row.center,
      source_sheet: row.source_sheet,
      
      // SHR оставляем нетронутым
      shr: row.shr || "",
      
      // Основные поля
      dof: shrData.dof || null,
      sid: sid,
      reg: reg,
      typ: shrData.typ || null,
      opr: shrData.opr || null,
      sts: shrData.sts || null,
      rmk: shrData.rmk || null,
      eet: shrData.eet || null,
      departure_time: depData.atd || null,
      arrival_time: arrData.ata || null,
      dep_coord: depData.adepz || null,
      dest_coord: arrData.adarrz || null,
      zone: shrData.zone || null,
      operator_phones: shrData.phones || [],
    };
  });
}

// Функция для удаления дубликатов по ключевым полям
function removeDuplicates(flightData) {
  const seen = new Set();
  const uniqueData = [];

  for (const flight of flightData) {
    // Создаем ключ для сравнения - комбинация самых важных полей
    const key = JSON.stringify({
      sid: flight.sid,
      dof: flight.dof,
      reg: flight.reg,
      departure_time: flight.departure_time,
      arrival_time: flight.arrival_time,
      dep_coord: flight.dep_coord ? 
        `${flight.dep_coord.latitude},${flight.dep_coord.longitude}` : null,
      dest_coord: flight.dest_coord ? 
        `${flight.dest_coord.latitude},${flight.dest_coord.longitude}` : null,
      typ: flight.typ,
      opr: flight.opr
    });

    if (!seen.has(key)) {
      seen.add(key);
      uniqueData.push(flight);
    } else {
      console.log(`Дубликат удален: SID=${flight.sid}, DOF=${flight.dof}, REG=${flight.reg}`);
    }
  }

  return uniqueData;
}

module.exports = { parseFlightData, removeDuplicates };