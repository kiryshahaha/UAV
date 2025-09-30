const { parseCoordinate } = require("./parseCoordinate");
const { parsePhones } = require("./parsePhones");

function parseSHR(message) {
  if (!message) return {};

  const result = {};
  const cleanMessage = message.replace(/\r\n/g, " ");
  const timePattern = /\b(\d{4})\b(?![NS]\d{5,7}[EW]|\+\d{10})/g; // Исключаем координаты и телефоны
  const coordPattern = /(\d{4,6}[NS]\d{5,7}[EW])/gi;
  const zonePattern = /ZONA\s+([^\/\r\n]+)/i;

  // Парсим времена
  const times = [...cleanMessage.matchAll(timePattern)]
    .map((m) => m[1])
    .filter((t) => {
      const hours = parseInt(t.slice(0, 2), 10);
      const minutes = parseInt(t.slice(2), 10);
      return hours < 24 && minutes < 60; // Только валидные ЧЧММ
    })
    .map((t) => `${t.slice(0, 2)}:${t.slice(2)}:00`)
    .filter((t) => t !== "00:00:00" && t !== "24:00:00");

  result.times = times.length > 0 ? times : null;

  // Парсим координаты
  const coordinates = [...cleanMessage.matchAll(coordPattern)]
    .map((m) => parseCoordinate(m[0]))
    .filter(Boolean)
    .filter(
      (c, i, s) =>
        s.findIndex(
          (x) => x.latitude === c.latitude && x.longitude === c.longitude
        ) === i
    );

  result.coordinates = coordinates.length > 0 ? coordinates : null;

  // Парсим ZONA
  const zoneMatch = cleanMessage.match(zonePattern);
  if (zoneMatch) {
    const zoneData = zoneMatch[1].trim();
    const zoneCoords = [...zoneData.matchAll(/\d{4,6}[NS]\d{5,7}[EW]/gi)]
      .map((m) => parseCoordinate(m[0]))
      .filter(Boolean);
    result.zone = zoneCoords.length > 0 ? zoneCoords : zoneData;
  }

  // Парсим поля
  const fields = [
    { key: "dof", pattern: /DOF\/(\d{6})/i },
    { key: "opr", pattern: /OPR\/(.+?)(?=\s+SID\/|\s+[A-Z]{3}\/|$)/i },
    { key: "reg", pattern: /REG\/([^\/\r\n]+?)(?=\/|$|\s+[A-Z]+\/)/i },
    { key: "typ", pattern: /TYP\/([^ \)]+)/i },
    { key: "sts", pattern: /STS\/([^ \)]+)/g },
    { key: "rmk", pattern: /RMK\/([\s\S]+?)(?=\s+SID\/|$)/i },
    { key: "eet", pattern: /EET\/([^\s\r\n]+)/i },
    { key: "sid", pattern: /SID\/(\d+)/i },
  ];

  fields.forEach(({ key, pattern }) => {
    const match = cleanMessage.match(pattern);
    if (match) {
      if (key === "sts") {
        result[key] = match.map((m) => m.replace("STS/", ""));
      } else if (key === "reg") {
        result[key] = match[1]
          .trim()
          .split(/[\s,]+/)
          .filter(Boolean)
          .map((r) => r.replace(/,$/, "").replace(/^REG/, "")) 
          .join(", ");
      } else {
        result[key] = match[1].trim();
      }
    }
  });

  result.phones = parsePhones(cleanMessage);

  return result;
}

module.exports = { parseSHR };
