const { parseCoordinate } = require("./parseCoordinate");

function parseDEP(message) {
  if (!message) return {};

  const cleanMessage = message.replace(/\r\n/g, ' '); // Remove newlines
  const result = {};
  const fields = [
    { key: "sid", pattern: /-SID\s+(\d+)/i, transform: Number },
    { key: "add", pattern: /-ADD\s+(\d{6})/i, transform: Number },
    {
      key: "atd",
      pattern: /-ATD\s+(\d{4})/i,
      transform: (v) => `${v.slice(0, 2)}:${v.slice(2)}:00`,
    },
    { key: "adep", pattern: /-ADEP\s+([^\s]+)/i },
    {
      key: "adepz",
      pattern: /-ADEPZ\s+.*?(\d{4,6}[NS]\d{5,7}[EW])/i, // Match coordinate after any text
      transform: parseCoordinate,
    },
    { key: "pap", pattern: /-PAP\s+(\d+)/i, transform: Number },
    { key: "reg", pattern: /-REG\s+([^\s)]+)/i },
  ];

  for (const { key, pattern, transform } of fields) {
    const match = cleanMessage.match(pattern);
    if (match) {
      result[key] = transform ? transform(match[1]) : match[1];
    }
  }

  return result;
}

module.exports = { parseDEP };