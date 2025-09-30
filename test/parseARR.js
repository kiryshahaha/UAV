const { parseCoordinate } = require('./parseCoordinate');

function parseARR(message) {
  if (!message) return {};

  const cleanMessage = message.replace(/\r\n/g, ' ');
  const result = {};
  const fields = [
    { key: 'sid', pattern: /-SID\s+(\d+)/i },
    { key: 'ada', pattern: /-ADA\s+(\d{6})/i },
    { key: 'ata', pattern: /-ATA\s+(\d{4})/i, transform: v => `${v.slice(0, 2)}:${v.slice(2)}:00` },
    { key: 'adarr', pattern: /-ADARR\s+([^\s]+)/i },
    {
      key: 'adarrz',
      pattern: /-ADARRZ\s+.*?(\d{4,6}[NS]\d{5,7}[EW])/i, // Match coordinate after any text
      transform: parseCoordinate,
    },
    { key: 'pap', pattern: /-PAP\s+(\d+)/i },
    { key: 'reg', pattern: /-REG\s+([^\s)]+)/i },
  ];

  fields.forEach(({ key, pattern, transform }) => {
    const match = cleanMessage.match(pattern);
    if (match) {
      result[key] = transform ? transform(match[1]) : match[1];
    }
  });

  return result;
}

module.exports = { parseARR };