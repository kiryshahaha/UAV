function parsePhones(message) {
  if (!message) return [];

  const phoneRegex =
    /(?:\+7|8)[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}/g;
  const rawPhones = message.match(phoneRegex) || [];

  const normalized = rawPhones
    .map((p) => p.replace(/[^\d]/g, "")) 
    .map((p) => {
      if (p.length === 11 && p.startsWith("8")) return "+7" + p.slice(1);
      if (p.length === 11 && p.startsWith("7")) return "+" + p;
      return null;
    })
    .filter(Boolean)
    .filter((p, index, self) => self.indexOf(p) === index); 

  return normalized;
}

module.exports = { parsePhones };
