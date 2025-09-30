function parseCoordinate(coord) {
  if (!coord || typeof coord !== "string") return null;

  coord = coord.replace(/\s+/g, "").toUpperCase();


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

module.exports = { parseCoordinate };
