function cleanDataframe(data, headers) {
  return data.filter(row => headers.some(header => {
    const value = row[header];
    return value !== null && value !== undefined && value !== '' && 
           (!Array.isArray(value) || value.length > 0) &&
           (typeof value !== 'object' || Object.keys(value).length > 0);
  }));
}

module.exports = { cleanDataframe };