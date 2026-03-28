/**
 * Groups stock by product code
 * and separates stock types
 */
function mapStock(stockResponse) {
  const grouped = {};

  for (const item of stockResponse) {
    const code = item.fullCode;

    if (!grouped[code]) {
      grouped[code] = {
        code,
        available: 0,
        reserved: 0,
        incoming: 0,
        lastUpdated: item.modifiedDate
      };
    }

    grouped[code].available += item.stock || 0;
    grouped[code].reserved += item.reservedStock || 0;
    grouped[code].incoming += item.incomingStock || 0;

    if (item.modifiedDate > grouped[code].lastUpdated) {
      grouped[code].lastUpdated = item.modifiedDate;
    }
  }

  return Object.values(grouped);
}

module.exports = {
  mapStock
};
