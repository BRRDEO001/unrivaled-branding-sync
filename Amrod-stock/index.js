const { getAmrodStock } = require('./stockService');

(async () => {
  try {
    const token = process.env.AMROD_TOKEN;

    if (!token) {
      throw new Error('AMROD_TOKEN is missing');
    }

    const stock = await getAmrodStock(token);

    console.log('📦 Amrod stock fetched');
    console.log(JSON.stringify(stock, null, 2));
  } catch (err) {
    console.error('🔥 Stock sync failed:', err.message);
    process.exit(1);
  }
})();
