const axios = require('axios');
const sql = require('mssql');
const dotenv = require('dotenv');
const cron = require('node-cron');

dotenv.config();

const dbConfig = {
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_DATABASE,
    server: process.env.DB_SERVER,
    options: {
        encrypt: true,
        trustServerCertificate: true
    }
};

//last 5 min data 
async function fetchDataFromAPI() {
    const now = new Date();
    const fromDate = new Date(now - 5 * 60 * 1000); 
    
    const params = new URLSearchParams({
        plant: 'B051',
        createdDateFrom: fromDate.toISOString(),
        createdDateTo: now.toISOString()
    });

    try {
        const response = await axios.get(`https://${process.env.API_URL}?${params}`);
        return response.data.result;
    } catch (error) {
        console.error('Error fetching data:', error);
        return null;
    }
}

// Function to check if record already exists
async function checkIfRecordExists(pool, item) {
    const result = await pool.request()
        .input('prodOrder', sql.VarChar, item.prodOrder)
        .input('createdDateTime', sql.DateTime, new Date(item.createdDateTime))
        .query(`
            SELECT COUNT(*) as count 
            FROM SubAssemblyData 
            WHERE prodOrder = @prodOrder 
            AND createdDateTime = @createdDateTime
        `);
    return result.recordset[0].count > 0;
}

// Modified insert function to avoid duplicates
async function insertDataToSQL(data) {
    try {
        const pool = await sql.connect(dbConfig);
        let insertedCount = 0;
        
        for (const item of data) {
            // Check if record already exists
            const exists = await checkIfRecordExists(pool, item);
            
            if (!exists) {
                await pool.request()
                    .input('plant', sql.VarChar, item.plant)
                    .input('factory', sql.VarChar, item.factory)
                    .input('workcenter', sql.VarChar, item.workcenter)
                    .input('mainWorkcenter', sql.VarChar, item.mainWorkcenter)
                    .input('subOperationId', sql.VarChar, item.subOperationId)
                    .input('operation', sql.VarChar, item.operation)
                    .input('shiftDate', sql.DateTime, new Date(item.shiftDate))
                    .input('shiftId', sql.VarChar, item.shiftId)
                    .input('timeSlot', sql.VarChar, item.timeSlot)
                    .input('prodOrder', sql.VarChar, item.prodOrder)
                    .input('docket', sql.VarChar, item.docket)
                    .input('size', sql.VarChar, item.size)
                    .input('qty', sql.Int, item.qty)
                    .input('smv', sql.Decimal, item.smv)
                    .input('oraclePn', sql.VarChar, item.oraclePn)
                    .input('qrcode', sql.VarChar, item.qrcode)
                    .input('createdDateTime', sql.DateTime, new Date(item.createdDateTime))
                    .query(`
                        INSERT INTO SubAssemblyData 
                        (plant, factory, workcenter, mainWorkcenter, subOperationId, 
                         operation, shiftDate, shiftId, timeSlot, prodOrder, docket, 
                         size, qty, smv, oraclePn, qrcode, createdDateTime)
                        VALUES 
                        (@plant, @factory, @workcenter, @mainWorkcenter, @subOperationId,
                         @operation, @shiftDate, @shiftId, @timeSlot, @prodOrder, @docket,
                         @size, @qty, @smv, @oraclePn, @qrcode, @createdDateTime)
                    `);
                insertedCount++;
            }
        }

        await sql.close();
        console.log(`Found ${data.length} records, Inserted ${insertedCount} new records`);
    } catch (error) {
        console.error('Error inserting data:', error);
    }
}

async function main() {
    const data = await fetchDataFromAPI();
    if (data && data.length > 0) {
        await insertDataToSQL(data);
    } else {
        console.log('No new data found in the last 5 minutes');
    }
}

// Run every 5 minutes
cron.schedule('*/5 * * * *', () => {
    console.log('Running data fetch and insert task...', new Date().toISOString());
    main();
});

// Initial run
main();