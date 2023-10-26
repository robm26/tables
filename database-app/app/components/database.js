const mysql = require('mysql');
const util  = require('util');

// copy the following into file mysql-credentials.json and customize for your host
// {
//     host     : 'rmeg2eeeeeesqx.cwbe83eeeewr.us-east-1.rds.amazonaws.com',
//     user     : 'user',
//     password : 'TopSecretPassword',
//     database : 'customer_activity'
// };

// import config from './mysql-credentials.json';
const config = require('./mysql-credentials.json');
//
// export const database = config.database;
// export const host = config.host;

const connection = mysql.createConnection(config);
const query = util.promisify(connection.query).bind(connection);

connection.connect(); // keep connection open and reuse

// export const getInfo = async (key) => {
//     return(config[key]);
// }

const runSql = async (sql) => {

    let result;
    let latency = 0;

    try {
        // connection.connect();
        const options = {sql: sql};

        const timeStart = new Date();
        result = await query(options);
        const timeEnd = new Date();
        const timeDiff = timeEnd - timeStart;
        latency = timeDiff;

    }  catch (error) {

        console.log(JSON.stringify(error, null, 2));
        result = {
            error:error
        };
    }

    // connection.end();

    return({result:result, latency:latency});
}

export {runSql};

// module.exports = {runSql};

