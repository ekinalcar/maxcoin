// request is a module that makes http calls easier
const request = require('request');
const redis = require('redis');

const MongoClient = require('mongodb').MongoClient;
const dsn = 'mongodb://localhost:37017/maxcoin';

// Generic function that fetches the closing bitcoin dates of the last month from a public API
function fetchFromAPI(callback) {
    // We are using fat arrow (=>) syntax here. This is a new way to create anonymous functions in Node
    // Please review the Node.js documentation if this looks unfamiliar to you
    request.get('https://api.coindesk.com/v1/bpi/historical/close.json', (err, raw, body) => {
        return callback(err, JSON.parse(body));
    });
}

function insertMongodb(collection, data) {
    const promisedInserts = [];

    Object.keys(data).forEach((key) => {
        promisedInserts.push(
            collection.insertOne({date: key, value: data[key]}),
        );
    });
    return Promise.all(promisedInserts);
}

MongoClient.connect(dsn, {useNewUrlParser: true, useUnifiedTopology: true}, (err, client) => {
    if (err) throw err;
    console.log('connected to the db');

    const db = client.db('maxcoin');
    const collection = db.collection('value');

    fetchFromAPI((err, data) => {
        if (err) throw err;
        insertMongodb(collection, data.bpi)
            .then((result) => {
                console.log(`Successfully inserted ${result.length} documents into mongodb`);
                const options = {'sort': [['value', 'desc']]};
                collection.findOne({}, options, (err, doc) => {
                    if (err) throw err;
                    console.log(`MongoDb: the one month max value is ${doc.value} and it was reached on ${doc.date}`);
                    client.close();
                });
            }).catch((err) => {
            console.log(err);
            process.exit();
        });
    });
});

function insertRedis(client, data, callback) {
    const values = ['values'];

    Object.keys(data).forEach((key) => {
        values.push(data[key]);
        values.push(key);
    });
    client.zadd(values, callback);
}

const redisClient = redis.createClient(7379);
redisClient.on('connect', () => {
    console.log('redis connected');

    fetchFromAPI((err, data) => {
        if (err) throw err;

        insertRedis(redisClient, data.bpi, (err, results) => {
            if (err) throw err;
            console.log(`Successfully inserted ${results} key/value pairs into redis`);

            redisClient.zrange('values', -1, -1, 'withscores', (err, result) => {
                if (err) throw err;
                console.log(`REDIS: the one month max value is ${result[1]} and it was reached on ${result[0]}`);
                redisClient.end();
            });
        });
    });
});




