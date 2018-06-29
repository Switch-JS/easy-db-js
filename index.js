'use strict';

const redis = require("./src/redis_pool");
const mongo = require("./src/mongo_pool");

module.exports = function (options) {
    if (options.redis_uri) {
        Object.defineProperty(global, '$redis', {value: new redis(options.redis_uri)});
    }
    if (options.mongo_uri) {
        Object.defineProperty(global, '$mongo', {value: new mongo(options.mongo_uri)});
    }
    return require('./src/collection');
};