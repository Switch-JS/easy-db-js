'use strict';

'use strict';

const generic = require("generic-pool");
const util = require("util");
const url = require("url");

const mongodb = require("mongodb");
const logger = require("log4js").getLogger('mongodb');


class Pool {
    constructor(uri) {
        this.options = url.parse(uri);
        this.rw = generic.createPool({
            create: function () {
                logger.debug("create mongodb connection %j", url.format(this.options));
                return util.promisify(mongodb.connect)(url.format(this.options));
            }.bind(this),
            destroy: function (c) {
                logger.debug('close mongo connection!');
                c.close();
            }
        }, {min: 1, max: 16});

        this.ro = undefined;
    }

    async acquire(job) {
        if (typeof job !== 'function') {
            return;
        }
        const client = await this.rw.acquire();
        const res = await job(client).catch(err => {
            logger.error("mongodb command failed by %s", err.message || err);
        });
        this.rw.release(client);
        return res;
    }
}

module.exports = Pool;
