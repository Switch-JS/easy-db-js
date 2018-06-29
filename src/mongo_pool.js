'use strict';

'use strict';

const generic = require("generic-pool");
const util = require("util");
const url = require("url");

const mongodb = require("mongodb");

class Pool {
    constructor(uri) {
        this.options = url.parse(uri || 'redis://127.0.0.1:6379/0');

        this.rw = generic.createPool({
            create: function () {
                return util.promisify(mongodb.connect)(url.format(this.options), {autoReconnect: true});
            }.bind(this),
            destroy: function (c) {
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
        await job(client).catch(err => {
            console.error("mongodb exec command failed by %s", err.message || err);
        });
        this.rw.release(client);
    }
}

module.exports = Pool;
