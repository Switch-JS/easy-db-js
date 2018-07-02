'use strict';

'use strict';

const generic = require("generic-pool");
const util = require("util");
const url = require("url");

const mongodb = require("mongodb");

class Pool {
    constructor(uri) {
        this.options = url.parse(uri);
        this.rw = generic.createPool({
            create: function () {
                console.log("create mongodb connection %j", url.format(this.options));
                return util.promisify(mongodb.connect)(url.format(this.options));
            }.bind(this),
            destroy: function (c) {
                console.log('close mongo connection!');
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
            console.error("mongodb command failed by %s", err.message || err);
        });
        this.rw.release(client);
        return res;
    }
}

module.exports = Pool;
