'use strict';

const generic = require("generic-pool");
const util = require("util");
const url = require("url");

const redis = require("redis");


class Pool {
    constructor(uri) {
        console.log('create redis pool from %s', uri);
        this.options = url.parse(uri);

        this.rw = generic.createPool({
            create: function () {
                return util.promisify(this._connect2master.bind(this))();
            }.bind(this),
            destroy: function (c) {
                c.quit();
            }
        }, {min: 1, max: 16});

        this.ro = undefined;
    }

    _connect2master(cb) {
        console.log('connect 2 redis server %s', url.format(this.options));
        const c = redis.createClient(url.format(this.options));
        c.on('error', cb);
        c.on('ready', () => {
            if (c.server_info.role === 'master') {
                console.log('connected 2 redis server %s', url.format(this.options));
                return cb(null, c);
            }
            c.quit();

            this.options.host = c.server_info.master_host + ":" + c.server_info.master_port;
            this.options.hostname = c.server_info.master_host;
            this.options.port = c.server_info.master_port;
            this._connect2master(cb);
        });
    }

    async command() {
        const args = [].slice.call(arguments);
        const client = await this.rw.acquire();
        const cmd = args.shift();
        if (!client || !cmd || typeof client[cmd] !== 'function') {
            this.rw.release(client);
            console.error('redis command failed by %s', cmd);
            return;
        }
        console.log('<= %s  %j', cmd, args);
        let result = await util.promisify(client[cmd].bind(client))(...args).catch((err) => {
            console.error("redis command [%s => %j] failed by %s", cmd, args, err.message || err);
        });
        this.rw.release(client);
        if (typeof result === 'object') {
            for (let i in result) {
                try {
                    result[i] = JSON.parse(result[i]);
                } catch (_) {
                }
            }
        }
        if (typeof result === 'string') {
            try {
                result = JSON.parse(result);
            } catch (_) {
            }
        }
        console.log("=> %s %j", cmd, result);
        return result;
    };

    async hmset(key, obj) {
        const args = ['hmset', key];
        for (let i in obj) {
            args.push(i, typeof obj[i] === 'object' ? JSON.stringify(obj[i]) : obj[i].toString());
        }
        return await this.command(...args);
    }

    async hmget() {
        const args = [].slice.call(arguments);
        const key = args.shift();
        return await this.command(...['HMGET', key].concat(args));
    }

    async incrby(key, value, rate = 1, floor = 0) {
        const r = await this.command('INCRBY', key, value * rate);
        if (r < floor) {
            await this.command('INCRBY', key, -value * rate);
            return 0;
        }
        return r;
    }

    async hincrby(key, json, rate = 1, floor = undefined) {
        let changed = {};
        let values = {};
        let failed = false;
        for (let i in json) {
            let v = parseInt(json[i]);
            v = isNaN(v) ? 0 : Math.ceil(v * rate);
            const r = await this.command('HINCRBY', key, i, v);
            changed[i] = v;
            values[i] = r;

            const min = typeof floor === 'object' ? floor[i] : floor;
            if (typeof min !== 'undefined' && r < min) {
                failed = true;
                break;
            }
        }
        if (failed) {
            for (let i in changed) {
                await this.command('HINCRBY', key, i, 0 - changed[i]);
            }
            return undefined;
        }
        return {changed, values};
    }
}

module.exports = Pool;