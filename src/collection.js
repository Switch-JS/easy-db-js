'use strict';
const mongodb = require("mongodb");
const logger = require("log4js").getLogger('easy-db');

class Collection {
    constructor(name) {
        this.name = name;
    }
}

class Hash {
    constructor(id) {

        Object.defineProperty(this, '$id', {value: id || new mongodb.ObjectID().toString()});
        Object.defineProperty(this, '$ref', {value: {type: '__hash__', id: this.$id}});
        Object.defineProperty(this, '$resolved', {value: false, writable: true});
        Object.defineProperty(this, '$new', {value: true, writable: true});
    }

    static async $resolve(id) {
        const h = new Hash(id);
        if (!!id) {
            await h.$resolve().catch(err => {
                logger.error('Collection object resolve failed %s', err.message || err);
            });
        }
        return h;
    }

    async $resolve() {
        if (this.$resolved) {
            logger.error("hash object $resolved %s", this.$resolved);
            return;
        }
        const data = await $redis.command('HGETALL', this.$id);
        if (!!data) {
            this.$new = false;

            for (let i in data) {
                if (typeof data[i] === 'object') {
                    if (data[i].type === '__hash__') {
                        this[i] = await Hash.$resolve(data[i].id);
                        continue;
                    }
                    if (data[i].type === '__array__') {
                        this[i] = await LArray.$resolve(data[i].id);
                        continue;
                    }
                }
                this[i] = data[i];
            }
        }
        this.$resolved = true;
        return this;
    }

    async $incrby(incrs, rate = 1, floor = {}) {

        const changed = {};
        let success = true;
        for (let i in incrs) {
            changed[i] = rate * parseInt(incrs[i]);
            this[i] = await $redis.command('HINCRBY', this.$id, i, changed[i]);
            if (this[i] < floor[i]) {
                success = false;
                break;
            }
        }
        if (!success) {
            await this.$incrby(changed, -1);
            return;
        }

        return changed;
    }

    async $mset(object) {
        if (!this.$resolved) {
            await this.$resolve();
        }
        const args = ['HMSET', this.$id];
        for (let i in object) {
            let v = object[i];
            if (typeof v === 'object') {
                if (!(v instanceof Hash) && !(v instanceof LArray)) {
                    if (v instanceof Array) {
                        const t = new LArray();
                        v = await t.$push(...v);
                    } else {
                        if (v === null) {
                            continue;
                        }
                        const t = new Hash();
                        v = await t.$mset(v);
                    }
                }
            }
            if (v instanceof Hash || v instanceof LArray) {
                await v.$referenceof(this.$id);
                args.push(i, JSON.stringify(v.$ref));
            } else {
                args.push(i, v);
            }
            this[i] = v;
        }
        if (args.length > 2) {
            await $redis.command(...args);
        }
        return this;
    }

    async $set(k, v) {
        if (!this.$resolved) {
            await this.$resolve();
        }

        if (typeof v === 'object') {
            if (!(v instanceof Hash) && !(v instanceof LArray)) {
                if (v instanceof Array) {
                    const t = new LArray();
                    v = await t.$push(...v);
                } else {
                    if (v === null) {
                        return this;
                    }
                    const t = new Hash();
                    v = await t.$mset(v);
                }
            }
        }
        this[k] = v;

        if (v instanceof Hash || v instanceof LArray) {
            await $redis.command('HSET', this.$id, k, JSON.stringify(v.$ref));
        } else {
            await $redis.command('HSET', this.$id, k, v);
        }
        return this;
    }

    async $referenceof(id) {
        await $redis.command('LPUSH', `reference.${this.$id}`, id);
        return this;
    }

    async $remove(k) {
        if (!this.$resolved) {
            await this.$resolve();
        }
        if (this[k] && (this[k] instanceof Hash || this[k] instanceof LArray)) {
            await this[k].$release(this.$id);
        }
        delete this[k];
        return this;
    }

    async $release(id) {
        if (!this.$resolved) {
            await this.$resolve();
        }

        if (id) {
            await $redis.command('LREM', `reference.${this.$id}`, 0, id);
        }

        for (let i in this) {
            await this.$remove(i);
        }

        const ref = await $redis.command('LLEN', `reference.${this.$id}`);
        if (ref === 0) {
            await $redis.command('DEL', this.$id);
        }
    }
}

class LArray extends Array {
    constructor(id) {
        super();

        Object.defineProperty(this, '$id', {value: id || new mongodb.ObjectID().toString()});
        Object.defineProperty(this, '$ref', {value: {type: '__array__', id: this.$id}});
        Object.defineProperty(this, '$resolved', {value: false, writable: true});
        Object.defineProperty(this, '$new', {value: true, writable: true});
    }

    static async $resolve(id) {
        const l = new LArray(id);
        await l.$resolve().catch(err => {
            logger.error('Collection object resolve failed %s', err.message || err);
        });
        return l;
    }

    async $resolve() {
        if (this.$resolved) {
            return this;
        }
        const data = await $redis.command('LRANGE', this.$id, 0, -1);
        if (data) {
            this.$new = false;

            for (let i in data) {
                if (typeof data[i] === 'object') {
                    if (data[i].type === '__hash__') {
                        this.push(await Hash.$resolve(data[i].id));
                        continue;
                    }
                    if (data[i].type === '__array__') {
                        this.push(await LArray.$resolve(data[i].id));
                        continue;
                    }
                }
                this.push(data[i]);
            }
        }
        this.$resolved = true;
        return this;
    }

    async $push() {
        if (!this.$resolved) {
            await this.$resolve();
        }
        const args = [].slice.call(arguments);

        const cmds = ['LPUSH', this.$id];
        for (let i in args) {
            let obj = args[i];
            switch (obj.type) {
                case '__hash__':
                    obj = await Hash.$resolve(obj.id);
                    break;
                case '__array__':
                    obj = await LArray.$resolve(obj.id);
                    break;
                default:
                    if (typeof obj === 'object') {
                        if (obj instanceof Array) {
                            const a = new LArray();
                            await a.$push(...obj);
                            obj = a;
                        } else {
                            const b = new Hash();
                            await b.$mset(obj);
                            obj = b;
                        }
                    }
                    break;
            }

            if (obj instanceof Hash || obj instanceof LArray) {
                this.push(await obj.$referenceof(this.$id));
                cmds.push(JSON.stringify(obj.$ref));
                continue;
            }
            this.push(obj);
        }
        if (cmds.length > 2) {
            await $redis.command(...cmds);
        }
        return this;
    }

    async $set(index, value) {
        if (!this.$resolved) {
            await this.$resolve();
        }
        if (typeof this[index] === 'undefined') {
            logger.warn('can`t set %d to %j by out of range!', index, value);
            return;
        }
        if (this[index] instanceof Hash || this[index] instanceof LArray) {
            await this[index].$release(this.$id);
        }
        if (typeof value === 'object') {
            if (value instanceof Array) {
                if (!(value instanceof LArray)) {
                    const a = new LArray();
                    await a.$push(...value);
                    value = a;
                }
            } else {
                if (!(value instanceof Hash)) {
                    const o = new Hash();
                    await o.$mset(value);
                    value = o;
                }
            }
        }

        if (value instanceof Hash || value instanceof LArray) {
            await value.$referenceof(this.$id);
            await $redis.command('LSET', this.$id, index, value.$ref);
        } else {
            await $redis.command('LSET', this.$id, index, value);
        }
        return 'ok';
    }

    async $shift() {
        if (!this.$resolved) {
            await this.$resolve();
        }

        const head = this.shift();
        if (head && (head instanceof Hash || head instanceof LArray)) {
            await head.$release(this.$id);
        }
        await $redis.command('LPOP', this.$id);
        return head;
    }

    async $release(id) {
        if (!this.$resolved) {
            await this.$resolve();
        }
        if (id) {
            await $redis.command('LREM', `reference.${this.$id}`, 0, id);
        }
        for (let i in this) {
            if (this[i] instanceof Hash || this[i] instanceof LArray) {
                await this[i].$release(this.$id);
            }
        }

        const ref = await $redis.command('LLEN', `reference.${this.$id}`);
        if (ref === 0) {
            await $redis.command('DEL', this.$id);
        }
        return this;
    }

    async $referenceof(id) {
        await $redis.command('LPUSH', `reference.${this.$id}`, id);
        return this;
    }
}


module.exports = {
    Hash, LArray, Collection
};