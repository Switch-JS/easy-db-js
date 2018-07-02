'use strict';

const easy = require("../index")({redis_uri: "redis://127.0.0.1:6379", mongo_uri: "mongodb://192.168.0.32:27017/dev"});
describe('redis collection test', () => {
    it('Hash Test', async () => {
        // const jasonbourn = new easy.Hash('jasonbourne');
        // await jasonbourn.$resolve();
        // if (jasonbourn.$new) {
        //     await jasonbourn.$mset({name: 'jasonbourne', age: 32});
        // }
        // console.log(jasonbourn);
        // const mesis = new easy.Hash('mesis');
        // await mesis.$resolve();
        // if (mesis.$new) {
        //     await mesis.$mset({name: 'mesis', age: 32, friends: [jasonbourn]});
        // }
        // console.log(mesis);
        //
        // const myself = new easy.Hash('goofo');
        // await myself.$resolve();
        // if (myself.$new) {
        //     await myself.$mset({name: 'goofo', age: 34});
        //     await myself.$set('friends', [jasonbourn, {type: '__hash__', id: 'mesis'}]);
        // }
        // await myself.$incrby({age: 1, money: 10});
        // await myself.$incrby({money: 50}, -1, {money: 0});
        // console.log(myself);

        const info = await $mongo.acquire(async (client) => {
            console.log(client.collection);
            const c = client.collection('cgbuser');
            return await c.findOne();
        });

        console.log(info);
    });
});