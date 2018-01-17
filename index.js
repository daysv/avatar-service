const path = require('path');
const querystring = require('querystring');
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const Koa = require('koa');
const send = require('koa-send');
const conditional = require('koa-conditional-get');
const etag = require('koa-etag');
const mysql = require('mysql');
const request = require('request');


const config = require('./config');

const app = new Koa();

const pool = mysql.createPool(config.mysql);

const queryFid = (avatarId, ownerId) => {
    if (!ownerId && !avatarId) return Promise.reject('need ownerId or avatarId');
    return new Promise((resolve, reject) => {
        pool.getConnection(function (err, connection) {
            if (err) {
                if (connection) {
                    connection.release();
                }
                return reject(err)
            }
            const condition = ownerId ? `owner_id= '${ownerId}'` : `avatar_id= '${avatarId}'`;
            connection.query(`select * from pub_avatar where ${condition} order by create_date desc limit 1`, function (error, results) {
                connection.release();
                if (error) return reject(error);
                resolve(results[0] ? results[0]['file_id'] : '');
            });
        });
    });
};

app.use(async (ctx, next) => {
    try {
        const fid = await queryFid(ctx.query.avatarId, ctx.query.ownerId);
        if (fid) {
            const query = querystring.stringify({q: ctx.query.q, size: ctx.query.size});
            await new Promise((resolve, reject) => {
                request({
                    method: 'GET',
                    url: `${config.target}${fid}?${query}`,
                    headers: ctx.request.header,
                }).on('response', (res) => {
                    if (!/^(2|3)/.test(res.statusCode)) {
                        reject();
                    } else {
                        ctx.set(res.headers);
                        ctx.status = res.statusCode;
                        res.pipe(ctx.res);
                        resolve();
                    }
                }).on('error', reject)
            });
        } else {
            await next();
        }
    } catch (e) {
        await next();
    }
});

app.use(conditional());
app.use(etag());

app.use(async (ctx) => {
    const type = ctx.query.type || '';
    const url = `${path.join('static', type)}.png`;
    try {
        await send(ctx, url);
    } catch (err) {
        ctx.status = 404;
    }
});

if (cluster.isMaster) {
    for (var i = 0; i < numCPUs / 2; i++) {
        cluster.fork();
    }
} else if (cluster.isWorker) {
    app.listen(config.port);
}
