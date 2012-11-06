var program = require("commander")
var tair = require('tair');

program.version('0.0.1')
    .usage('[options]')
    .option('-c, --conf <string>', 'config file')
    .parse(process.argv);

if(program.conf==undefined){
    console.log("must input config")
    process.exit(1)
}

var conf = require("./conf/"+program.conf);
process.conf = conf;

var log4js = require('log4js');
log4js.configure('./conf/log4js.json', {});

process.log4j = log4js;


var kestrelPool = poolModule.Pool({
    name: "memcache",
    create : function(callback){
        var client = new memcache.Client(process.conf.kestrel.port, process.conf.kestrel.ip);
        callback(null,client);
    }  ,
    destroy  : function(client) { client.close(); }, //当超时则释放连接
    max      : 10,   //最大连接数
    idleTimeoutMillis : 10,  //超时时间
    log : true
});
var configServer = [{host:conf.tair.ip, port: conf.tair.port}];
var tairPool = poolModule.Pool({
    name: "tair",
    create : function(callback){
        var tairClient = new tair('group_1', configServer, function (err){ if (err) { log.error(err); }});
        callback(null,tairClient);
    }  ,
    destroy  : function(client) { }, //当超时则释放连接
    max      : 10,   //最大连接数
    idleTimeoutMillis : 10,  //超时时间
    log : true
});

var push_sender = require("./push-sender").push_sender;

setInterval(push_sender(kestrelPool,tairPool),500)