var buffer = require("buffer")
var path = require("path")
var thrift_path = require.resolve("thrift")
var thrift = require("thrift")
var ttransport = require(path.resolve(path.dirname(thrift_path),"transport"));
TBinaryProtocol = require(path.resolve(path.dirname(thrift_path),"protocol")).TBinaryProtocol;

var ShareStruct_ttypes = require("./thrift/ShareStruct_Types")
var ErrorNo_ttypes = require("./thrift/ErrorNo_Types")
var Exception_ttypes = require("./thrift/Exception_Types")
var TimeLineService = require("./thrift/TimeLineIFace")

var log = process.log.getLogger("push-sender")

var connection = thrift.createConnection(process.conf.timeline.ip, process.conf.timeline.port)
var thriftclient = thrift.createClient(TimeLineService, connection);

POST=false

exports.push_sender=function(kestrelPool,tair){
    if(POST) return
    kestrelPool.borrow(function(err,kestrel){
        kestrel.get("PostMsg",function(err,reply){
            kestrelPool.release(kestrel);
            POST  = false;
            var msg;
            var rec = ttransport.TBufferedTransport.receiver(function(data){
                var input = new TBinaryProtocol(data);
                msg = new ShareStruct_ttypes.Msg();
                msg.read(input)
                input.readMessageEnd();
            })
            rec(mbuf);
            tair.set(msg.mid,mbuf,function(err,reply){
                switch (msg.type){
                    case ShareStruct_ttypes.MsgType.Post:
                        thriftclient.add(msg.uid,[ShareStruct_ttypes.TimeLineType.Inbox],msg.mid)
                        break;
                    case ShareStruct_ttypes.MsgType.Comment:
                        break;
                }
            });
        });
    })
}