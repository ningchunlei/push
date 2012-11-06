var buffer = require("buffer")
var utils = require("util")
var path = require("path")
var thrift_path = require.resolve("thrift")
var thrift = require("thrift")
var ttransport = require(path.resolve(path.dirname(thrift_path),"transport"));
TBinaryProtocol = require(path.resolve(path.dirname(thrift_path),"protocol")).TBinaryProtocol;

var ShareStruct_ttypes = require("./thrift/ShareStruct_Types")
var ErrorNo_ttypes = require("./thrift/ErrorNo_Types")
var Exception_ttypes = require("./thrift/Exception_Types")
var TimeLineService = require("./thrift/TimeLineIFace")
var IkaoDBService = require("./thrift/IKaoDBIFace")
var PRelationService = require("./thrift/PRelationIFace")

var log = process.log.getLogger("push-sender")

var timelineConnection = thrift.createConnection(process.conf.timeline.ip, process.conf.timeline.port)
var timeline = thrift.createClient(TimeLineService, timelineConnection);

var dbConnection = thrift.createConnection(process.conf.db.ip, process.conf.db.port)
var idb = thrift.createClient(IkaoDBService, dbConnection);

var prelationConnection = thrift.createConnection(process.conf.prelation.ip, process.conf.prelation.port)
var prelation = thrift.createClient(PRelationService, prelationConnection);

POST=false

exports.push_sender=function(kestrelPool,tairPool){
    if(POST) return
    POST = true;
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
            process.log.info(util.format("PostMsg:%s",msg))
            tairPool.borrow(function(err,tair){
                tair.set(msg.mid,mbuf,function(err,reply){
                    tairPool.release(tair)
                    switch (msg.type){
                        case ShareStruct_ttypes.MsgType.Post:
                            timeline.add(msg.uid,[ShareStruct_ttypes.TimeLineType.Askbox],msg.mid)
                            idb.addForTL(msg.uid,[ShareStruct_ttypes.TimeLineType.Askbox],msg.mid,function(){
                                idb.incrForUser(msg.uid,ShareStruct_ttypes.TimeLineType.Askbox,1)
                            })
                            idb.postMsg(msg)
                            prelation.getFans(msg.uid,0,-1,function(err,reply){
                                reply.forEach(function(fansid){
                                    timeline.add(fansid,[ShareStruct_ttypes.TimeLineType.Inbox],msg.mid)
                                })
                            })
                            timeline.add(ShareStruct_ttypes.ReserveUID.latest,[ShareStruct_ttypes.TimeLineType.Inbox],msg.mid)
                            timeline.add(msg.subject,[ShareStruct_ttypes.TimeLineType.Inbox],msg.mid)

                            switch(msg.grade){
                                case ShareStruct_ttypes.GradeCategory.Grade7:
                                    timeline.add(msg.subject+10,[ShareStruct_ttypes.TimeLineType.Inbox],msg.mid)
                                    break
                                case ShareStruct_ttypes.GradeCategory.Grade8:
                                    timeline.add(msg.subject+20,[ShareStruct_ttypes.TimeLineType.Inbox],msg.mid)
                                    break
                                case ShareStruct_ttypes.GradeCategory.Grade9:
                                    timeline.add(msg.subject+30,[ShareStruct_ttypes.TimeLineType.Inbox],msg.mid)
                                    break
                            }

                            break;
                        case ShareStruct_ttypes.MsgType.Comment:
                            timeline.add(msg.uid,[ShareStruct_ttypes.TimeLineType.AnswerBox],msg.mid)
                            idb.addForTL(uid,[ShareStruct_ttypes.TimeLineType.AnswerBox],msg.mid,function(){
                                idb.incrForUser(msg.uid,ShareStruct_ttypes.TimeLineType.AnswerBox,1)
                            })
                            idb.postMsg(msg)
                            prelation.getFans(msg.uid,0,-1,function(err,reply){
                                reply.forEach(function(fansid){
                                    timeline.add(fansid,[ShareStruct_ttypes.TimeLineType.Inbox],msg.mid)
                                })
                            })

                            prelation.getFans(msg.questionmid,0,-1,function(){
                                reply.forEach(function(fansid){
                                    timeline.add(fansid,[ShareStruct_ttypes.TimeLineType.NoticeBox],msg.mid)
                                })
                            })

                            break;
                    }
                });
            })
        });
    })
}