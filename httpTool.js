  var request = require('request');
  var PushModel = require('./pushModel').PushModel;
  var http = require('http');
  var url = require('url');
  var error = {};
  process.setMaxListeners(0);
  var post = function(url,_sdata,cback,count,_timeout){
    var _timeOut = 100000;
	  var option = {
      url: url,
      json: _sdata,
      timeout: _timeOut,
      pool: {
        maxSockets: 2000
      }
    };
    var reqData = function(count,sdata){
      request.post(option, function(e, r, body) {
        if (e) {
          if(count == 0){
            //log.error('method=post,'+e+',url='+url);
            if(cback){
              cback(null);
            }
          }else{
            count = count - 1;
            reqData(count,sdata);
          }
        } else {
          if(cback){
            cback(body);
          }        
        }  
      });
    }
    reqData(count,_sdata);
  }
  //推送数据
  var push = function(sdata,cback) {
    var _url = 'http://127.0.0.1:10050/ReceiverCapital';
    post(_url,sdata,cback,0,100000);
  }


  exports.pushData = function(prefix,valType,equities,del){
    var pushModel = new PushModel();
    pushModel.prefix = prefix;
    pushModel.valType = valType;
    pushModel.equities = equities;
    pushModel.timestamp = new Date().format('yyyyMMdd');
    pushModel.del = del;
    push(pushModel);
  }

