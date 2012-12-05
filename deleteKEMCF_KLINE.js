var redis= require('redis').createClient(6390,'172.16.33.203');
redis.keys('KEMCF.MAIN.*.*',function(e,r){
	if(r){
		process_f(r);
	}
});

var process_deal = function(array) {
    var cp = require('child_process');
    var n = cp.fork(__dirname + '/process_deleteKEMCF.js');
    n.on('message', function(m) {});
    n.send({
      'array': array
    });
  };
var process_f = function(keys) {
    var length = keys.length;
    var threads = 16;
    var index = parseInt(length / threads);
    for(var i = 0; i < threads; i++) {
      var temp = keys.slice(index * i, index * (i + 1));
      process_deal(temp);
    }
  }