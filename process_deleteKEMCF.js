  // var redis = require('redis').createClient(6390, '172.16.33.203');
  var forEachSync = require('./forEachSync');
  var REDIS = null;
  process.on('message', function(task) {
    var redis = task.redis;
    REDIS = require('redis').createClient(redis.split(':')[1], redis.split(':')[0]);
  	start(task.array);
  });
  var start = function(array) {
  		array.forEachSync(function(item, index, cback) {
  			REDIS.del(item, function(e, r) {
  				if(e)
  					console.log(e);
  				else
						console.log(item);
  				cback();
  			});
  		}, function() {
  			console.log('pid=' + process.pid + ' delete over.');
  			process.exit(0);
  		});
  	}