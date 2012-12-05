var forEachSync = require('./forEachSync');
var REDIS = null;
var COUNT = 0;
process.on('message', function(task) {
	var redis = task.redis;
	COUNT = task.count;
	REDIS = require('redis').createClient(redis.split(':')[1], redis.split(':')[0]);
	start(task.array);
});
var start = function(array) {
		array.forEachSync(function(item, index, cback) {
			REDIS.ltrim(item,COUNT,-1, function(e, r) {
				if(e) console.log(e);
				else console.log(item);
				cback();
			});
		}, function() {
			console.log('pid=' + process.pid + ' delete over.');
			process.exit(0);
		});
	}