var forEachSync = require('./forEachSync');
var request = require("request");
var fs = require("fs");

process.on('message', function(task) {
	dealData(task.array);
});
var random = function(url) {
		var numInput = new Number(10000);
		var numOutput = new Number(Math.random() * numInput).toFixed(0);
		url = url + numOutput;
		return url;
	}
var dealData = function(arrays) {
		async.forEach(arrays, function(item, cback) {
			var url = 'http://hqdata.compass.cn/test/zsw.py/data?cmd=[<id>,%20143,%200,%200,%204294967295,%204294967295,%20-2000]|';
			url = random(url);
			url = url.replace(/<id>/g, item);
			request(url, function(error, response, body) {
				if(body != '[]0'){
					fs.writeFileSync(__dirname + '/data/' + item + '', body);
					console.log('process.pid=' + process.pid + ',' + item);
				}				
				cback();
			});
		}, function() {
			console.log('it is over.');
			process.exit(0);
		});
		// arrays.forEachSync(function(item, index, cback) {
		// 	var url = 'http://hqdata.compass.cn/test/zsw.py/data?cmd=[<id>,%20143,%200,%200,%204294967295,%204294967295,%20-2000]|';
		// 	url = random(url);
		// 	url = url.replace(/<id>/g, item);
		// 	request(url, function(error, response, body) {
		// 		fs.writeFileSync(__dirname + '/data/' + item + '', body);
		// 		console.log('process.pid='+process.pid+','+item);
		// 		cback();
		// 	});
		// }, function() {
		// 	console.log('it is over.');
		// 	process.exit(0);
		// });
	}