  var redis = require('redis').createClient(6390, '172.16.33.203');
  var forEachSync = require('./forEachSync');
process.on('message', function(task) {
	start(task.array);
});
var start = function(array){
	array.forEachSync(function(item,index,cback){
		redis.del(item,function(e,r){
			console.log(item);
			cback();
		});
	},function(){
		console.log('pid='+process.pid+' delete over.');
	});
}