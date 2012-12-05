var ids = require('./allIds').ids;
var arrays = [];
var json = {};
for(var i in ids){
	json[ids[i]] = i.replace('HQ','');
	arrays.push(ids[i]);
}

var process_deal = function(array){
    var cp = require('child_process');
    var n = cp.fork(__dirname + '/fetchData.js');
    n.on('message', function (m) {});
    n.send({'array':array});
};
var process_f = function(keys){
    var length = keys.length;
    var index = parseInt(length/8);

    var temp = keys.slice(0,index);
    process_deal(temp);

    temp = keys.slice(index,index*2);
    process_deal(temp);

    temp = keys.slice(index*2,index*3);
    process_deal(temp);

    temp = keys.slice(index*3,index*4);
    process_deal(temp);

    temp = keys.slice(index*4,index*5);
    process_deal(temp);

    temp = keys.slice(index*5,index*6);
    process_deal(temp);

    temp = keys.slice(index*6,index*7);
    process_deal(temp);

    temp = keys.slice(index*7,length);
    process_deal(temp);

}
process_f(arrays);
