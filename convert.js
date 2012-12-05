var dive = require('dive');
var forEachSync = require('./forEachSync');
var fs = require("fs");
var fileList = [];
dive(__dirname+'/data', {
  recursive: false,
  all: false,
  directories: false,
  files: true
}, function(err, file) {
  if(err) {
    console.log(err);
  } else {
    fileList.push(file);
  }
}, function() {
  //console.log(fileList);
  process_f(fileList);
});

var process_deal = function(array) {
    var cp = require('child_process');
    var n = cp.fork(__dirname + '/convertData.js');
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
