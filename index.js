// var redis = require('redis').createClient(6390, '172.16.33.203');
var program = require('commander');
var dive = require('dive');
var fs = require("fs");
var DATA_PATH = null;
var REDIS = null;
var KEY = null;
var TYPE = null;
var COUNT = 0
var THREADS = 8;
var fileList = [];

var loadData = function() {
    dive(DATA_PATH, {
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
      process_load_f(fileList);
    });
  };

var process_load_deal = function(array) {
    var cp = require('child_process');
    var n = cp.fork(__dirname + '/convertData.js');
    n.on('message', function(m) {});
    n.send({
      'array': array,
      'redis': REDIS
    });
  };
var process_load_f = function(keys) {
    var length = keys.length;
    var threads = length < THREADS ? length : THREADS;
    var index = parseInt(length / threads);
    for(var i = 0; i < threads; i++) {
      var temp = keys.slice(index * i, index * (i + 1));
      process_load_deal(temp);
    }
  }
var deleteData = function() {
    var redis = require('redis').createClient(REDIS.split(':')[1], REDIS.split(':')[0]);
    redis.keys(KEY, function(e, r) {
      if(r && r.length > 0) {
        process_f(r);
      } else if(e) {
        console.log(e);
      } else {
        console.log('there is null in redis.');
      }
    });
  };


var process_deal = function(array) {
    var cp = require('child_process');
    var n = cp.fork(__dirname + '/process_deleteKEMCF.js');
    n.on('message', function(m) {});
    n.send({
      'array': array,
      'redis': REDIS
    });
  };
var process_f = function(keys) {
    var length = keys.length;
    var threads = length < THREADS ? length : THREADS;
    var index = parseInt(length / threads);
    for(var i = 0; i < threads; i++) {
      var temp = keys.slice(index * i, index * (i + 1));
      process_deal(temp);
    }
  }


String.prototype.trim = function() {
  return this.replace(/(^\s*)(\s*$)/g, '');
}
var getRedis = function(redis) {
    REDIS = redis;
  };
var getKey = function(key) {
    KEY = key;
  };
var getThreads = function(threads) {
    THREADS = threads;
  };
var getType = function(type) {
    TYPE = type.trim();
    if(TYPE == 'del') {
      if(KEY == null || REDIS == null) {
        console.log('the key is null or the redisIP is null,please check the help by [node index -h]');
      } else {
        deleteData();
      }
    } else if(TYPE == 'load') {
      if(REDIS == null || DATA_PATH == null) {
        console.log('the redisIp is null or DATA_PATH is null.please check the help by [node index -h]');
      } else {
        loadData();
      }
    }
  };
var process_trim_deal = function(array){
  var cp = require('child_process');
    var n = cp.fork(__dirname + '/process_trim_list.js');
    n.on('message', function(m) {});
    n.send({
      'array': array,
      'redis': REDIS,
      'count':COUNT
    });
};
var process_trim_f = function(keys) {
    var length = keys.length;
    var threads = length < THREADS ? length : THREADS;
    var index = parseInt(length / threads);
    for(var i = 0; i < threads; i++) {
      var temp = keys.slice(index * i, index * (i + 1));
      process_trim_deal(temp);
    }
  }
var trimList = function() {
  var redis = require('redis').createClient(REDIS.split(':')[1], REDIS.split(':')[0]);
  redis.keys(KEY, function(e, r) {
    if(r && r.length > 0) {
      process_trim_f(r);
    } else if(e) {
      console.log(e);
    } else {
      console.log('there is null in redis.');
    }
  });
};

var getPath = function(path) {
    DATA_PATH = path;
  };
var getCount = function(count) {
    COUNT = parseInt(count) * -1;
    if(REDIS == null || COUNT == 0 || KEY == null) {
      console.log('the redisIp is null or the c parameter is missing or the k parameter is missing.');
    } else {
      trimList();
    }
  }
program.version('0.1').usage('[options] ').option('-k, --key <n>', '要删除的key', getKey).option('-t, --type <n>', '操作类型是删除(del)还是加载历史数据(load)', getType).option('-r --redis <n>', 'redis地址', getRedis).option('-l --thread length <n>', '启用的线程数默认是8个', getThreads).option('-p --data path <n>', '资金流向历史数据的备份文件', getPath).option('-c --trim count <n>', '从尾开始按需截取list类型数据', getCount);

program.on('--help', function() {
  console.log('  Examples:');
  console.log('');
  console.log('    1:删除数据(支持模糊删除)');
  console.log('      $ node index.js -r 172.16.33.203:6390 -k MLINE.SH600000 -l 10 -t del ');
  console.log('');
  console.log('    2:加载资金流向K线历史数据');
  console.log('      $ node index.js -r 172.16.33.203:6390 -p /home/cool/data -l 10 -t load ');
  console.log('');
  console.log('    3:从尾开始按需截取list类型数据(支持模糊截取)');
  console.log('      $ node index.js -r 172.16.33.203:6390 -k KEMCF.MAIN.SH* -c 120 ');
  console.log('');
  console.log('    [注意] 操作类型-t一定要放在最后');
});

program.parse(process.argv);