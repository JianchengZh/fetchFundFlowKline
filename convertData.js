var forEachSync = require('./forEachSync');
var dateFormat = require('./dateFormat');
var async = require('async');
var _ = require('underscore');
var http = require('./httpTool');
var fs = require("fs");
var ids = require('./allIds').ids;
// var redis = require('redis').createClient(6390, '172.16.33.203');
var REDIS = null;
var json = {};
var KUNIT = null;
for(var i in ids) {
	json[ids[i]] = i.replace('HQ', '');
}

process.on('message', function(task) {
	var redis = task.redis;
	REDIS = require('redis').createClient(redis.split(':')[1], redis.split(':')[0]);
	start(task.array);
});

/**
 * K线实体
 */
var KLineUnit = function() {
		var self = this;
		this.time; // 时间
		this.volum = 0; // 成交量
		this.amount = 0; // 成交额
		this.toArray = function() {
			return [self.time, self.volum, self.amount];
		};
		this.getTime = function() {
			return self.time;
		};
		this.setTime = function(time) {
			self.time = time;
		};
		this.setVolum = function(volum) {
			self.volum = parseFloat(self.volum) + parseFloat(volum);
		};
		this.getVolum = function() {
			return self.volum;
		};
		this.setAmount = function(amount) {
			self.amount = parseFloat(self.amount) + parseFloat(amount);
		};
		this.getAmount = function() {
			return self.amount;
		};
	};
/**
 * 克隆对象
 */
var clone = function(object) {
		if(typeof object != 'object') return object;
		if(object == null || object == undefined) return object;
		var newObject = new Object();
		for(var i in object) {
			newObject[i] = clone(object[i]);
		}
		return newObject;
	};
var setKLineUnit = function(item, unit) {
		var kUnit = null;
		if(_.isArray(item)) {
			kUnit = new KLineUnit();
			kUnit.setTime(item[0]);
			kUnit.setVolum(item[1]);
			kUnit.setAmount(item[2]);
		} else {
			kUnit = clone(item);
			kUnit.setTime(unit.getTime());
			kUnit.setVolum(unit.getVolum());
			kUnit.setAmount(unit.getAmount());
		}
		return kUnit;
	};
/**
 * 年K
 */
var dealYearKline = function(results, key) {
		console.log('pid:' + process.pid + ',' + key);
		var tempRes = {};
		var TIME = '';
		var START = '';
		var count = 0;
		async.forEach(results, function(item, cb) {
			var kUnit = setKLineUnit(item);
			var ymd = item[0].toString().substring(0, 8); // 年月日
			var year = item[0].toString().substring(0, 4);
			try {
				if(parseFloat(ymd) < parseFloat(year + '' + '1231')) {
					var _date = new Date(year + '-12-31');
					var _weekDay = _date.getDay();
					while(_weekDay == 6 || _weekDay == 0) { // 判断不在周末
						var _tempDate = _date.minusDays(1).format('yyyy-MM-dd');
						_date = new Date(_tempDate);
						_weekDay = _date.getDay();
					}
					TIME = _date.addDays(0).format('yyyyMMdd');
				}
			} catch(e) {
				console.log(e.stack);
			}
			kUnit.setTime(TIME);
			if(KUNIT != null) {
				if(kUnit.getTime() == KUNIT.getTime()) {
					var unit = setKLineUnit(KUNIT, kUnit);
					tempRes[kUnit.getTime()] = clone(unit).toArray();
					KUNIT = clone(unit);
					cb();
				} else {
					tempRes[kUnit.getTime()] = clone(kUnit).toArray();
					KUNIT = clone(kUnit);
					cb();
				}
			} else {
				tempRes[kUnit.getTime()] = clone(kUnit).toArray();
				KUNIT = clone(kUnit);
				cb();
			}
		}, function() {
			var worker = new Worker(1, key);
			worker.setTask(tempRes);
		});
	};

/**
 * 半年K
 */
var dealHalfYKline = function(results, key) {
		console.log('pid:' + process.pid + ',' + key);
		var tempRes = {};
		var TIME = '';
		var count = 0;
		async.forEach(results, function(item, cb) {
			var kUnit = setKLineUnit(item);
			var ym = item[0].toString().substring(0, 6); // 年月
			var month = item[0].toString().substring(4, 6); // 月
			var year = item[0].toString().substring(0, 4); // 年
			var compareSummerYM = year + '' + '07';
			var compareWinterYM = year + '' + '01';
			var compareWinterYMEnd = parseFloat(year + 1) + '' + '01';
			if(parseFloat(ym) >= parseFloat(compareWinterYM) && parseFloat(ym) < parseFloat(compareSummerYM)) {
				var _date = new Date(year + '-06-30');
				var _weekDay = _date.getDay();
				while(_weekDay == 6 || _weekDay == 0) { // 判断不在周末
					var _tempDate = _date.minusDays(1).format('yyyy-MM-dd');
					_date = new Date(_tempDate);
					_weekDay = _date.getDay();
				}
				TIME = _date.format('yyyyMMdd');
			} else if(parseFloat(ym) > parseFloat(compareSummerYM) && parseFloat(ym) <= parseFloat(compareWinterYMEnd)) {
				var _date = new Date(year + '-12-31');
				var _weekDay = _date.getDay();
				while(_weekDay == 6 || _weekDay == 0) { // 判断不在周末
					var _tempDate = _date.minusDays(1).format('yyyy-MM-dd');
					_date = new Date(_tempDate);
					_weekDay = _date.getDay();
				}
				TIME = _date.format('yyyyMMdd');
			}
			kUnit.setTime(TIME);
			if(KUNIT != null) {
				if(kUnit.getTime() == KUNIT.getTime()) {
					var unit = setKLineUnit(KUNIT, kUnit);
					tempRes[kUnit.getTime()] = clone(unit).toArray();
					KUNIT = clone(unit);
					cb();
				} else {
					tempRes[kUnit.getTime()] = clone(kUnit).toArray();
					KUNIT = clone(kUnit);
					cb();
				}
			} else {
				tempRes[kUnit.getTime()] = clone(kUnit).toArray();
				KUNIT = clone(kUnit);
				cb();
			}
		}, function() {
			var worker = new Worker(1, key);
			worker.setTask(tempRes);
		});
	};

/**
 * 处理季K
 */
var dealJiKline = function(results, key) {
		console.log('pid:' + process.pid + ',' + key);
		var tempRes = {};
		var TIME = '';
		var count = 0;
		async.forEach(results, function(item, cb) {
			var kUnit = setKLineUnit(item);
			var ym = item[0].toString().substring(0, 6); // 年月
			var month = ym.toString().substring(4, 6); // 月
			var year = ym.toString().substring(0, 4); // 年
			var day = item[0].toString().substring(6, 8); // 日
			var compareSummerYM = year + '' + '04'; // 夏季401~630
			var compareSpringYM = year + '' + '01'; // 春季101~331
			var compareAutumYM = year + '' + '07'; // 秋季701~930
			var compareWinterYM = year + '' + '10'; // 冬季1001～1231
			var compareNextSpringYM = parseFloat(parseFloat(year) + 1) + '' + '01'; // 第二年的3月份
			if(parseFloat(ym) >= parseFloat(compareSpringYM) && parseFloat(ym) < parseFloat(compareSummerYM)) {
				var _date = new Date(year + '-03-31');
				if(parseFloat(new Date().toString()) < parseFloat(year + '0331')) {
					_date = new Date();
				}
				var _weekDay = _date.getDay();
				while(_weekDay == 6 || _weekDay == 0) { // 判断不在周末
					var _tempDate = _date.minusDays(1).format('yyyy-MM-dd');
					_date = new Date(_tempDate);
					_weekDay = _date.getDay();
				}
				TIME = _date.format('yyyyMMdd'); // 春季
				// console.log('春'+TIME);
			} else if(parseFloat(ym) >= parseFloat(compareSummerYM) && parseFloat(ym) < parseFloat(compareAutumYM)) {
				var _date = new Date(year + '-06-30');
				if(parseFloat(new Date().toString()) < parseFloat(year + '0630')) {
					_date = new Date();
				}
				var _weekDay = _date.getDay();
				while(_weekDay == 6 || _weekDay == 0) { // 判断不在周末
					var _tempDate = _date.minusDays(1).format('yyyy-MM-dd');
					_date = new Date(_tempDate);
					_weekDay = _date.getDay();
				}
				TIME = _date.format('yyyyMMdd'); // 夏季
				// console.log('夏季'+TIME);
			} else if(parseFloat(ym) >= parseFloat(compareAutumYM) && parseFloat(ym) < parseFloat(compareWinterYM)) {
				var _date = new Date(year + '-09-30');
				if(parseFloat(new Date().toString()) < parseFloat(year + '0930')) {
					_date = new Date();
				}
				var _weekDay = _date.getDay();
				while(_weekDay == 6 || _weekDay == 0) { // 判断不在周末
					var _tempDate = _date.minusDays(1).format('yyyy-MM-dd');
					_date = new Date(_tempDate);
					_weekDay = _date.getDay();
				}
				TIME = _date.format('yyyyMMdd'); // 秋季
				// console.log('秋'+TIME);
			} else if(parseFloat(ym) >= parseFloat(compareWinterYM) && parseFloat(ym) < parseFloat(compareNextSpringYM)) {
				// console.log(compareNextSpringYM,ym);
				var _date = new Date(year + '-12-31');
				// console.log(parseFloat(new Date().toString()),parseFloat(year+'1231'));
				if(parseFloat(new Date().toString()) < parseFloat(year + '1231')) {
					_date = new Date();
				}

				var _weekDay = _date.getDay();
				// console.log(_weekDay);
				while(_weekDay == 6 || _weekDay == 0) { // 判断不在周末
					var _tempDate = _date.minusDays(1).format('yyyy-MM-dd');
					_date = new Date(_tempDate);
					_weekDay = _date.getDay();
				}
				TIME = _date.format('yyyyMMdd'); // 冬季
				// console.log('冬'+TIME);
			}
			// console.log(TIME);
			kUnit.setTime(TIME);
			if(KUNIT != null) {
				if(kUnit.getTime() == KUNIT.getTime()) {
					var unit = setKLineUnit(KUNIT, kUnit);
					tempRes[kUnit.getTime()] = clone(unit).toArray();
					KUNIT = clone(unit);
					cb();
				} else {
					tempRes[kUnit.getTime()] = clone(kUnit).toArray();
					KUNIT = clone(kUnit);
					cb();
				}
			} else {
				tempRes[kUnit.getTime()] = clone(kUnit).toArray();
				KUNIT = clone(kUnit);
				cb();
			}
		}, function() {
			// console.log(tempRes);
			var worker = new Worker(1, key);
			worker.setTask(tempRes);
		});
	};
/**
 * 处理月K
 */
var dealMonthKline = function(results, key) {
		console.log('pid:' + process.pid + ',' + key);
		var tempRes = {};
		var START = '';
		var count = 0;
		async.forEach(results, function(item, cb) {
			var kUnit = setKLineUnit(item);
			var ym = item[0].toString().substring(0, 6); // 年月
			if(count == 0) {
				START = ym;
				count = count + 1;
			}
			if(START != ym) { // 不在同月内
				START = ym;
			}
			kUnit.setTime(START);
			if(KUNIT != null) {
				if(kUnit.getTime() == KUNIT.getTime()) {
					var unit = setKLineUnit(KUNIT, kUnit);
					tempRes[kUnit.getTime()] = clone(unit).toArray();
					KUNIT = clone(unit);
					cb();
				} else {
					tempRes[kUnit.getTime()] = clone(kUnit).toArray();
					KUNIT = clone(kUnit);
					cb();
				}
			} else {
				tempRes[kUnit.getTime()] = clone(kUnit).toArray();
				KUNIT = clone(kUnit);
				cb();
			}
		}, function() {
			var worker = new Worker(1, key);
			worker.setTask(tempRes);
		});
	};
/**
 * 处理周K
 */
var dealWeekKline = function(results, key) {
		console.log('pid:' + process.pid + ',' + key);
		var tempRes = {};
		var START = '';
		var END = '';
		var count = 0;
		var _time = '';
		async.forEach(results, function(item, cb) {
			var kUnit = setKLineUnit(item);
			var ymd = item[0].toString().substring(0, 8); // 年月日
			var year = ymd.substring(0, 4);
			var month = ymd.substring(4, 6);
			var day = ymd.substring(6, 8);
			var date = new Date();
			date.setFullYear(year, parseFloat(month - 1), day);
			var weekDay = date.getDay();
			if(count == 0) { // 此处的count是为了保证START除了周日赋值外，只赋一次值
				_time = ymd;
				count = count + 1;
			}
			if(parseFloat(6 - weekDay) > 1) {
				var diff = parseFloat(6 - weekDay - 1);
				_time = date.addDays(diff).format('yyyyMMdd');
			}
			if(parseFloat(6 - weekDay) == 1) {
				_time = ymd; // 周五
			}

			kUnit.setTime(_time);
			if(KUNIT != null) {
				if(kUnit.getTime() == KUNIT.getTime()) {
					var unit = setKLineUnit(KUNIT, kUnit);
					tempRes[kUnit.getTime()] = clone(unit).toArray();
					KUNIT = clone(unit);
					cb();
				} else {
					tempRes[kUnit.getTime()] = clone(kUnit).toArray();
					KUNIT = clone(kUnit);
					cb();
				}
			} else {
				tempRes[kUnit.getTime()] = clone(kUnit).toArray();
				KUNIT = clone(kUnit);
				cb();
			}
		}, function() {
			var worker = new Worker(1, key);
			worker.setTask(tempRes);
		});
	};
/**
 * 工作队列
 */
var Worker = function(type, key) {
		this.type = type;
		var self = this;
		this.q = async.queue(function(item, cb) {
			if(type == 1) {
				pushData(key, item.item.join('|'), cb);
			}
		}, 1)
		this.setTask = function(array) {
			for(var i in array) {
				self.q.push({
					'item': array[i]
				}, function(err) {
					if(err) {
						console.log(getException(err));
					}
				});
			}
		};
	};
/**
 * 以list存入redis
 */
var pushData = function(key, str, callback) {
		// console.log('pid:'+process.pid+','+key);
		//redis.ltrim(key, -120, -1, function(ee, rr) {
			//if(!ee) {
				REDIS.rpush(key, str, function(err, res) {
					if(err) console.log(err);
					if(callback) callback();
				});
			//}
		//});
	};

var saveKLine = function(results, key, cb) {
		console.log('pid:' + process.pid + ',' + key);
		var dayKey = key.split('.');
		var weekKey = [dayKey[0], dayKey[1], dayKey[2], dayKey[3]];
		var monthKey = [dayKey[0], dayKey[1], dayKey[2], dayKey[3]];
		var jiKey = [dayKey[0], dayKey[1], dayKey[2], dayKey[3]];
		var halfYKey = [dayKey[0], dayKey[1], dayKey[2], dayKey[3]];
		var yearKey = [dayKey[0], dayKey[1], dayKey[2], dayKey[3]];
		weekKey[3] = 'WK';
		monthKey[3] = 'MTH';
		jiKey[3] = 'SY';
		halfYKey[3] = 'HY';
		yearKey[3] = 'FY';
		dealWeekKline(results, weekKey.join('.')); // 处理周K线
		dealMonthKline(results, monthKey.join('.')); // 处理月K线
		dealJiKline(results, jiKey.join('.')); // 处理季K
		dealHalfYKline(results, halfYKey.join('.')); // 半年K
		dealYearKline(results, yearKey.join('.')); // 年K
		async.forEach(results, function(item, callback) {
			var str = item.join('|');
			process.nextTick(function() {
				pushData(key, str, callback);
			});
		}, function() {
			cb();
		});
	};
var dealDataEmpty = function(arrays, ccback) {
		arrays.forEachSync(function(item, index, cback) {
			var arrayData = [];
			var data = fs.readFileSync(item, 'utf8');
			var t = item.split('/');
			var key = 'KEMCF.EMPTY.' + json[t[t.length - 1]] + '.DAY';
			data = data.substring(0, data.length - 1);
			var _array = JSON.parse(data);
			_array.forEachSync(function(val, _index, callback) {
				var ymd = val[2];
				var emptyIn = parseFloat(val.slice(6, 7)) + parseFloat(val.slice(12, 13)) + parseFloat(val.slice(48, 49));
				var emptyOut = parseFloat(val.slice(9, 10)) + parseFloat(val.slice(15, 16)) + parseFloat(val.slice(51, 52));
				arrayData.push([ymd, emptyIn, emptyOut]);
				callback();
			}, function() {
				saveKLine(arrayData, key, cback);
			});
		}, function() {
			//console.log('pid=' + process.pid + ' is over');
			ccback(null, arrays)
		});
	}

var dealDataMain = function(arrays, ccback) {
		arrays.forEachSync(function(item, index, cback) {
			var arrayData = [];
			var data = fs.readFileSync(item, 'utf8');
			var t = item.split('/');
			var key = 'KEMCF.MAIN.' + json[t[t.length - 1]] + '.DAY';
			data = data.substring(0, data.length - 1);
			var _array = JSON.parse(data);
			_array.forEachSync(function(val, _index, callback) {
				var ymd = val[2];
				var emptyIn = parseFloat(val.slice(6, 7)) + parseFloat(val.slice(12, 13));
				var emptyOut = parseFloat(val.slice(9, 10)) + parseFloat(val.slice(15, 16));
				arrayData.push([ymd, emptyIn, emptyOut]);
				callback();
			}, function() {
				saveKLine(arrayData, key, cback);
			});
		}, function() {
			ccback(null, arrays);
		});
	}

	//捕获系统异常
	process.on('uncaughtException', function(e) {
		if(e && e.stack) {
			console.error('uncaughtException:', e.stack);
		} else {
			console.error('uncaughtException:', e);
		}
	});

var start = function(array) {
		async.waterfall([

		function(callback) {
			dealDataEmpty(array, callback);
		}, function(array, callback) {
			dealDataMain(array, callback);
		}], function() {
			console.log('pid=' + process.pid + ' is over');
			process.exit(0);
		});
	}