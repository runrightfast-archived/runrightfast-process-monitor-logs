/**
 * Copyright [2013] [runrightfast.co]
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

'use strict';
var expect = require('chai').expect;

var LogManager = require('..').LogManager;
var fs = require('fs');
var file = require('file');
var path = require('path');
var when = require('when');

describe('LogManager', function() {
	var logDir = file.path.abspath('temp/logs');

	var options = {
		logDir : logDir,
		logLevel : 'DEBUG'
	};

	console.log('logDir : ' + logDir);

	before(function(done) {
		file.mkdirs(logDir, parseInt('0755', 8), function(err) {
			if (err) {
				done(err);
			} else {
				done();
			}
		});
	});

	afterEach(function(done) {
		setTimeout(function() {
			fs.readdir(logDir, function(err, names) {
				names.forEach(function(name) {
					fs.unlinkSync(path.join(logDir, name));
					console.log('DELETED : ' + name);
				});
				done();
			});
		}, 50);
	});

	it('can watch a logDir for file changes', function(done) {
		var logManager = new LogManager(options);
		logManager.start();
		expect(logManager.started()).to.equal(true);

		var now = new Date();
		var logFile = path.join(logDir, 'ops.' + now.getMilliseconds() + '.log.001');
		fs.writeFile(logFile, '\nSOME DATA', function(err) {
			if (err) {
				console.error('*** writeFile failed : ' + err);
				done(err);
			} else {
				for ( var i = 0; i < 3; i++) {
					fs.exists(logFile, function(exists) {
						fs.appendFile(logFile, '\ndata to append', function(err) {
							if (err) {
								console.error('Append failed : ' + err);
							} else {
								console.log('Appended to : ' + logFile);
							}
						});
					});
				}
				setImmediate(function() {
					logManager.stop();
					expect(logManager.watchEventCount).to.be.gt(0);
					setTimeout(done, 20);
				});
			}
		});
	});

	it('can list the log files', function(done) {
		var logManager = new LogManager(options);

		var logFile = path.join(logDir, 'ops.' + new Date().getMilliseconds() + '.log.001');
		fs.writeFileSync(logFile, '\nSOME DATA');
		var promise = logManager.logDirectoryFilesPromise();
		when(promise, function(files) {
			console.log('files : ' + files);
			expect(files.length).to.be.gt(0);
			done();
		}, function(err) {
			done(err);
		});

	});

	it('can gzip old logs', function(done) {
		var logManager = new LogManager(options);

		var now = new Date();
		var logFile = path.join(logDir, 'ops.' + now.getMilliseconds() + '.log.001');
		fs.writeFile(logFile, '\nSOME DATA', function(err) {
			if (err) {
				console.error('*** writeFile failed : ' + err);
				done(err);
			} else {
				logManager.gzip(logFile);

				setTimeout(function() {
					expect(fs.existsSync(logFile)).to.equal(false);
					expect(fs.existsSync(logFile + '.gz')).to.equal(true);
					done();
				}, 20);

			}
		});
	});

	it("gzips active log files that are beyond the max active log files limit", function(done) {
		var logManager = new LogManager(options);
		logManager.start();
		expect(logManager.started()).to.equal(true);

		var isDone = false;
		for ( var i = 0; i <= logManager.maxNumberActiveFiles; i++) {
			var logFile = path.join(logDir, 'ops.' + process.pid + '.log.00' + i);
			fs.writeFile(logFile, '\nSOME DATA', function(err) {
				if (err) {
					console.error('*** writeFile failed : ' + err);
					done(err);
				} else {
					setImmediate(function() {
						if (isDone) {
							return;
						}
						console.log('logManager.watchEventCount = ' + logManager.watchEventCount);
						if (logManager.watchEventCount >= (logManager.maxNumberActiveFiles)) {
							logManager.stop();
							setTimeout(function() {
								var expectedGzippedFile = path.join(logDir, 'ops.' + process.pid + '.log.000.gz');
								expect(fs.existsSync(expectedGzippedFile)).to.equal(true);
								for ( var ii = logManager.maxNumberActiveFiles; ii > 0; ii--) {
									var expectedNonGzippedFile = path.join(logDir, 'ops.' + process.pid + '.log.00' + ii);
									expect(fs.existsSync(expectedNonGzippedFile)).to.equal(true);
								}
								done();
							}, 100);
							isDone = true;
						}
					});

				}
			});
		}

	});

	it('can delete old log files', function(done) {
		var logManager = new LogManager(options);

		logManager.start();

		var logFile = path.join(logDir, 'ops.' + process.pid + '.log.001.gz');
		fs.writeFileSync(logFile, 'SOME DATA');

		var oldLogFile = path.join(logDir, 'ops.' + process.pid + '.log.002.gz');
		fs.writeFileSync(oldLogFile, 'SOME DATA');
		var expireTime = new Date(logManager.getLogsRetentionTimeMillis() - 1);
		console.log('expireTime = ' + new Date(expireTime).toISOString());
		console.log('logManager.getLogsRetentionTimeMillis() = ' + new Date(logManager.getLogsRetentionTimeMillis()).toISOString());
		fs.utimesSync(oldLogFile, expireTime, expireTime);

		setTimeout(function() {
			logManager.stop();
			expect(fs.existsSync(logFile)).to.equal(true);
			expect(fs.existsSync(oldLogFile)).to.equal(false);
			done();
		}, 50);

	});

	it('can tail a log file', function(done) {
		var logManager = new LogManager(options);

		var logFile = path.join(logDir, 'ops.' + process.pid + '.log.001');
		console.log('logFile = ' + logFile);
		var data = '';
		var i = 0;
		for (i = 0; i < 20; i++) {
			data += '\n***' + i;
		}
		fs.writeFileSync(logFile, data);

		var tailOptions = {
			file : logFile,
			onDataCallback : function(data) {
				console.log(data.toString());
			},
			onCloseCallback : function(code) {
				console.log('code = ' + code);
			},
			onRegistrationCallback : function(err, file, listenerId) {
				setTimeout(function() {
					logManager.stopTailFollowing(file, listenerId);
					done();
				}, 100);
			}
		};

		logManager.tailFollow(tailOptions);

		for (i = 20; i < 40; i++) {
			var ii = i;
			fs.appendFile(logFile, '\n***' + ii, function(err) {
				if (err) {
					done(err);
				}
			});
		}
	});

	it('multiple listeners can tail the same log file', function(done) {
		var logManager = new LogManager(options);
		logManager.start();

		var logFile = path.join(logDir, 'ops.' + process.pid + '.log.001');
		console.log('logFile = ' + logFile);
		var data = '';
		var i = 0;
		for (i = 0; i < 20; i++) {
			data += '\n***' + i;
		}
		fs.writeFileSync(logFile, data);

		var tailOptions1 = {
			file : logFile,
			onDataCallback : function(data) {
				console.log('#1 - ' + data.toString());
			},
			onCloseCallback : function(code) {
				console.log('#1 - ' + 'code = ' + code);
			}
		};

		var tailOptions2 = {
			file : logFile,
			onDataCallback : function(data) {
				console.log('#2 - ' + data.toString());
			},
			onCloseCallback : function(code) {
				console.log('#2 - ' + 'code = ' + code);
			}
		};

		logManager.tailFollow(tailOptions1);
		logManager.tailFollow(tailOptions2);

		for (i = 20; i < 40; i++) {
			var ii = i;
			fs.appendFile(logFile, '\n***' + ii, function(err) {
				if (err) {
					done(err);
				}
			});
		}

		setTimeout(function() {
			logManager.stop();
			done();
		}, 100);
	});

	it('stop will kill any processes that are tailing log files', function(done) {
		var logManager = new LogManager(options);
		logManager.start();

		var logFile = path.join(logDir, 'ops.' + process.pid + '.log.001');
		var data = '';
		var i = 0;
		for (i = 0; i < 20; i++) {
			data += '\n***' + i;
		}
		fs.writeFileSync(logFile, data);

		var tailOptions = {
			file : logFile,
			onDataCallback : function(data) {
				console.log(data.toString());
			},
			onCloseCallback : function(code) {
				console.log('code = ' + code);
			},
			onRegistrationCallback : function(err, file, listenerId) {
				setTimeout(function() {
					logManager.stop();
					done();
				}, 100);
			}
		};

		logManager.tailFollow(tailOptions);

		for (i = 20; i < 40; i++) {
			var ii = i;
			fs.appendFile(logFile, '\n***' + ii, function(err) {
				if (err) {
					done(err);
				}
			});
		}
	});

	it('can tail -n a log file', function(done) {
		var logManager = new LogManager(options);

		var logFile = path.join(logDir, 'ops.' + process.pid + '.log.001');
		var data = '';
		for ( var i = 0; i < 20; i++) {
			data += '\n***' + i;
		}
		fs.writeFileSync(logFile, data);

		logManager.tail({
			file : logFile,
			onDataCallback : function(data) {
				console.log(data.toString());
			},
			onCloseCallback : function(code) {
				console.log('code = ' + code);
				done();
			}
		});

	});

	it('can head -n a log file', function(done) {
		var logManager = new LogManager(options);

		var logFile = path.join(logDir, 'ops.' + process.pid + '.log.001');
		var data = '';
		for ( var i = 0; i < 20; i++) {
			data += '***' + i + '\n';
		}
		fs.writeFileSync(logFile, data);

		logManager.head({
			file : logFile,
			onDataCallback : function(data) {
				console.log(data.toString());
			},
			onCloseCallback : function(code) {
				console.log('code = ' + code);
				done();
			}
		});
	});

	it('start and stop can be called mulitple times with no harm', function() {
		var logManager = new LogManager(options);
		logManager.start();
		expect(logManager.started()).to.equal(true);
		logManager.start();
		expect(logManager.started()).to.equal(true);

		logManager.stop();
		expect(logManager.started()).to.equal(false);
		logManager.stop();
		expect(logManager.started()).to.equal(false);
	});

	it('trying to gzip file that does not exist will simply log it', function() {
		var logManager = new LogManager(options);
		logManager.gzip('fsdfsdfsf');
	});

	it('trying to head a file that does not exist will simply log it', function() {
		var logManager = new LogManager(options);

		logManager.head({
			file : 'sfsdfsfs',
			onDataCallback : function(data) {
				console.log(data.toString());
			},
			onCloseCallback : function(code) {
				console.log('code = ' + code);
				done();
			}
		});
	});

	it('trying to tail a file that does not exist will simply log it', function() {
		var logManager = new LogManager(options);

		logManager.tail({
			file : 'sfsdfsfs',
			onDataCallback : function(data) {
				console.log(data.toString());
			},
			onCloseCallback : function(code) {
				console.log('code = ' + code);
				done();
			}
		});
	});

	it('trying to tail -f a file that does not exist will simply log it and notify the onRegistrationCallback with an Error', function(done) {
		var logManager = new LogManager(options);

		var tailOptions = {
			file : 'sfsdfsdfsdf',
			onDataCallback : function(data) {
				console.log(data.toString());
			},
			onCloseCallback : function(code) {
				console.log('code = ' + code);
			},
			onRegistrationCallback : function(err, file, listenerId) {
				if (err) {
					done();
				} else {
					done(new Error('expected an Error because the file does not exist'));
				}
			}
		};

		logManager.tailFollow(tailOptions);

	});
});