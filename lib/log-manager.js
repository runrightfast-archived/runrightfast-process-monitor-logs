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

/**
 * The LogManager performs the following
 * 
 * <code>
 * 1. watches the log dir for new files
 * 2. GZIPs any log files files that have no corresponding active process - this is dtermined by parsing the pid out of the log file name
 * 3. For any log files that have an active process, files with sequences older than the maxNumberActiveFiles are gzipped.
 * 4. Files older than retentionDays are deleted.
 * </code>
 * 
 * options
 * 
 * <code>
 * {
 * logDir : '/logs/api-gateway-1.0.0',				// REQUIRED
 * logLevel : 'WARN',								// OPTIONAL - default is 'WARN',
 * maxNumberActiveFiles : 5,				    	// OPTIONAL - max number of active log files to keep. Default is 5.
 * retentionDays : 10						        // OPTIONAL - number of days to retain gzipped log files. Default is 10 days
 * }
 * </code>
 */
(function() {
	'use strict';

	var lodash = require('lodash');
	var assert = require('assert');
	var extend = require('extend');
	var fs = require('fs');
	var childProcess = require('child_process');
	var zlib = require('zlib');
	var path = require('path');
	var when = require('when');
	var uuid = require('uuid');

	var events = require('runrightfast-commons').events;
	var eventEmitter = new events.AsyncEventEmitter();

	var logging = require('runrightfast-commons').logging;
	var pkgInfo = require('pkginfo')(module, 'name');
	var log = logging.getLogger(pkgInfo.name);

	var fileWatcher = null;

	// [event].[pid].log.[fileNumber]
	// e.g., ops.25559.log.001
	var goodLogFilePattern = /^\w+\.(?:(\d+))\.log\.(?:(\d+))$/;

	var gzippedLogFilePattern = /^\w+\.(?:(\d+))\.log\.(?:(\d+))\.gz$/;

	/* default config */
	var config = {
		logLevel : 'WARN',
		maxNumberActiveFiles : 5,
		retentionDays : 10
	};

	var validateConfig = function(config) {
		assert(lodash.isObject(config), 'options is required and must be an Object');
		assert(lodash.isString(config.logDir), 'options.logDir is required and must be a String');
		assert(lodash.isNumber(config.retentionDays) && config.retentionDays > 0, 'options.retentionDays is required and must be > 0');
		assert(lodash.isNumber(config.maxNumberActiveFiles) && config.maxNumberActiveFiles > 0, 'options.maxNumberActiveFiles is required and must be > 0');
	};

	var LogManager = function(options) {
		extend(true, config, options);
		logging.setLogLevel(log, config.logLevel);
		if (log.isDebugEnabled()) {
			log.debug(config);
		}
		validateConfig(config);

		this.logDir = config.logDir;
		this.watchEventCount = 0;
		this.goodLogFilePattern = goodLogFilePattern;
		this.maxNumberActiveFiles = config.maxNumberActiveFiles;
		this.retentionDays = config.retentionDays;
		// file path -> {process:[process],listeners:{}
		// where listeners is a map of : listenerId ->
		// {onDataCallback,onCloseCallback}
		this.tailProcesses = {};
	};

	LogManager.prototype.start = function() {
		var self = this;
		if (!fileWatcher) {
			fileWatcher = fs.watch(this.logDir, function(event, filename) {
				if (log.isDebugEnabled()) {
					log.debug(event + ' : ' + filename);
				}
				self.watchEventCount++;
				self.handleLogDirEvent(event, filename);
			});
			log.info('Started watching : ' + this.logDir);
		} else {
			if (log.isDebugEnabled()) {
				log.debug('Already watching : ' + this.logDir);
			}
		}
	};

	LogManager.prototype.stop = function() {
		if (fileWatcher) {
			var self = this;
			fileWatcher.close();
			fileWatcher = undefined;
			log.info('Stopped watching : ' + this.logDir);

			lodash.keys(this.tailProcesses).forEach(function(file) {
				if (log.isDebugEnabled()) {
					log.debug('killing tail process for : ' + file);
				}
				self.tailProcesses[file].process.kill();
			});
			eventEmitter.removeAllListeners();
		} else {
			if (log.isDebugEnabled()) {
				log.debug('Not watching : ' + this.logDir);
			}
		}
	};

	LogManager.prototype.started = function() {
		return !!fileWatcher;
	};

	LogManager.prototype.logDirectoryFilesPromise = function() {
		var self = this;
		return when.promise(function(resolve, reject) {
			fs.readdir(self.logDir, function(err, files) {
				if (err) {
					log.error('Failed reading dir : ' + self.logDir + " : " + err);
					reject(err);
				} else {
					if (log.isDebugEnabled()) {
						log.debug('logDirectoryFilesPromise() resolved : ' + files);
					}
					resolve(files);
				}
			});
		});
	};

	LogManager.prototype.handleLogDirEvent = function(event, filename) {
		var self = this;
		if (log.isDebugEnabled()) {
			log.debug('handleLogDirEvent invoked : ' + event + ' : ' + filename);
		}

		var pidsPromise = when.promise(function(resolve, reject) {
			childProcess.exec("ps | awk '{print $1;}'", function(error, stdout, stderr) {
				if (error) {
					log.error('exec error: ' + error);
					reject(error);
				} else {
					var pids = stdout.split('\n');
					pids.shift();
					pids = pids.map(function(pid) {
						return parseInt(pid, 10);
					});
					pids = pids.filter(function(pid) {
						return !lodash.isNaN(pid);
					});
					if (log.isDebugEnabled()) {
						log.debug('stdout: ' + pids + (stderr ? ('\nstderr: ' + stderr) : ''));
					}

					resolve(pids);
				}

			});
		});

		when(pidsPromise, function(pids) {
			when(self.logDirectoryFilesPromise(), function(files) {
				var matchResults = [];
				var gzippedLogFiles = [];
				files.forEach(function(file) {
					var matchResult = file.match(goodLogFilePattern);
					if (matchResult) {
						if (log.isDebugEnabled()) {
							log.debug(file + ' : matchResult : ' + matchResult);
						}
						matchResults.push({
							file : path.join(self.logDir, file),
							pid : parseInt(matchResult[1], 10),
							logFileSequence : parseInt(matchResult[2], 10)
						});
					} else {
						matchResult = file.match(gzippedLogFilePattern);
						if (matchResult) {
							if (log.isDebugEnabled()) {
								log.debug(file + ' : matchResult : ' + matchResult);
							}
							gzippedLogFiles.push({
								file : path.join(self.logDir, file),
								pid : parseInt(matchResult[1], 10),
								logFileSequence : parseInt(matchResult[2], 10)
							});
						} else {
							if (log.isDebugEnabled()) {
								log.debug('no match for : ' + file);
							}
						}
					}
				});

				if (matchResults.length > 0) {
					self.processFiles(pids, matchResults);
				}

				if (gzippedLogFiles.length > 0) {
					self.deleteOldLogFiles(gzippedLogFiles);
				}
			});

		});

	};

	/**
	 * 
	 * @param pids
	 * @param matchResults
	 *            an object with the following properties
	 * 
	 * <code>
	 * file 			absolute log file path
	 * pid 				
	 * logFileSequence 
	 * <code>
	 */
	LogManager.prototype.processFiles = function(pids, matchResults) {
		var activeLogFiles = [];
		var self = this;
		matchResults.forEach(function(matchResult) {
			if (log.isDebugEnabled()) {
				log.debug('lodash.contains(pids, matchResult.pid) = ' + lodash.contains(pids, matchResult.pid) + ' :: ' + pids + '::' + matchResult.pid);
			}
			if (!lodash.contains(pids, matchResult.pid)) {
				self.gzip(matchResult.file);
			} else {
				activeLogFiles.push(matchResult);
			}
		});
		this.processFilesForActiveProcesses(activeLogFiles);
	};

	/**
	 * @return the Epoch time for retaining log files. Log files that are older
	 *         than the time returned can be deleted.
	 */
	LogManager.prototype.getLogsRetentionTimeMillis = function() {
		var now = new Date();
		return now.getTime() - (1000 * 60 * 60 * 24) * this.retentionDays;
	};

	LogManager.prototype.deleteOldLogFiles = function(gzippedLogFiles) {
		var expireTime = this.getLogsRetentionTimeMillis();
		if (log.isDebugEnabled()) {
			log.debug('expireTime = ' + new Date(expireTime).toISOString());
		}
		gzippedLogFiles.forEach(function(f) {
			fs.exists(f.file, function(exists) {
				if (exists) {
					var statsPromise = when.promise(function(resolve, reject) {
						fs.stat(f.file, function(err, stats) {
							if (err) {
								reject(err);
							} else {
								resolve(stats);
							}
						});
					});

					when(statsPromise, function(stats) {
						if (log.isDebugEnabled()) {
							log.debug('stats.mtime.getTime() = ' + stats.mtime.toISOString());
						}
						if (stats.mtime.getTime() < expireTime) {
							fs.unlink(f.file, function(err) {
								if (err) {
									fs.exists(f.file, function(exists) {
										if (exists) {
											log.error('failed to delete : ' + f.file + ' : ' + err);
										}
									});
								} else {
									if (log.isInfoEnabled()) {
										log.info('deleteOldLogFiles() : deleted : ' + f.file);
									}
								}
							});
						} else {
							if (log.isDebugEnabled()) {
								log.debug('log file falls within the retention period : ' + f.file + ' : mtime = ' + stats.mtime.toISOString());
							}
						}
					}, function(err) {
						fs.exists(f.file, function(exists) {
							if (exists) {
								log.warn('Failed to stat : ' + f + ' : ' + err);
							}
						});
					});

				}
			});
		});
	};

	LogManager.prototype.processFilesForActiveProcesses = function(activeLogFiles) {
		var self = this;
		var activeLogFilesGroupedByPid = lodash.groupBy(activeLogFiles, function(f) {
			return f.pid;
		});

		var groupsAboveMaxActiveLogFiles = [];
		lodash.keys(activeLogFilesGroupedByPid).forEach(function(key) {
			if (log.isDebugEnabled()) {
				log.debug('activeLogFilesGroupedByPid[\'' + key + '\'].length = ' + activeLogFilesGroupedByPid[key].length);
			}
			if (activeLogFilesGroupedByPid[key].length > self.maxNumberActiveFiles) {
				groupsAboveMaxActiveLogFiles.push(activeLogFilesGroupedByPid[key]);
			}
		});

		groupsAboveMaxActiveLogFiles.forEach(function(group) {
			var sortedFiles = lodash.sortBy(group, function(f) {
				return f.pid + '.' + f.logFileSequence;
			});
			sortedFiles.reverse();
			var filesToGzip = sortedFiles.slice(self.maxNumberActiveFiles);
			filesToGzip.forEach(function(f) {
				self.gzip(f.file);
			});
		});

	};

	/**
	 * 
	 * @param logFile
	 *            absolute file path
	 */
	LogManager.prototype.gzip = function(logFile) {
		if (log.isDebugEnabled()) {
			log.debug('gzip(' + logFile + ')');
		}
		fs.exists(logFile, function(exists) {
			if (exists) {
				if (log.isDebugEnabled()) {
					log.debug('gzip(' + logFile + ') - logFile exists');
				}
				var gzip = zlib.createGzip();
				var inp = fs.createReadStream(logFile);
				var out = fs.createWriteStream(logFile + '.gz');

				out.on('finish', function() {
					fs.unlink(logFile, function(err) {
						if (err) {
							fs.exists(logFile, function(exists) {
								if (exists) {
									log.error('failed to delete : ' + logFile + ' : ' + err);
								}
							});
						} else {
							if (log.isDebugEnabled()) {
								log.debug('deleted : ' + logFile);
							}
						}
					});
				});

				inp.pipe(gzip).pipe(out);
			} else {
				if (log.isDebugEnabled()) {
					log.debug('gzip(' + logFile + ') - logFile does not exist');
				}
			}
		});
	};

	/**
	 * Performs a linux tail -n
	 * 
	 * options
	 * 
	 * <code> 
	 * file					REQUIRED - absolute file path to tail
	 * onDataCallback		REQUIRED - callback for data
	 * 								 - function(data){} - where data is Buffer
	 * onCloseCallback		OPTIONAL - callback for when there is no more data
	 * 								 - function(code){} - where code is the process exit code
	 * lines				OPTIONAL - default is 10
	 * <code>
	 */
	LogManager.prototype.tail = function(options) {
		assert(options, 'options is required');
		assert(options.file, 'options.file is required');
		assert(lodash.isFunction(options.onDataCallback), 'options.onDataCallback is required and must be a function');
		if (!lodash.isUndefined(options.onCloseCallback)) {
			assert(lodash.isFunction(options.onCloseCallback), 'options.onCloseCallback is required and must be a function');
		}
		if (!lodash.isUndefined(options.lines)) {
			assert(lodash.isNumber(options.lines) && options.lines > 0, 'options.lines must be a number > 0');
		}

		fs.exists(options.file, function(exists) {
			if (exists) {
				var tail = childProcess.spawn('tail', [ '-n', options.lines || 10, options.file ]);

				tail.stdout.on('data', function(data) {
					options.onDataCallback(data);
				});

				if (options.onCloseCallback) {
					tail.on('close', function(code) {
						options.onCloseCallback(code);
					});
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug('tail() - file does not exist: ' + options.file);
				}
			}
		});

	};

	/**
	 * performs a Linux head -n
	 * 
	 * options
	 * 
	 * <code> 
	 * file					REQUIRED - absolute file path to head
	 * onDataCallback		REQUIRED - callback for data
	 * 								 - function(data){} - where data is Buffer
	 * onCloseCallback		OPTIONAL - callback for when there is no more data
	 * 								 - function(code){} - where code is the process exit code
	 * lines				OPTIONAL - default is 10
	 * <code>
	 */
	LogManager.prototype.head = function(options) {
		assert(options, 'options is required');
		assert(options.file, 'options.file is required');
		assert(lodash.isFunction(options.onDataCallback), 'options.onDataCallback is required and must be a function');
		if (!lodash.isUndefined(options.onCloseCallback)) {
			assert(lodash.isFunction(options.onCloseCallback), 'options.onCloseCallback is required and must be a function');
		}
		if (!lodash.isUndefined(options.lines)) {
			assert(lodash.isNumber(options.lines) && options.lines > 0, 'options.lines must be a number > 0');
		}

		fs.exists(options.file, function(exists) {
			if (exists) {
				var head = childProcess.spawn('head', [ '-n', options.lines || 10, options.file ]);

				head.stdout.on('data', function(data) {
					options.onDataCallback(data);
				});

				if (options.onCloseCallback) {
					head.on('close', function(code) {
						options.onCloseCallback(code);
					});
				}
			} else {
				if (log.isDebugEnabled()) {
					log.debug('head() - file does not exist: ' + options.file);
				}
			}
		});

	};

	/**
	 * options
	 * 
	 * <code> 
	 * file							REQUIRED - absolute file path to tail
	 * onDataCallback				REQUIRED - callback for data
	 * 										 - function(data){} - where data is Buffer
	 * onCloseCallback				OPTIONAL - callback for when there is no more data
	 * 										 - function(code){} - where code is the process exit code
	 * onRegistrationCallback		OPTIONAL - callback which sends back the listener id, which can be used to stop following
	 * 										 - function(err,file,listenerId){} 
	 * <code>
	 *
	 */
	LogManager.prototype.tailFollow = function(options) {
		var self = this;
		assert(options, 'options is required');
		assert(options.file, 'options.file is required');
		assert(lodash.isFunction(options.onDataCallback), 'options.onDataCallback is required and must be a function');
		if (!lodash.isUndefined(options.onCloseCallback)) {
			assert(lodash.isFunction(options.onCloseCallback), 'options.onCloseCallback is required and must be a function');
		}
		if (!lodash.isUndefined(options.onRegistrationCallback)) {
			assert(lodash.isFunction(options.onRegistrationCallback), 'options.onRegistrationCallback is required and must be a function');
		}
		if (!lodash.isUndefined(options.lines)) {
			assert(lodash.isNumber(options.lines) && options.lines > 0, 'options.lines must be a number > 0');
		}

		fs.exists(options.file, function(exists) {
			if (exists) {
				var listenerId = uuid.v4();
				if (options.onRegistrationCallback) {
					options.onRegistrationCallback(null, options.file, listenerId);
				}

				var tailDataEvent = 'tail::data::' + options.file;
				var tailCloseEvent = 'tail::close::' + options.file;
				eventEmitter.on(tailDataEvent, options.onDataCallback);
				if (options.onCloseCallback) {
					eventEmitter.on(tailCloseEvent, options.onCloseCallback);
				}

				if (!self.tailProcesses[options.file]) {
					var tail = childProcess.spawn('tail', [ '-f', options.file ]);
					var listeners = {};
					listeners[listenerId] = {
						onDataCallback : options.onDataCallback,
						onCloseCallback : options.onCloseCallback
					};
					self.tailProcesses[options.file] = {
						process : tail,
						listeners : listeners
					};

					tail.stdout.on('data', function(data) {
						eventEmitter.emit(tailDataEvent, data);
					});

					if (options.onCloseCallback) {
						tail.on('close', function(code) {
							eventEmitter.emit(tailCloseEvent, code);
						});
					}
				} else {
					self.tailProcesses[options.file].listeners[listenerId] = {
						onDataCallback : options.onDataCallback,
						onCloseCallback : options.onCloseCallback
					};
				}

			} else {
				if (log.isDebugEnabled()) {
					log.debug('tail() - file does not exist: ' + options.file);
				}
				if (options.onRegistrationCallback) {
					options.onRegistrationCallback(new Error('file does not exist: ' + options.file));
				}
			}
		});

	};

	LogManager.prototype.stopTailFollowing = function(file, listenerId) {
		var tailedProcess = this.tailProcesses[file];
		if (tailedProcess) {
			var callbacks = tailedProcess.listeners[listenerId];
			if (callbacks) {
				if (log.isDebugEnabled()) {
					log.debug('stopTailFollowing() : found callbacks for tailed process : ' + file + ' -> ' + listenerId);
				}
				var tailDataEvent = 'tail::data::' + file;
				var tailCloseEvent = 'tail::close::' + file;
				eventEmitter.removeListener(tailDataEvent, callbacks.onDataCallback);
				if (callbacks.onCloseCallback) {
					eventEmitter.removeListener(tailCloseEvent, callbacks.onCloseCallback);
				}
				delete tailedProcess.listeners[listenerId];
				var remainingListenerCount = lodash.keys(tailedProcess.listeners).length;
				if (log.isDebugEnabled()) {
					log.debug('remainingListenerCount = ' + remainingListenerCount);
				}
				if (remainingListenerCount === 0) {
					tailedProcess.process.kill();
					delete this.tailProcesses[file];
				}
			}
		}
	};

	module.exports = LogManager;

}());