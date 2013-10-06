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
			fileWatcher.close();
			fileWatcher = undefined;
			log.info('Stopped watching : ' + this.logDir);
		} else {
			if (log.isDebugEnabled()) {
				log.debug('Not watching : ' + this.logDir);
			}
		}
	};

	LogManager.prototype.started = function() {
		return !!fileWatcher;
	};

	LogManager.prototype.handleLogDirEvent = function(event, filename) {
		var self = this;
		if (log.isDebugEnabled()) {
			log.debug('handleLogDirEvent invoked : ' + event + ' : ' + filename);
		}
		childProcess.exec("ps | awk '{print $1;}'", function(error, stdout, stderr) {
			if (error) {
				log.error('exec error: ' + error);
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
					log.debug('stdout: ' + pids);
					log.debug('stderr: ' + stderr);
				}

				var matchResults = [];
				var gzippedLogFiles = [];

				fs.readdir(self.logDir, function(err, files) {
					if (err) {
						log.error('Failed reading dir : ' + self.logDir + " : " + err);
					} else {
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
					}
				});

			}

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

	LogManager.prototype.deleteOldLogFiles = function(gzippedLogFiles) {
		var now = new Date();
		var expireTime = now.getTime() - (1000 * 60 * 60 * 24) * this.retentionDays;
		gzippedLogFiles.forEach(function(f) {
			fs.exists(f.file, function(exists) {
				if (exists) {
					fs.stat(f.file, function(err, stats) {
						if (err) {
							log.warn('Failed to stat : ' + f + ' : ' + err);
						} else {
							if (stats.ctime.getTime() < expireTime) {
								fs.unlink(f.file, function(err) {
									if (err) {
										fs.exists(f.file, function(exists) {
											if (exists) {
												log.error('failed to delete : ' + f.file + ' : ' + err);
											}
										});
									} else {
										if (log.isDebugEnabled()) {
											log.debug('deleted : ' + f.file);
										}
									}
								});
							}
						}
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
		var key = null;
		for (key in activeLogFilesGroupedByPid) {
			if (activeLogFilesGroupedByPid[key].length > this.maxNumberActiveFiles) {
				groupsAboveMaxActiveLogFiles.push(activeLogFilesGroupedByPid[key]);
			}
		}

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

	module.exports = LogManager;

}());