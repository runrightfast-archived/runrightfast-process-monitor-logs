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

	after(function(done) {
		file.walk(logDir, function(err, path, dirs, names) {
			names.forEach(function(name) {
				fs.unlinkSync(name);
				console.log('DELETED : ' + name);
			});

			done();
		});

	});

	it('can watch a logDir for file changes', function(done) {
		var logManager = new LogManager(options);
		logManager.start();
		expect(logManager.started()).to.equal(true);

		var now = new Date();
		var file = path.join(logDir, now.getMilliseconds() + '.txt');
		fs.writeFile(file, '\nSOME DATA', function(err) {
			if (err) {
				done(err);
			} else {
				for ( var i = 0; i < 3; i++) {
					fs.appendFileSync(file, 'data to append');
				}
				setImmediate(function() {
					logManager.stop();
					expect(logManager.watchEventCount).to.be.gt(0);
					done();
				});
			}
		});
	});

	it.skip('can list the log files', function() {

	});

	it.skip('can gzip old logs', function() {

	});

	it.skip('can delete old log files', function() {

	});

	it.skip('can tail a log file', function() {

	});

	it.skip('can tail -n a log file', function() {

	});

	it.skip('can head -n a log file', function() {

	});
});