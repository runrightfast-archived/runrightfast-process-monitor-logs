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
 * options
 * 
 * <code>
 * {
 * logDir : '/logs/api-gateway-1.0.0',				// REQUIRED
 * logLevel : 'WARN'								// OPTIONAL - default is 'WARN'
 * }
 * </code>
 */
(function() {
	'use strict';

	var lodash = require('lodash');
	var assert = require('assert');
	var extend = require('extend');
	var fs = require('fs');

	var logging = require('runrightfast-commons').logging;
	var pkgInfo = require('pkginfo')(module, 'name');
	var log = logging.getLogger(pkgInfo.name);

	var fileWatcher = null;

	/* default config */
	var config = {
		logLevel : 'WARN'
	};

	var validateConfig = function(config) {
		assert(lodash.isObject(config), 'options is required and must be an Object');
		assert(lodash.isString(config.logDir), 'options.logDir is required and must be a String');
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
	};

	LogManager.prototype.start = function() {
		var self = this;
		if (!fileWatcher) {
			fileWatcher = fs.watch(this.logDir, function(event, filename) {
				if (log.isDebugEnabled()) {
					log.debug(event + ' : ' + filename);
				}
				self.watchEventCount++;
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

	module.exports = LogManager;

}());