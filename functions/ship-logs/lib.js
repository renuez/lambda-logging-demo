'use strict';

const co      = require('co');
const Promise = require('bluebird');
const parse   = require('./parse');
const net     = require('net');
const logzio  = require('logzio-nodejs');
const host    = process.env.logstash_host || 'listener.logz.io';
const port    = process.env.logstash_port || 8071;
const protocol= process.env.logstash_protocol || 'https';
const token   = process.env.token; //|| '<INSERT_TOKEN_FOR_LOCAL_DEBUGGING>';

let processAll = co.wrap(function* (logGroup, logStream, logEvents) {
  let lambdaVersion = parse.lambdaVersion(logStream);
  let functionName  = parse.functionName(logGroup);

  yield new Promise((resolve, reject) => {

    const logger = logzio.createLogger({
      token: token,
      host: host,
      port: port,
      protocol: protocol,
      type: 'cloudwatch',
      debug: true,
    });

    for (let logEvent of logEvents) {
      try {
        let log = parse.logMessage(logEvent);
        
        if (log) {
          log.logStream     = logStream;
          log.logGroup      = logGroup;
          log.functionName  = functionName;
          log.lambdaVersion = lambdaVersion;
          log.fields        = log.fields || {};
          logger.log(log);
        }
      } catch (err) {
        console.error(err.message);
      }

      logger.sendAndClose();
      resolve();
    };
  });
});

module.exports = processAll;