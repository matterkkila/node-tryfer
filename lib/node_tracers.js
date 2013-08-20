// Copyright 2012 Rackspace Hosting, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * Tracers only for Node.
 * Note: Node tracers assume tryfer library is included in a long running
 * process.
 */

if (typeof module === 'undefined' || !module.exports) {
  return;
}

var util = require('util');
var async = require('async');

var EndAnnotationTracer = require('./tracers').EndAnnotationTracer;
var formatters = require('./formatters');

/**
 * Buffer traces and defer recording until maxTraces have been received or
 * sendInterval has elapsed since the last trace was recorded.
 *
 * @param {Tracer} tracer Tracer provider to record buffered tracers to.
 * @param {Options} options Options object with the following keys:
 * - maxTraces - Number of traces to buffer before recording occurs (default:
 *   50).
 * - sendInterval - An average number of seconds which can pass after the last
 *   trace has been sent to the backend before sending all the buffered traces
 *   to the backend again (default: 10).
 */
function BufferingTracer(tracer, options) {
  options = options || {};

  var self = this;

  this._tracer = tracer;
  this._maxTraces = options.maxTraces || 50;
  this._sendInterval = options.sendInterval ? (options.sendInterval * 1000) : 10 * 1000;
  this._lastSentTs = Date.now();
  this._buffer = [];
  this._stopped = false;

  this._periodSendTimeoutId = setTimeout(this._periodicSendFunction.bind(this),
                                         this._sendInterval);
}

BufferingTracer.prototype.sendTraces = function(traces) {
  var buffer;

  this._buffer = this._buffer.concat(traces);

  if (this._buffer.length >= this._maxTraces) {
    buffer = this._buffer.slice();
    this._buffer = [];

    // Flush the buffer in the next tick
    process.nextTick(this._sendTraces.bind(this, buffer));
  }
};

/**
 * Clear any outgoing timers and stop the tracer.
 */
BufferingTracer.prototype.stop = function() {
  clearTimeout(this._periodSendTimeoutId);
  this._stopped = true;
};

BufferingTracer.prototype._periodicSendFunction = function() {
  var now = Date.now(), buffer;

  if (((now - this._sendInterval) > this._lastSentTs) &&
      this._buffer.length >= 1) {
    buffer = this._buffer.slice();
    this._buffer = [];
    this._sendTraces(buffer);
  }

  // Re-schedule itself
  this._periodSendTimeoutId = setTimeout(this._periodicSendFunction.bind(this),
                                         this._sendInterval);
};

BufferingTracer.prototype._sendTraces = function(traces) {
  this._lastSentTs = Date.now();
  this._tracer.sendTraces(traces);
};

/**
 * A tracer that records to zipkin through scribe. Requires a scribe
 * client ({node-scribe}).
 *
 * @param {scribe.Scribe object} scribeClient The client to use to write to
 *    scribe
 * @param {String} category The category to use when writing to scribe -
 *    defaults to "zipkin" if not passed
 */
function RawZipkinTracer(scribeClient, category) {
  this.scribeClient = scribeClient;
  this.category = (category) ? category : 'zipkin';

  EndAnnotationTracer.call(this, this.sendTraces);
}

util.inherits(RawZipkinTracer, EndAnnotationTracer);

RawZipkinTracer.prototype._sendTrace = function(tuple) {
  var trace = tuple[0], annotations = tuple[1];

  async.waterfall([
    formatters.formatForZipkin.bind(null, trace, annotations),
    this.scribeClient.send.bind(this.scribeClient, this.category)
  ], function(err) {});
};

RawZipkinTracer.prototype.sendTraces = function(traces) {
  var callback = function() {};

  async.forEachLimit(traces, 10, this._sendTrace.bind(this),
                     callback);
};

/**
 * A tracer that records to zipkin through scribe. Requires a scribe
 * client ({node-scribe}).
 *
 * @param {scribe.Scribe object} scribeClient The client to use to write to
 *    scribe
 * @param {String} category The category to use when writing to scribe -
 *    defaults to "zipkin" if not passed
 */
function ZipkinTracer(scribeClient, category, options) {
  var rawTracer = new RawZipkinTracer(scribeClient, category);
  this._tracer = new BufferingTracer(rawTracer, options);

  this.stop = this._tracer.stop.bind(this._tracer);

  EndAnnotationTracer.call(this, this.sendTraces);
}

util.inherits(ZipkinTracer, EndAnnotationTracer);

ZipkinTracer.prototype.sendTraces = function(traces) {
  this._tracer.sendTraces(traces);
};

module.exports.BufferingTracer = BufferingTracer;
module.exports.RawZipkinTracer = RawZipkinTracer;
module.exports.ZipkinTracer = ZipkinTracer;
