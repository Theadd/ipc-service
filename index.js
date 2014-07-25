/**
 * Created by Theadd on 24/7/14.
 */

exports.Service = Service

var inherits = require('inherits')
var EventEmitter = require('events').EventEmitter
var util = require('./lib/util')

inherits(Service, EventEmitter)

var defaultConfig = {
  'recentPoolMaxSize': 250,
  'poolMinSize': 0, //Whenever this._pool.length reaches this limit an 'empty' event is emitted.
  'runInterval': 0,
  'appspace': 'appspace.',
  'id': 'id',
  'retry': 5000,
  'silent': true,
  'networkHost': 'localhost',
  'networkPort': 8000
}

function Service (config, sid) {
  var self = this
  EventEmitter.call(self)
  self._sid = sid || util.getRandomString(8)
  self._pool = []
  self._localPool = []
  self._recentPool = []
  self._isClient = false
  self._isClientConnected = false
  self._isServer = false
  self._active = true
  self._config = util.extendObject(defaultConfig, config)
  self._ipc = require('node-ipc')
  self._ipc.config = util.extendObject(self._ipc.config, self._config)
  var os = require('os')
  self._platform = os.platform()
}

Service.prototype.config = function (param, value) {
  param = param || false
  value = value || null

  if (!param) {
    return this._config
  }
  if (typeof param === 'object') {
    this._config = util.extendObject(this._config, param)
    this._ipc.config = util.extendObject(this._ipc.config, this._config)
    return this._config
  } else {
    if (value != null) {
      this._config[param] = value
      return this._ipc.config[param] = value
    } else {
      return this._config[param]
    }
  }
}

/** Returns consecutive item in queue (FIFO). Optionally, preserving that item on the queue. False by default.
 *
 * @type {Function}
 */
Service.prototype.next = function (preserve) {
  var self = this
  preserve = preserve || false
  if (self._active && self._pool.length) {
    if (preserve) {
      return self._pool[0]
    } else {
      var item = self._pool.shift()
      if (self._recentPool.push(item) > this._config['recentPoolMaxSize']) {
        self._recentPool.splice(0, 25)
      }
      if (self._pool.length == self._config['poolMinSize']) {
        console.log("(self._pool.length == self._config['poolMinSize'])")
        self.emit('empty')
      }
      return item
    }
  } else {
    return null
  }
}

/** Whether this item is already in the queue or has been processed recently.
 *
 * @type {Function}
 */
Service.prototype.exists = function (item) {
  return (this._pool.indexOf(item) != -1 || this._recentPool.indexOf(item) != -1)
}

Service.prototype.queue = function (item, prioritize) {
  var self = this
  if (self._isServer) {
    return (prioritize || false) ? self._pool.unshift(item) : self._pool.push(item)
  } else if (self._isClient && self._isClientConnected) {
    prioritize = prioritize || false;
    (prioritize) ? self._localPool.unshift(item) : self._localPool.push(item)
    if (prioritize && self._localPool.length == 1) {
      self._ipc.of[self._config.id].emit(
        'priorityItem',
        self._localPool.pop()
      )
    } else {
      while (self._localPool.length) {
        self._ipc.of[self._config.id].emit(
          'item',
          self._localPool.shift()
        )
      }
    }
    return true
  } else {
    if (!self._isClient) self.client()
    setTimeout( function () { self.queue(item, (prioritize || false))}, 100)
    return true
  }
}

/** Initialize and start IPC server. */
Service.prototype.server = function () {
  this._isServer = true
  console.log("Initializing " + this._config.id + " IPC server on platform " + this._platform);
  ((this._platform == 'win32') ? this._ipc.serveNet(this._ipc.config.networkHost, this._ipc.config.networkPort, this._serverCallback) : this._ipc.serve(this._ipc.config.socketRoot + this._ipc.config.appspace + this._ipc.config.id, this._serverCallback))

  this._ipc.server.start()
}

/**
 * Connect client to IPC server
 */
Service.prototype.client = function () {
  this._isClient = true;
  (this._platform == 'win32') ? this._ipc.connectToNet(this._ipc.config.id, this._ipc.config.networkHost, this._ipc.config.networkPort, this._clientCallback) : this._ipc.connectTo(this._ipc.config.id, this._ipc.config.socketRoot + this._ipc.config.appspace + this._ipc.config.id, this._clientCallback)
}

/** IPC Callbacks */

Service.prototype._serverCallback = function () {
  var self = this
  ipc.server.on (
    'item',
    function (data, socket) {
      self.queue(data, false)
    }
  )
  ipc.server.on (
    'priorityItem',
    function (data, socket) {
      self.queue(data, true)
    }
  )
}

Service.prototype._clientCallback = function() {
  var self = this
  ipc.of[self._config.id].on(
    'connect',
    function(){
      console.log("Connected to " + self._config.id + " IPC server")
      self._isClientConnected = true
    }
  )
  ipc.of[self._config.id].on(
    'disconnect',
    function(){
      console.log('Not connected to " + this._config.id + " IPC server')
      self._isClientConnected = false
    }
  )
}

Service.prototype.run = function () {
  var self = this,
    item

  if ((item = self.next()) != null) {
    self.emit('process', item)
  }
  if (self._config['runInterval']) {
    setTimeout(self.run(), self._config['runInterval'])
  }
}
