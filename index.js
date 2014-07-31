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
  'queueStackSize': 42,
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
  self._alive = true
  self._config = util.extendObject(defaultConfig, config)
  self._ipc = require('node-ipc')
  self._ipc.config = util.extendObject(self._ipc.config, self._config)
  self._stats = {'sid': self._sid, 'id': self._ipc.config['id'], 'role': 'unknown'}
  var os = require('os')
  self._platform = os.platform()
}

Service.prototype.sid = function (value) {
  if (value || false) {
    self._stats['sid'] = value
    return this._sid = value
  } else {
    return this._sid
  }
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
        self.emit('empty')
      }
      self._stats['pool-size'] = self._pool.length
      ++self._stats['items-served']
      return item
    }
  } else {
    if (self._active && self._alive && self._pool.length == self._config['poolMinSize']) {
      self.emit('empty')
    }
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
    if (!self.exists(item)) {
      return (self._stats['pool-size'] = (prioritize || false) ? self._pool.unshift(item) : self._pool.push(item))
    } else return self._pool.length
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
    return (self._stats['local-pool-size'] = self._localPool.length)
  } else {
    if (!self._isClient) {
      self.client()
    }
    if (prioritize || false) {
      return (self._stats['local-pool-size'] = self._localPool.push(item))
    } else {
      if (self._stats['retry-queuing'] > self._config['queueStackSize']) {
        return (self._stats['local-pool-size'] = self._localPool.push(item))
      } else {
        ++self._stats['retry-queuing']
        setTimeout( function () { self.queue(item, false); --self._stats['retry-queuing'] }, 5000)
      }
    }
    return true
  }
}

Service.prototype.exec = function (command) {
  var self = this

  self._ipc.of[self._config.id].emit(
    'command',
    command
  )
}

/** Initialize and start IPC server. */
Service.prototype.server = function () {
  var self = this

  self._stats = util.extendObject(self._stats, {'items-served': 0, 'items-processed': 0, 'pool-size': 0, 'idle': 0})
  self._isServer = true
  console.log("Initializing " + self._config.id + " IPC server on platform " + self._platform)
  if (self._platform == 'win32') {
    self._ipc.serveNet(self._ipc.config.networkHost, self._ipc.config.networkPort, function () {
      return self._serverCallback.apply(self, arguments)
    })
  } else {
    self._ipc.serve(self._ipc.config.socketRoot + self._ipc.config.appspace + self._ipc.config.id, function () {
      return self._serverCallback.apply(self, arguments)
    })
  }

  self._ipc.server.start()
}

/** Connect client to IPC server. */
Service.prototype.client = function () {
  var self = this

  self._stats = util.extendObject(self._stats, {'retry-queuing': 0, 'local-pool-size': 0})
  self._isClient = true
  if (self._platform == 'win32') {
    self._ipc.connectToNet(self._ipc.config.id, self._ipc.config.networkHost, self._ipc.config.networkPort, function () {
      return self._clientCallback.apply(self, arguments)
    })
  } else {
    self._ipc.connectTo(self._ipc.config.id, self._ipc.config.socketRoot + self._ipc.config.appspace + self._ipc.config.id, function () {
      return self._clientCallback.apply(self, arguments)
    })
  }
}

/** IPC Callbacks */

Service.prototype._serverCallback = function () {
  var self = this

  self._ipc.server.on (
    'item',
    function (data, socket) {
      self.queue(data, false)
    }
  )
  self._ipc.server.on (
    'priorityItem',
    function (data, socket) {
      self.queue(data, true)
    }
  )
  self._ipc.server.on (
    'command',
    function (data, socket) {
      var spreadIt = self._runCommand(data)
      if (spreadIt != false) {
        self._ipc.server.broadcast(
          'command',
          spreadIt
        )
      }
    }
  )
}

Service.prototype._clientCallback = function () {
  var self = this
  self._ipc.of[self._ipc.config.id].on(
    'connect',
    function () {
      console.log("Connected to " + self._ipc.config.id + " IPC server")
      self._isClientConnected = true
    }
  )
  self._ipc.of[self._ipc.config.id].on(
    'disconnect',
    function () {
      console.log("Not connected to " + self._ipc.config.id + " IPC server")
      self._isClientConnected = false
    }
  )
  self._ipc.of[self._ipc.config.id].on(
    'command',
    function (data) {
      self._runCommand(data)
    }
  )
}

/** Start or continue processing next item in pool.
 *
 * If config('runInterval') is NOT 0 it will call this method repeatedly given this value in milliseconds.
 * Otherwise, you will have to call this method whenever required. Useful for avoiding overlaps.
 *
 * Emits a 'process' event containing the next item in pool (FIFO order by default).
 */
Service.prototype.run = function () {
  var self = this,
    item

  if ((item = self.next()) != null) {
    self.emit('process', item)
  } else {
    ++self._stats['idle']
  }
  if (self._config['runInterval']) {
    setTimeout(function () { self.run() }, self._config['runInterval'])
  }
}

Service.prototype.getStats = function () {

  this._stats['id'] = this._ipc.config['id']
  this._stats['role'] = (this._isServer) ? 'server' : (this._isClient) ? 'client' : 'unknown'
  return this._stats
}

Service.prototype._save = function (callback, path) {
  var fs = require('fs')

  data = JSON.stringify(this._pool)
  path = path || ("./" + this._config.id + ".pool")
  callback = callback || function(err) {
    if (err) {
      console.log(err)
    }
  }
  fs.writeFile(path, data, callback)
}

Service.prototype._runCommand = function (command) {
  var spreadCommand = false
  command['sid'] = command['sid'] || null

  if (command['name'].length && (command['sid'] == null || command['sid'] == this._sid)) {
    switch (command['name']) {
      case 'pause': this._active = false; break
      case 'resume': this._active = true; break
      case 'alive':
      case 'start':
      case 'stop':
        this._alive = this._isClientConnected = (command['name'] == 'stop') ? false : (command['name'] == 'start') ? true : Boolean(command['value'])
        spreadCommand = {'name': 'alive', 'value': this._alive}
        console.log("Service._alive = " + this._alive)
        break
      case 'terminate':
      case 'kill':
        this._alive = false
        spreadCommand = {'name': 'alive', 'value': this._alive}
        this._save(util.killCurrentProcess)
        break
      case 'config':
        command['value'] = command['value'] || {}
        this.config(command['value'])
        break
      case 'spread':
      case 'relay':
        spreadCommand = command['value'] || false
        break
      case 'log':
        console.log(command['value'])
        break
    }
  }

  return spreadCommand
}


/* COMMANDS:
 pause: this._active = false
 resume: this._active = true
 stop: this._alive = false && clients.broadcastState
 start: this._alive = true && clients.broadcastState
 terminate|kill: this._alive = false && clients.broadcastState && empty pool[s]
 config: {'param': value}
 'name': 'spread|relay', 'value': {'name'[, 'sid'][, 'value']}: (clients.broadcast)
 'name': 'log', 'value': 'message'

 EXAMPLE COMMAND: {'name': 'config', 'value': {'retry': 2500}}

 */
