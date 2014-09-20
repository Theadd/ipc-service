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
  'poolMinSize': 0, //Whenever this._pool.length equals or is below this limit an 'empty' event is emitted.
  'runInterval': 0,
  'queueStackSize': 42,
  'appspace': 'appspace.',
  'id': 'id',
  'retry': 5000,
  'silent': true,
  'networkHost': 'localhost',
  'networkPort': 8000,
  'path': './'
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
  self._ipc = new require('node-ipc')
  delete require.cache[require.resolve('node-ipc')]
  self._ipc.config = util.extendObject(self._ipc.config, self._config)
  self._stats = {'sid': self._sid, 'id': self._ipc.config['id'], 'role': 'none'}
  var os = require('os')
  self._platform = os.platform()
  setInterval(function () { self._sustainStability() }, 60000)
  self._handleSafeExit()
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
    if (self._active && self._alive && self._pool.length <= self._config['poolMinSize']) {
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
  this._stats['role'] = (this._isServer) ? 'server' : (this._isClient) ? 'client' : 'none'
  return this._stats
}

/** Push items from pool to disk. Used when local pool is too big or when receiving the kill or terminate command.
 * All parameters are optional and can be specified in any order except <path> which, if specified, must be the
 * second <string> parameter.
 *
 * @param {string} [filename=config.id + ".pool"]
 * @param {string} [path=config.path]
 * @param {number} [numItems=all] If not set, all items from pool will be stored on disk and removed from pool.
 * @param {boolean} [writeSync=false]
 * @param {function} [callback]
 */
Service.prototype.save = function () {
  var fs = require('fs'), filename, path, numItems, writeSync = false, callback, data = ''
  args = Array.prototype.slice.call(arguments, 0)

  for (var key in args) {
    switch (typeof args[key]) {
      case 'function': callback = args[key]; break
      case 'number': numItems = args[key]; break
      case 'boolean': writeSync = args[key]; break
      case 'string': (filename || false) ? path = args[key] : filename = args[key]; break
    }
  }

  if (this._isServer) {
    data = this._pool.splice((numItems || this._pool.length) * -1).join("\n") + "\n"
  } else {
    data = this._localPool.splice((numItems || this._localPool.length) * -1).join("\n") + "\n"
  }
  if (data.length > 1) {
    path = path || this._config.path
    filename = filename || (this._config.id + ".pool")

    if (writeSync) {
      fs.appendFileSync(path + filename, data)
    } else {
      fs.appendFile(path + filename, data, callback)
    }
  } else if (typeof callback === "function") {
    callback()
  }
}

/** Pull items from disk to pool.
 * All parameters are optional and can be specified in any order except <path> which, if specified, must be the
 * second <string> parameter.
 *
 * @param {string} [filename=config.id + ".pool"]
 * @param {string} [path=config.path]
 * @param {number} [numItems=250]
 * @param {function} [callback]
 */
Service.prototype.restore = function () {
  var self = this, fs = require('fs'), filename, path, numItems = 250, callback, data
  args = Array.prototype.slice.call(arguments, 0)

  if (!self._isServer) {
    return console.warn("Only servers can restore items from disk!")
  }

  for (var key in args) {
    switch (typeof args[key]) {
      case 'function': callback = args[key]; break
      case 'number': numItems = args[key]; break
      case 'string': (filename || false) ? path = args[key] : filename = args[key]; break
    }
  }

  path = path || self._config.path
  filename = filename || (self._config.id + ".pool")
  callback = callback || function(err) {
    if (err) {
      console.log(err)
    }
  }

  if (fs.existsSync(path + filename)) {
    var readline = require('readline'), stream = require('stream'),
      instream = fs.createReadStream(path + filename),
      outstream = new stream

    self._rl = readline.createInterface(instream, outstream)
    self._tmp = {'line': 0, 'remaining': ''}

    self._rl.on('line', function(line) {
      if (line.length) {
        if (++self._tmp.line <= numItems) {
          self.queue(line)
        } else {
          self._tmp.remaining += line + "\n"
        }
      }
    })

    self._rl.on('close', function() {
      fs.writeFile(path + filename, self._tmp.remaining, callback)
      delete self._tmp
      delete self._rl
    })
  }
}

Service.prototype._sustainStability = function (filename, path, maxPoolSize) {
  var fs = require("fs"), stats, poolSize
  filename = filename || (this._config.id + ".pool")
  path = path || this._config.path
  maxPoolSize = maxPoolSize || 2000
  poolSize = (this._isServer) ? this._pool.length : (this._isClient) ? this._localPool.length : (maxPoolSize / 2)

  if (poolSize >= maxPoolSize) {
    this.save(filename, path, poolSize - Math.ceil(maxPoolSize / 2))
  } else if (this._isServer && poolSize <= Math.ceil(maxPoolSize / 4)) {
    this.restore(filename, path, Math.ceil(maxPoolSize / 2))
  }
}

Service.prototype._runCommand = function (command) {
  var spreadCommand = false
  command['sid'] = command['sid'] || null

  if (command['name'].length && (command['sid'] == null || command['sid'] == this._sid)) {
    switch (command['name']) {
      case 'pause': this.active(false); break
      case 'resume': this.active(true); break
      case 'alive':
      case 'start':
      case 'stop':
        this.alive((command['name'] == 'stop') ? false : (command['name'] == 'start') ? true : command['value'])
        spreadCommand = {'name': 'alive', 'value': this._alive}
        break
      case 'terminate':
      case 'kill':
        this.terminate(true)
        spreadCommand = {'name': 'alive', 'value': this._alive}
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

Service.prototype.active = function (value) {
  if (typeof value === "undefined") {
    return this._active
  } else {
    this._active = Boolean(value)
  }
}

Service.prototype.alive = function (value) {
  if (typeof value === "undefined") {
    return this._alive
  } else {
    this._alive = this._isClientConnected = Boolean(value)
  }
}

Service.prototype.terminate = function (killProcess, writeSync) {
  killProcess = killProcess || false
  writeSync = writeSync || false
  this.alive(false)
  this.active(false)
  this.save(((killProcess) ? util.killCurrentProcess : null), writeSync)
}

Service.prototype._handleSafeExit = function () {
  var self = this
  process.stdin.resume()
  process.on('exit', function () {
    arguments[String(arguments.length)] = false
    arguments[String(arguments.length + 1)] = true
    return self.terminate.apply(self, arguments)
  })
  process.on('SIGINT', function () {
    arguments[String(arguments.length)] = true
    arguments[String(arguments.length + 1)] = true
    return self.terminate.apply(self, arguments)
  })
  process.on('uncaughtException', function (err) {
    console.log(err.stack)
    arguments[String(arguments.length)] = true
    arguments[String(arguments.length + 1)] = true
    return self.terminate.apply(self, arguments)
  })
}
