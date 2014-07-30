/**
 * Created by Theadd on 7/25/14.
 */

var extend = require('util')._extend

var getRandomString = exports.getRandomString = function (len) {
  return (Math.random().toString(36)+'00000000000000000').slice(2, len+2)
}

var extendObject = exports.extendObject = function (primary, secondary) {
  secondary = secondary || null
  var o = extend({}, primary)
  if (secondary != null) {
    extend(o, secondary)
  }
  return o
}

var killCurrentProcess = exports.killCurrentProcess = function (delay) {
  delay = delay || 1000
  console.log("killCurrentProcessWithDelay("+delay+")"+(new Date().getTime()))
  setTimeout(function () {
    console.log("EXIT!"+(new Date().getTime()))
    process.exit(code=0)
  }, delay)
}

