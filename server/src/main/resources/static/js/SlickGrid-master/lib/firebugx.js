if (typeof console === "undefined" || typeof console.log === "undefined") {
  var names = ["log", "debug", "info", "warn", "error", "assert", "dir", "dirxml",
      "group", "groupEnd", "time", "timeEnd", "count", "trace", "profile", "profileEnd"];

  window.console = {};
  for (var i = 0; i < names.length; ++i) {
    window.console[names[i]] = function() {}
  }
}