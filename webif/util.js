
var ensureArray = function(obj) {
    if (!angular.isArray(obj)) {
      return [obj];
    }
    return obj;
};

var getTree = function(obj, path) {
    var paths = path.split('.'),
        next = paths[0],
        rest = paths.slice(1).join('.'),
        result = [];
    obj = ensureArray(obj);
    if (next === '') {
      return obj;
    }
    angular.forEach(obj, function(o) {
      if (angular.isDefined(o[next])) {
        result = result.concat(getTree(o[next], rest));
      }
    });
    return result;
};

var setTree = function(obj, path, value) {
  var paths = path.split('.'),
      next = paths[0],
      rest = paths.slice(1).join('.');
  obj = ensureArray(obj);
  angular.forEach(obj, function(o) {
    if (!rest.length) {
      o[next] = value;
    } else {
      if (!angular.isDefined(o[next])) {
        o[next] = {};
      }
      setTree(o[next], rest);
    }
  });
};

