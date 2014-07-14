
var API_BASE = 'http://grano.local:5000/api/1';
var API_PROJECT = 'south_africa';

var granoQuery = angular.module('granoQuery', ['ngRoute']);

/*
var toQueryJson = function(data) {
  var skip = ['obj', 'path'],
      o = angular.copy(data);
  var traverse = function(e) {
    if (angular.isArray(e)) {
      angular.forEach(e, function(v, i) {
        e[i] = traverse(v);
      });
    } else if (angular.isObject(e)) {
      var out = {};
      angular.forEach(e, function(v, k) {
        if (skip.indexOf(k) == -1) {
          out[k] = traverse(v);
        }
      });
      return out;
    }
    return e;
  }
  return angular.toJson(traverse(o));
};


var Node = function(query) {
  this.query = query;
}
*/


granoQuery.factory('schemata', function($http, $q){
  var url = API_BASE + '/projects/' + API_PROJECT + '/schemata',
      res = $http.get(url, {'params': {'limit': 1000}});

  var getSchemata = function(obj) {
    var dfd = $q.defer();
    res.then(function(ts) {
      var schemata = [];
      angular.forEach(ts.data.results, function(schema) {
        if (schema.obj === obj) {
          schemata.push(schema);
        }
      });
      dfd.resolve(schemata);
    });
    return dfd.promise;
  };

  return {
    'get': getSchemata
  };
});

granoQuery.factory('query', function($http, $rootScope, $location){
  var query = {'id': null, 'properties': {'name': null},
               'relations': [{'other': {'id':  null}}]
              };

  var init = function() {
      var qs = $location.search();
      if (angular.isDefined(qs['q'])) {
        query = angular.fromJson(qs['q']);
      }
      update();
  };

  var get = function() {
    var path = Array.prototype.slice.call(arguments).join('.').split('.');
    var getRec = function(obj, path) {
      if (!angular.isArray(obj)) {
        obj = [obj];
      }
      if (!path.length) {
        return obj;
      }
      var next = path[0],
          rest = path.slice(1),
          result = [];
      angular.forEach(obj, function(o) {
        if (angular.isDefined(o[next])) {
          result = result.concat(getRec(o[next], rest));
        }
      });
      return result;
    }
    return getRec(query, path)[0];
  };

  var set = function() {
    var args = Array.prototype.slice.call(arguments),
        value = args[args.length - 1],
        path = args.slice(0, args.length -1).join('.').split('.');

    var setRec = function(obj, path) {
      if (!angular.isArray(obj)) {
        obj = [obj];
      }
      var next = path[0],
          rest = path.slice(1);
      angular.forEach(obj, function(o) {
        if (!rest.length) {
          o[next] = value;
        } else {
          if (!angular.isDefined(o[next])) {
            o[next] = {};
          }
          setRec(o[next], rest);
        }
      });
    };
    setRec(query, path);
  };

  var update = function() {
    $location.search('q', angular.toJson(query));
    var q = angular.copy(query);
    q['project'] = API_PROJECT;
    q['limit'] = 15;
    $rootScope.$broadcast('quiBeginLoad');
    var params = {'query': angular.toJson([q])};
    var res = $http.get(API_BASE + '/query', {'params': params});
    res.then(function(rd) {
      result = rd.data.result;
      $rootScope.$broadcast('quiUpdateResult', result);
    });
  };

  return {
    'init': init,
    'get': get,
    'set': set,
    'update': update,
    'query': query
  };
});

granoQuery.factory('results', function($http, $rootScope, $location, query) {
  var results = [];

  var flatten = function(obj, prefix) {
    // shouldn't this go into query?
    if (!angular.isArray(obj)) {
      obj = [obj];
    }

    var ret = [];
    angular.forEach(obj, function(o) {
      ret.push([prefix, o]);
      var keys = ['relations', 'other'];
      angular.forEach(keys, function(k) {
        if (angular.isDefined(o[k])) {
          var sub_prefix = prefix.length ? prefix + '.' + k : k;
          ret = ret.concat(flatten(o[k], sub_prefix));
        }
      });
    });
    return ret;
  };

  $rootScope.$on('quiUpdateResult', function(event, res) {
    results = res;
    var flats = flatten(query.query, '');
    console.log(flats);
  });

  return {

  };
});

/*
granoQuery.factory('query', function($http, $rootScope, $location){

  var query = {'id': null, 'properties': {'name': null}};

  var init = function() {
      var qs = $location.search();
      if (angular.isDefined(qs['q'])) {
        query = angular.fromJson(qs['q']);
      }
      run();
  };

  var run = function() {
    // Run the request against the server.
    $location.search('q', angular.toJson(query));
    var q = angular.copy(query);
    q['project'] = API_PROJECT;
    q['limit'] = 15;
    $rootScope.$broadcast('quiBeginLoad');
    console.log(toQueryJson(q));
    var params = {'query': toQueryJson([q])};
    var res = $http.get(API_BASE + '/query', {'params': params});
    res.then(function(rd) {
      result = rd.data.result;
      $rootScope.$broadcast('quiUpdateResult', result);
    });
  };

  var branches = function() {
    // All the children that are supposed to be visible
    // as sidebar widgets.
    var rec = function(el, obj, path) {
      if (angular.isArray(el)) {
        el = el[0];
      }
      el['obj'] = obj;
      el['path'] = path;
      var branches = [el],
          obj_next = obj === 'entity' ? 'relation' : 'entity';
      if (angular.isDefined(el['relations'])) {
        var path_next = path.concat(['relations']);
        branches = branches.concat(rec(el['relations'],
                                       obj_next,
                                       path_next));
      }
      if (angular.isDefined(el['other'])) {
        var path_next = path.concat(['other']);
        branches = branches.concat(rec(el['other'],
                                       obj_next,
                                       path_next));
      }
      return branches;
    };
    return rec(query, 'entity', []);
  };

  var columns = function() {
    var cols = [];
    angular.forEach(branches(), function(branch) {
      if (angular.isObject(branch['properties'])) {
        angular.forEach(branch['properties'], function (v, k) {
          if (v === null || (angular.isObject(v) && v['value'] === null)) {
            cols.push({'name': k, 'branch': branch});
          }
        });
      }
    });
    return cols;
  };

  return {
    'run': run,
    'init': init,
    'update': run,
    'branches': branches,
    'columns': columns
  };

});
*/

granoQuery.controller('ResultTableCtrl', function ($scope, query) {
  $scope.results = [];

  $scope.columnValue = function(column, result) {
    var rec = function(o, path) {
      if (!path.length) {
        return o;
      }
      return rec(o[path[0]] || {}, path.slice(1));
    };
    var obj = rec(result, column.branch.path);
    return obj.properties[column.name].value;
  };

  $scope.$on('quiUpdateResult', function(event, results) {
    $scope.results = results;
    //$scope.columns = query.columns();
  });
});


granoQuery.controller('BranchCtrl', function ($scope, query, schemata) {
  $scope.schemata = [];
  $scope.visibleSchemata = [];
  $scope.schema = null;

  $scope.$watch('schema', function(n) {
    var schema = !n || !n.length ? null : n;
    if ($scope.branch.obj === 'entity') {
      query.schemata = schema;
    } else {
      query.schema = schema;
    }
    //console.log(angular.toJson($scope.branch));
  });

  $scope.$watch('branch', function(n) {
    //console.log(angular.toJson(n));
    if (angular.isDefined(query['schemata'])) {
      $scope.schema = query.schemata;
    } else if (angular.isDefined(query['schema'])) {
      $scope.schema = query.schema;
    }
  });

  schemata.get($scope.branch.obj).then(function(s) {
    var visible = [];
    angular.forEach(s, function(sc) {
      if (!sc.hidden) visible.push(sc);
    });
    $scope.visibleSchemata = visible;
    $scope.schemata = s;
  });

});


granoQuery.controller('AppCtrl', function ($scope, $http, $q, schemata, query, results) {
  $scope.loading = false;
  $scope.results = results;
  //$scope.query = query;

  $scope.$on('quiBeginLoad', function() {
    $scope.loading = true;
  });
  
  $scope.$on('quiUpdateResult', function(event, result) {
    $scope.loading = false;
  });

  
  query.init();
  //var x = queryNEW.get('properties');
  //console.log(x);
  //queryNEW.set('properties.national_id', 'value', '4493984989');
  //queryNEW.set('relations', [{'id': null}]);
  //queryNEW.set('relations.other', {'id': null});
  console.log(query.query);
});


granoQuery.config(['$routeProvider', function($routeProvider) {
  $routeProvider.
    when('/', {
      controller: 'AppCtrl',
      templateUrl: 'query.html'
    }).
    otherwise({
      redirectTo: '/'
    });
}]);
