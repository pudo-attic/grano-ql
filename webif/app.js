
var API_BASE = 'http://grano.local:5000/api/1';
var API_PROJECT = 'south_africa';

var granoQuery = angular.module('granoQuery', ['ngRoute']);

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
  var query = {'id': null, 'properties': {'name': null}};
  var branches = [];

  var init = function() {
      var qs = $location.search();
      if (angular.isDefined(qs['q'])) {
        query = angular.fromJson(qs['q']);
      }
      update();
  };

  var get = function() {
    var path = Array.prototype.slice.call(arguments).join('.');
    return getTree(query, path)[0];
  };

  var set = function() {
    var args = Array.prototype.slice.call(arguments),
        value = args[args.length - 1],
        path = args.slice(0, args.length -1).join('.');
    return setTree(query, path, value);
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
      $rootScope.$broadcast('quiUpdateResult', rd.data.result);
    });
  };

  var flatten = function(obj, prefix) {
    var ret = [];
    obj = ensureArray(obj);
    angular.forEach(obj, function(o) {
      ret.push([prefix, o]);
      var keys = ['relations', 'other'];
      angular.forEach(keys, function(k) {
        if (angular.isDefined(o[k])) {
          var sub_prefix = prefix.length ? prefix + '.' + k : k;
          console.log(sub_prefix);
          ret = ret.concat(flatten(o[k], sub_prefix));
        }
      });
    });
    return ret;
  };

  var branches = function() {
    return flatten(query, '');
  };

  return {
    'init': init,
    'get': get,
    'set': set,
    'branches': branches,
    'update': update,
    'query': query
  };
});

granoQuery.factory('results', function($http, $rootScope, $location, query) {

  var rows = function(results) {
    var rows = [];
    angular.forEach(results, function(row) {
      var cols = [];
      angular.forEach(query.branches(), function(branch) {
        var path = branch[0], obj = branch[1];
        // TODO: handle multiple result in nested table
        var data = getTree(row, path)[0];
        //if (!angular.isDefined(data['properties'])) return;
        angular.forEach(data['properties'], function(p, k) {
          cols.push({
            'name': k,
            'obj': obj,
            'data': p
          });
        });
      });
      rows.push(cols);
    });
    return rows;
  };

  var headers = function() {
    var cols = [];
    angular.forEach(query.branches(), function(branch) {
      var path = branch[0], obj = branch[1];
      angular.forEach(obj['properties'], function(p, k) {
        cols.push({
          'name': k,
          'obj': obj
        });
      });
    });
    return cols;
  }

  return {
    'rows': rows,
    'headers': headers
  };
});


granoQuery.controller('ResultTableCtrl', function ($scope, results) {
  $scope.rows = [];
  $scope.headers = []

  $scope.$on('quiUpdateResult', function(event, data) {
    $scope.headers = results.headers();
    $scope.rows = results.rows(data);
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
  });

  $scope.$watch('branch', function(n) {
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


granoQuery.controller('AppCtrl', function ($scope, $http, $q, schemata, query) {
  $scope.loading = false;

  $scope.$on('quiBeginLoad', function() {
    $scope.loading = true;
  });

  $scope.$on('quiUpdateResult', function(event, result) {
    $scope.loading = false;
  });

  query.init();
  //console.log(query.query);
  query.update();
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
