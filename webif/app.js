
var API_BASE = 'http://grano.local:5000/api/1';
var API_PROJECT = 'south_africa';

var granoQuery = angular.module('granoQuery', ['ngRoute', 'ui.bootstrap']);

granoQuery.factory('schemata', function($http, $q){
  var url = API_BASE + '/projects/' + API_PROJECT + '/schemata',
      res = $http.get(url, {'params': {'limit': 1000, 'full': true}});

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
  var query = {'id': null, 'properties': {'name': null}},
      previous = angular.copy(query);
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
    path = path.replace(/^\./, '').replace(/\.$/, '')
    setTree(query, path, value);
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

  var sync = function() {
    if (!angular.equals(query, previous)) {
      update();
    }
    previous = angular.copy(query);
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
    'sync': sync,
    'query': query
  };
});

granoQuery.factory('results', function($http, $rootScope, $location, query) {
  var branches = [];

  $rootScope.$on('quiUpdateResult', function(event, data) {
    branches = query.branches();
  });

  var rows = function(results) {
    var rows = [];
    angular.forEach(results, function(row) {
      var cols = [];
      angular.forEach(branches, function(branch) {
        var path = branch[0], obj = branch[1];
        // TODO: handle multiple result in nested table
        var data = getTree(row, path)[0];
        angular.forEach(data['properties'], function(p, k) {
          cols.push({
            'name': k,
            'obj': obj,
            'data': p
          });
        });
      });
      rows.push(cols.sort(function(a, b) {
        return a.name.localeCompare(b.name);
      }));
    });
    return rows;
  };

  var headers = function() {
    var cols = [];
    angular.forEach(branches, function(branch) {
      var path = branch[0], obj = branch[1];
      angular.forEach(obj['properties'], function(p, k) {
        cols.push({
          'name': k,
          'obj': obj
        });
      });
    });
    return cols.sort(function(a, b) {
      return a.name.localeCompare(b.name);
    });
  };

  return {
    'rows': rows,
    'headers': headers
  };
});


granoQuery.controller('ResultTableCtrl', function ($scope, results) {
  $scope.rows = [];
  $scope.headers = [];

  $scope.$on('quiUpdateResult', function(event, data) {
    $scope.headers = results.headers();
    $scope.rows = results.rows(data);
  });
});


granoQuery.controller('BranchCtrl', function ($scope, query, schemata) {
  $scope.path = "";
  $scope.obj = {'schema': null};
  $scope.obj_type = null;
  $scope.schemata = [];
  $scope.attributes = [];
  $scope.visibleSchemata = [];

  $scope.setSchema = function(e) {
    $scope.obj.schema = e.name;
    $scope.obj.properties = {'name': null};
  };

  $scope.getSchemaLabel = function() {
    var label = 'Any entities';
    angular.forEach($scope.schemata, function(s) {
      if ($scope.obj.schema == s.name) {
        label = s.meta.plural_upper || s.label;
      }
    });
    return label;
  };

  $scope.columnAttributes = function () {
    var attributes = [];
    angular.forEach($scope.attributes, function(a) {
      if (a.hidden) return;
      if (angular.isDefined($scope.obj['properties']) &&
          angular.isDefined($scope.obj.properties[a.name]) &&
          ($scope.obj.properties[a.name] == null ||
          $scope.obj.properties[a.name].value == null)) {
        return;
      }
      attributes.push(a);
    });
    return attributes;
  };

  $scope.addColumn = function(attr) {
    if (!angular.isDefined($scope.obj['properties'])) {
      $scope.obj['properties'] = {};
    }
    $scope.obj.properties[attr.name] = null;
  };

  $scope.$watch('branch', function(n) {
    if (!n) return;

    $scope.path = n[0];
    $scope.obj = n[1];
    $scope.obj_type = /.*relations$/.test($scope.path) ? 'relation' : 'entity';

    schemata.get($scope.obj_type).then(function(s) {
      var visible = [{'name': null, 'label': 'Any'}],
          attributes = [];
      angular.forEach(s, function(sc) {
        if (!sc.hidden) visible.push(sc);
        if (!$scope.obj.schema || sc.name == $scope.obj.schema ||
            $scope.obj_type == 'entity' && sc.name == 'base') {

          angular.forEach(sc.attributes, function(a) {
            var at = angular.copy(a);
            at['schema'] = sc;
            attributes.push(at);
          });

        }
      });

      $scope.visibleSchemata = visible;
      $scope.schemata = s;
      $scope.attributes = attributes;
    });
  });

  $scope.$watch('obj', function(o) {
    query.set($scope.path, 'schema', o.schema);
    query.set($scope.path, 'properties', o.properties);
    query.sync();
  }, true);

});


granoQuery.controller('AppCtrl', function ($scope, $http, $q, schemata, query) {
  $scope.loading = false;
  $scope.branches = query.branches();

  $scope.$on('quiBeginLoad', function() {
    $scope.loading = true;
    $scope.branches = query.branches();
  });

  $scope.$on('quiUpdateResult', function(event, result) {
    $scope.loading = false;
    //$scope.branches = query.branches();
  });

  query.init();
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
