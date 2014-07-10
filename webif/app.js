
var API_BASE = 'http://grano.local:5000/api/1';
var API_PROJECT = 'south_africa';

var granoQuery = angular.module('granoQuery', ['ngRoute']);

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


granoQuery.factory('schemata', function($http){
  var url = API_BASE + '/projects/' + API_PROJECT + '/schemata',
      res = $http.get(url, {'params': {'limit': 1000}});

  return {
    'schemata': res
  };
});


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
    $rootScope.$broadcast('quiBeginLoad');
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
    'branches': branches,
    'columns': columns
  };

});


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
    $scope.columns = query.columns();
  });
});


granoQuery.controller('BranchCtrl', function ($scope, query) {
  //console.log($scope.branch);
  $scope.obj = $scope.branch.obj;
});


granoQuery.controller('AppCtrl', function ($scope, $http, schemata, query) {
  $scope.loading = false;
  $scope.query = query;

  $scope.$on('quiBeginLoad', function() {
    $scope.loading = true;
  });
  
  $scope.$on('quiUpdateResult', function(event, result) {
    $scope.loading = false;
  });

  query.init();
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
