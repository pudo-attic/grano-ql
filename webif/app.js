
var API = 'http://grano.local:5000/api/1/projects/opennews2';

var granoQuery = angular.module('granoQuery', ['ngRoute', 'ui.bootstrap']);

function makeId() {
    var S4 = function() {
       return (((1+Math.random())*0x10000)|0).toString(16).substring(1);
    };
    return (S4()+S4()+S4());
}

granoQuery.factory('schemata', function($http, $q){
  var url = API + '/schemata',
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
    'get': getSchemata,
    'all': res
  };
});


granoQuery.factory('queryState', function($http, $rootScope, $location){
  var objects = [{'id': 'root', 'type': 'entity',
                  'fields': {'properties': [{'name': 'name', 'schema': 'base'}]},
                  'filters': {'properties': []}}],
      previous = null;

  var init = function() {
      var qs = $location.search();
      if (angular.isDefined(qs['o'])) {
        // can't simply assign because we can't replace 'objects'
        objects.splice(0);
        angular.forEach(angular.fromJson(qs['o']), function(e) {
          objects.push(e);
        });
      }
      sync();
  };

  var sync = function() {
    if (!angular.equals(objects, previous)) {
      $location.search('o', angular.toJson(objects));
      update();
    }
    previous = angular.copy(objects);
  };

  var update = function() {
    var q = makeQuery();
    q['limit'] = 15;
    $rootScope.$broadcast('querySend');
    var params = {'query': angular.toJson([q])};
    var res = $http.get(API + '/query', {'params': params});
    res.then(function(rd) {
      $rootScope.$broadcast('queryUpdate', rd.data.results);
    });
  };

  var by_id = function(id) {
    var obj = null;
    angular.forEach(objects, function(o) {
      if (o.id == id) obj = o;
    });
    return obj;
  };

  var makeQuery = function() {
    var root = by_id('root');
    var transformObj = function (obj) {
      var q = {'id': null, 'properties': {}};

      // define fields
      angular.forEach(obj.fields['properties'], function(o) {
        q['properties'][o.name] = null;
      });

      // define filters
      angular.forEach(obj.filters['properties'], function(o) {
        q['properties'][o.name] = {'value': o.value};
      });

      if (obj.filters['schema']) {
        var key = obj.type == 'entity' ? 'schemata' : 'schema';
        q[key] = obj.filters['schema'];
      }

      angular.forEach(objects, function(c) {
        if (c['parent'] !== obj.id) return;
        var key = obj.type == 'entity' ? 'relations' : 'other';
        q[key] = transformObj(c);
      });
      return q;
    };
    return transformObj(root);
  };

  var add = function(object) {
    object['id'] = makeId();
    object['parent'] = objects[objects.length-1].id;
    objects.push(object);
  };

  var fields = function() {
    var fields = [];
    angular.forEach(objects, function(object) {
      angular.forEach(object.fields.properties, function(p) {
        var field = angular.copy(p);
        field['type'] = object.type;
        field['id'] = object.id;
        field['get'] = makeGetter(object);
        fields.push(field);
      });
    });
    return fields;
  };

  var makeGetter = function(obj) {
    return function(data) {
      if (!obj['parent']) {
        return data;
      };
      var parent = null;
      angular.forEach(objects, function(o) {
        if (o.id == obj.parent) parent = o;
      });
      var parent_data = makeGetter(parent)(data),
          parent_key = parent.type == 'entity' ? 'relations' : 'other';
      data = parent_data[parent_key];
      // TODO: handle multi-row results
      if (angular.isArray(data)) data = data[0];
      return data;
    };
  };

  return {
    'objects': objects,
    'init': init,
    'add': add,
    'sync': sync,
    'by_id': by_id,
    'fields': fields
  };
});


granoQuery.controller('ResultTableCtrl', function ($scope, schemata, queryState) {
  $scope.rows = [];
  $scope.fields = [];

  $scope.removeColumn = function(field) {
    var obj = queryState.by_id(field.id);
    angular.forEach(obj.fields.properties, function(p, i) {
      if (p.name == field.name)
        obj.fields.properties.splice(i, 1);
    });
    queryState.sync();
  };

  $scope.$on('queryUpdate', function(event, data) {
    $scope.fields = queryState.fields();
    var rows = [];
    angular.forEach(data, function(row) {
      var cells = [];
      angular.forEach($scope.fields, function(field) {
        var obj = field.get(row),
            prop = obj.properties[field.name] || {};
        cells.push({
          'name': field.name,
          'type': field.type,
          'id': obj.id,
          'schema': field.schame,
          'value': prop['value']
        });
      });
      rows.push(cells);
    });
    $scope.rows = rows;
  });

});


granoQuery.controller('QueryObjectCtrl', function ($scope, queryState, schemata) {
  $scope.schemata = [];
  $scope.attributes = [];
  $scope.visibleSchemata = [];


  $scope.setSchema = function(e) {
    $scope.object.filters['schema'] = e.name;
  };

  $scope.removeLayer = function() {
    var idx = queryState.objects.indexOf($scope.object);
    queryState.objects.splice(idx);
  };

  $scope.getSchemaLabel = function() {
    var label = $scope.anyLabel();
    angular.forEach($scope.schemata, function(s) {
      if ($scope.object.filters['schema'] == s.name) {
        label = s.meta.plural_upper || s.label;
      }
    });
    return label;
  };

  $scope.anyLabel = function() {
    if ($scope.object.type == 'relation') return 'Any relation type';
    return 'Any entities';
  };

  $scope.actionLabel = function() {
    if ($scope.object.id == 'root') return 'Find';
    if ($scope.object.type == 'relation') return 'connected via';
    return 'to';
  };

  $scope.availableFields = function () {
    var attributes = [];
    angular.forEach($scope.attributes, function(a) {
      if (a.hidden) return;
      var taken = false;
      angular.forEach($scope.object.fields.properties, function(p) {
        if (p.name == a.name && p.schema == a.schema.name) taken = true;
      });
      if (!taken) attributes.push(a);
    });
    return attributes;
  };

  $scope.availableFilters = function () {
    var attributes = [];
    angular.forEach($scope.attributes, function(a) {
      if (a.hidden) return;
      var taken = false;
      angular.forEach($scope.object.filters.properties, function(p) {
        if (p.name == a.name && p.schema == a.schema.name) taken = true;
      });
      if (!taken) attributes.push(a);
    });
    return attributes;
  };

  $scope.addField = function(attr) {
    $scope.object.fields.properties.push({
      'name': attr.name,
      'schema': attr.schema.name
    });
  };

  $scope.addFilter = function(attr) {
    $scope.object.filters.properties.push({
      'name': attr.name,
      'schema': attr.schema.name,
      'value': ''
    });
  };

  $scope.$watch('object', function(obj) {
    if (!obj) return;

    schemata.get(obj.type).then(function(s) {
      var visible = [{'name': null, 'label': $scope.anyLabel()}],
          attributes = [];

      angular.forEach(s, function(sc) {
        if (!sc.hidden) visible.push(sc);
        if (sc.name == obj.filters['schema'] ||
            obj.type == 'entity' && sc.name == 'base') {

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

  $scope.$watch('object', function(o) {
    queryState.sync();
  }, true);

});


granoQuery.controller('QueryCtrl', function ($scope, $http, $q, schemata, queryState) {
  var allSchemata = [];
  schemata.all.then(function(schemata) {
    allSchemata = schemata.data.results;
  });

  $scope.loading = false;
  $scope.objects = queryState.objects;

  $scope.$on('querySend', function() {
    $scope.loading = true;
  });

  $scope.$on('queryUpdate', function(event, result) {
    $scope.loading = false;
  });

  $scope.getFieldAttribute = function(field, type) {
    var attribute = {'label': field.name};
    field.type = field.type || type;
    angular.forEach(allSchemata, function(s) {
      if (s.obj == field.type && s.name == field.schema) {
        angular.forEach(s.attributes, function(a) {
          if (a.name == field.name) attribute = a;
        });
      }
    });
    return attribute;
  };

  $scope.addLayers = function() {
    var lastObject = queryState.objects[queryState.objects.length-1];
    if (lastObject.type == 'entity') {
      queryState.add({'type': 'relation',
                      'fields': {'properties': []},
                      'filters': {'properties': []}});
    }
    queryState.add({'type': 'entity',
                    'fields': {'properties': [{'schema': 'base', 'name': 'name'}]},
                    'filters': {'properties': []}});
    queryState.sync();
  };

  queryState.init();
  queryState.sync();
});


granoQuery.config(['$routeProvider', function($routeProvider) {
  $routeProvider.
    when('/', {
      controller: 'QueryCtrl',
      templateUrl: 'query.html'
    }).
    otherwise({
      redirectTo: '/'
    });
}]);
