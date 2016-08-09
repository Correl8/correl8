var elasticsearch = require('elasticsearch');
var username = 'elastic';
var password = 'changeme'
var host = 'localhost';
var protocol = 'http';
var port = 9200;
var client = new elasticsearch.Client({
  log: 'info',
  host: [{
    protocol: protocol,
    host: host,
    port: port,
    auth: username + ':' + password
  }],
  apiVersion: 'master'
});

// set range for numeric timestamp guessing
var SECONDS_PAST = 60 * 60 * 24 * 365 * 10; // about 10 years in the past
var SECONDS_FUTURE = 60 * 60 * 24; // 24 hours in the future

var correl8 = function(doctype, basename) {
  this.INDEX_BASE = basename || 'correl8';
  this._type = doctype;
  this._index = (this.INDEX_BASE + '-' + this._type).toLowerCase();
  this.configIndex = INDEX_BASE.toLowerCase() + '-config';
  this.configType = 'config-' + this._type;

  var self = this;

  // config index created automatically if it doesn't exist already
  // should be blocking
  client.indices.exists({index: configIndex}).then(function(res) {
    if (!res) {
      client.indices.create({index: configIndex}).then(function() {
        // console.log('Created config index');
      }).catch(function() {
        console.warn('Could not created config index!');
      });
    }
  }).catch(function(error) {
    console.trace(error);
  });

  this.index = function(newName) {
    if (newName) {
      self._index = self.INDEX_BASE + '-' + newName.toLowerCase();
    }
    return self;
  }

  this.type = function(newName) {
    if (newName) {
      self._type = newName;
      self._index = self.INDEX_BASE + '-' + newName;
    }
    return self;
  }

  this.config = function(object) {
    var params = {
      index: self.configIndex,
      type: self.configType,
      id: self.configType
    };
    var searchParams = params;
    searchParams.q = 'id:' + self.configType;
    searchParams.body = {size: 1};
    if (object) {
      return client.search(searchParams).then(function(response) {
        var obj = this.trimResults(response);
        if (obj && obj.id) {
          params.id = self.configType;
          params.body = {doc: object};
          return client.update(params);
        }
        else {
          object.id = self.configType;
          params.body = object;
          return client.index(params);
        }
      });
    }
    return client.search(searchParams);
  }

  this.init = function(object) {
    var properties = createMapping(object)
    properties.timestamp = {type: 'date', format: 'strict_date_optional_time||epoch_millis'};
    return client.index({
      index: self._index,
      type: self._type,
      body: {}
    }).then(function() {
      client.indices.putMapping({
        index: self._index,
        type: self._type,
        body: {properties: properties}
      }).then(function() {
        // console.log(self._index + " initialized");
      });
    });
  };

  this.isInitialized = function() {
    return client.indices.exists({index: self._index});
  };

  this.clear = function() {
    return client.indices.getMapping({index: self._index}).then(function(map) {
      // console.log('Fetched index map');
      return client.indices.delete({index: self._index}).then(function() {
        // console.log('Deleted index');
        return client.indices.create({index: self._index});
      }).then(function() {
        var mappings = map[self._index].mappings;
        if (mappings && mappings[self._type] && mappings[self._type].properties) {
          return client.indices.putMapping({
            index: self._index,
            type: self._type,
            body: {properties: mappings[self._type].properties}
          });
        }
        else {
          console.warn('No existing mapping, set mapping with init');
        }
      });
    });
  };

  this.guessTime = function(obj) {
    var ts;
    if (!obj) {
      return;
    }
    // timestamp is a string that can be parsed by Date.parse
    if (!isNaN(Date.parse(obj))) {
      ts = new Date(obj);
    }
    // timestamp is milliseconds within valid range
    else if ((obj >= (ts.getTime() - SECONDS_PAST * 1000)) &&
             (obj <= (ts.getTime() - SECONDS_FUTURE * 1000))) {
      ts.setTime(obj);
    }
    // timestamp is seconds within valid range
    else if ((obj >= (ts.getTime() / 1000 - SECONDS_PAST)) &&
             (obj <= (ts.getTime() / 1000 - SECONDS_FUTURE))) {
      ts.setTime(obj * 1000);
    }
    return ts;
  }

  this.insert = function(object) {
    var ts = new Date();
    var guessed;
    if (object && object.timestamp) {
      if (guessed = guessTime(object.timestamp)) {
        ts = guessed;
      }
      else {
        var msg = 'Could not parse timestamp ' + object.timestamp + '!' +
          ' Overriding with ' + ts + '.';
        console.warn(msg);
      }
    }
    object.timestamp = ts;
    return client.indices.exists({index: self._index}).then(function() {
      // console.log('Index exists!');
      var params = {
        index: self._index,
        type: self._type,
        body: object
      };
      if (object.id) {
        params.id = object.id;
      }
      return client.index(params);
    });
  };

  this.bulk = function(bulk) {
    return client.bulk({index: self._index, type: self._type, body: bulk});
  };

  this.deleteOne = function(id) {
    return client.delete({index: self._index, type: self._type, id: id});
  };

  this.deleteMany = function(params) {
    return this.search(params).then(function(results) {
      // console.log(results);
      var bulk = [];
      for (var i=0; i<results.lenght; i++) {
        var id = results[i]._id;
        bulk.push({delete: {index: this._index, type: this._type, id: id}});
      }
      // console.log(bulk);
      this.bulk(bulk);
    })
  };

  this.remove = function() {
    return client.indices.delete({index: this._index}).then(function(result) {
      return client.indices.delete({index: this.configIndex});
    });
  }

  this.search = function(params) {
    return client.search({index: self._index, type: self._type, body: params});
  }

  this.msearch = function(params) {
    var searchParams = [];
    for (var i=0; i<params.length; i++) {
      searchParams[2*i] = {index: self._index, type: self._type};
      searchParams[2*i+1] = params[i];
    }
    return client.msearch({index: self._index, type: self._type, body: searchParams});
  }

  this.release = function() {
    // the process will hang for some time unless the Elasticsearch connection is closed
    return client.close();
  }

  this.trimResults = function(r) {
    if (r && r.hits && r.hits.hits && r.hits.hits[0] && r.hits.hits[0]._source) {
      return r.hits.hits[0]._source;
    }
    return false;
  }

  return this;

}

function createMapping(object) {
  var properties = {};
  for (var prop in object) {
    if (typeof(object[prop]) === 'object') {
      // recurse into sub objects
      properties[prop] = {type: 'object', properties: createMapping(object[prop])};
    }
    else {
      // doc_values: store values to disk (reduce heap size)
      properties[prop] = {type: object[prop], doc_values: true};
      // use propietary "text" type for analyzed strings
      if (object[prop] === 'text') {
        properties[prop].type = 'string';
        // doc_values do not currently work with analyzed string fields
        properties[prop].doc_values = false;
      }
      // other strings are not analyzed
      else if (object[prop] === 'string') {
        properties[prop].index = 'not_analyzed';
      }
    }
  }
  return properties;
}

module.exports = correl8;
