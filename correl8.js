var elasticsearch = require('elasticsearch');
var client = new elasticsearch.Client({
  host: 'localhost:9200',
  log: 'info'
});

var correl8 = function(doctype, basename) {
  this.INDEX_BASE = basename || 'correl8';
  this._type = doctype;
  this._index = this.INDEX_BASE + '-' + this._type;
  this.configIndex = INDEX_BASE + '-config';
  this.configType = 'config-' + this._type;

  // set range for numeric timestamp guessing
  this.SECONDS_PAST = 60 * 60 * 24 * 365 * 10; // about 10 years in the past
  this.SECONDS_FUTURE = 60 * 60 * 24; // 24 hours in the future
  var self = this;

  // config index created automatically if it doesn't exist already
  // this should be blocking code!
  client.indices.exists({index: configIndex}).then(function(res) {
    if (!res) {
      client.indices.create({index: configIndex}).then(function() {
        console.log('Created config index');
      });
    }
  });

  this.index = function(newName) {
    if (newName) {
      self._index = self.INDEX_BASE + '-' + newName;
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
      return client.search(params).then(function(response) {
        // console.log(response.hits.hits[0]._source);
        if (response && response.hits && response.hits.hits && response.hits.hits[0] && response.hits.hits[0]._source && response.hits.hits[0]._source.id) {
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
    params.q = 'id:' + self.configType;
    params.body = {size: 1};
    return client.search(params);
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
      });
    });
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

  this.insert = function(object) {
    var ts = new Date();
    if (object && object.timestamp) {
      // timestamp is a string that can be parsed by Date.parse
      if (!isNaN(Date.parse(object.timestamp))) {
        ts = new Date(object.timestamp);
      }
      // timestamp is milliseconds within valid range
      else if ((object.timestamp >= (ts.getTime() - SECONDS_PAST * 1000)) &&
               (object.timestamp <= (ts.getTime() - SECONDS_FUTURE * 1000))) {
        ts.setTime(object.timestamp);
      }
      // timestamp is seconds within valid range
      else if ((object.timestamp >= (ts.getTime() / 1000 - SECONDS_PAST)) &&
               (object.timestamp <= (ts.getTime() / 1000 - SECONDS_FUTURE))) {
        ts.setTime(object.timestamp * 1000);
      }
    }
    object.timestamp = ts;
    var monthIndex = self._index + '-' + ts.getFullYear() + '-' + (ts.getMonth() + 1);
    return client.indices.exists({index: self._index}).then(function() {
      console.log('Index exists!');
      return client.index({
        index: self._index,
        type: self._type,
        body: {doc: object, doc_as_upsert: true}
      });
    }).then(function() {
      console.log('Indexed document!');
      return client.indices.create({index: monthIndex});
    }).then(function() {
      console.log('Created monthIndex!');
      return client.indices.getMapping({index: self._index})
    }).then(function(mapping) {
      console.log('Fetched index mapping!');
      mapping.index = self._index;
      return client.indices.putMapping(mapping);
    }).then(function() {
      console.log('Created monthIndex mapping!');
    }).then(function() {
      return client.index({
        index: monthIndex,
        type: self._type,
        body: {doc: object, doc_as_upsert: true}
      });
    }).then(function() {
      console.log('Indexed document into month index!');
    }).catch(function(error) {
      console.trace(error);
    });
  };

  this.bulk = function(bulk) {
    return client.bulk({index: self._index, type: self._type, body: bulk});
  };

  this.search = function(params) {
    return client.search({index: self._index, type: self._type, body: params});
  }

  this.release = function() {
    // the process will hang for some time unless the Elasticsearch connection is closed
    client.close();
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
