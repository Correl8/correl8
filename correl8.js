const { Client } = require('@elastic/elasticsearch')

// set range for numeric timestamp guessing
const SECONDS_PAST = 60 * 60 * 24 * 365.25 * 10; // about 10 years in the past
const SECONDS_FUTURE = 60 * 60 * 24; // 24 hours in the future

const correl8 = function(doctype, basename, clientOpts) {
  clientOpts = clientOpts || {};
  clientOpts.host = clientOpts.host || [{
    host: 'localhost',
    protocol: 'http',
    port: 9200,
    auth: 'elastic:changeme'
  }];

  let self = this;
  this.INDEX_BASE = basename || 'correl8-elastic';
  this._type = doctype;
  this._index = (self.INDEX_BASE + '-' + this._type).toLowerCase();
  this.configIndex = self._index + '-config';
  this.configType = self.configIndex; // still required for 6.0 alpha

  this.getClientOpts = () => {
    return JSON.parse(JSON.stringify(clientOpts));
  };
  
  // console.log(clientOpts);
  // .Client() must get a fresh copy of opts - using a function!
  let client = new Client(self.getClientOpts());

  // config index created automatically if it doesn't exist already
  // should be blocking
  client.indices.exists({index: self.configIndex}).then((res) => {
    if (!res) {
      return client.index({
        index: self.configIndex,
        type: self.configType,
        body: {}
      }).then(() => {
        // console.log('Created config index');
      }).catch((error) => {
        console.warn('Could not create config index! ' + error);
      });
    }
  });

  this.index = (newName) => {
    if (newName) {
      self._index = self.INDEX_BASE + '-' + newName.toLowerCase();
    }
    return self;
  };

  this.type = (newName) => {
    if (newName) {
      self._type = newName;
      self._index = self.INDEX_BASE + '-' + newName.toLowerCase();
    }
    return self;
  };

  this.config = (object) => {
    let params = {
      index: self.configIndex,
      type: self.configType
    };
    let searchParams = Object.assign({}, params); // create copy
    searchParams.q = '_id:' + self.configType;
    searchParams.body = {size: 1};
    if (object) {
      return client.search(searchParams).then((response) => {
        var obj = self.trimResults(response);
        if (obj && obj._id) {
          params.id = obj._id;
          params.body = {doc: object};
          return client.update(params);
        }
        else {
          params.id = self.configType;
          params.body = object;
          return client.index(params);
        }
      });
    }
    return client.search(searchParams);
  };

  this.init = (object) => {
    var properties = createMapping(object)
    properties.timestamp = {type: 'date', format: 'strict_date_optional_time||epoch_millis'};
    return client.index({
      index: self._index,
      type: self._type,
      body: {}
    }).then(() => {
      return client.indices.putMapping({
        index: self._index,
        type: self._type,
        include_type_name: true,
        body: {properties: properties}
      }).then(() => {
        // console.log(self._index + " initialized");
      });
    });;
  };

  this.isInitialized = () => {
    return client.indices.exists({index: self._index});
  };

  this.clear = () => {
    return client.indices.getMapping({index: self._index}).then((map) => {
      // console.log('Fetched index map');
      return client.indices.delete({index: self._index}).then(() => {
        // console.log('Deleted index');
        return client.index({
          index: self._index,
          type: self._type,
          include_type_name: true,
          body: {}
        }).then(() => {
          var mappings = map[self._index].mappings;
          if (mappings && mappings[self._type] && mappings[self._type].properties) {
            if (mappings[self._type].properties.fielddata) {
              delete(mappings[self._type].properties.fielddata);
            }
            return client.indices.putMapping({
              index: self._index,
              type: self._type,
              include_type_name: true,
              body: {properties: mappings[self._type].properties}
            });
          }
          else {
            console.warn('No existing mapping, set mapping with init');
          }
        });
      });
    });
  };

  this.guessTime = (obj) => {
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
  };

  this.insert = (object) => {
    var ts = new Date();
    var guessed;
    if (object && object.timestamp) {
      if (guessed = self.guessTime(object.timestamp)) {
        ts = guessed;
      }
      else {
        var msg = 'Could not parse timestamp ' + object.timestamp + '!' +
          ' Overriding with ' + ts + '.';
        console.warn(msg);
      }
    }
    object.timestamp = ts;
    return client.indices.exists({index: self._index}).then(() => {
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

  this.bulk = (bulk, timeout) => {
    var params = {index: self._index, type: self._type, body: bulk};
    if (timeout) {
      params.timeout = timeout;
      params.requestTimeout = 300000;
    }
    return client.bulk(params);
  };

  this.deleteOne = (id) => {
    return client.delete({index: self._index, type: self._type, id: id});
  };

  this.deleteMany = (params) => {
    return this.search(params).then((results) => {
      // console.log(results);
      var bulk = [];
      for (var i=0; i<results.lenght; i++) {
        var id = results[i]._id;
        bulk.push({delete: {index: this._index, type: this._type, id: id}});
      }
      // console.log(bulk);
      this.bulk(bulk);
    });
  };

  this.remove = () => {
    return client.indices.delete({index: this._index}).then((result) => {
      return client.indices.delete({index: this.configIndex});
    });
  };

  this.startScroll = (params) => {
    params.index = self._index;
    params.type = self._type;
    return client.search(params);
  };

  this.scroll = (params) => {
    params.index = self._index;
    params.type = self._type;
    return client.scroll(params);
  };

  this.search = (params) => {
    return client.search({index: self._index, type: self._type, body: params});
  };

  this.msearch = (params) => {
    var searchParams = [];
    for (var i=0; i<params.length; i++) {
      searchParams[2*i] = {index: self._index, type: self._type};
      searchParams[2*i+1] = params[i];
    }
    return client.msearch({index: self._index, type: self._type, body: searchParams});
  };

  this.release = () => {
    // the process will hang for some time unless the Elasticsearch connection is closed
    return client.close();
  };

  this.trimResults = (r) => {
    if (r && r.body && r.body.hits && r.body.hits.hits && r.body.hits.hits[0] && r.body.hits.hits[0]._source) {
      return r.body.hits.hits[0]._source;
    }
    return false;
  };

  this.trimBulkResults = (r) => {
    return r.body;
  };

  return this;

}

const createMapping = (object) => {
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
/*
      if (object[prop] === 'text') {
        properties[prop].type = 'string';
        // doc_values do not currently work with analyzed string fields
        properties[prop].doc_values = false;
      }
      // other strings are not analyzed
      else if (object[prop] === 'string') {
        properties[prop].index = 'not_analyzed';
      }
*/
      if (object[prop] === 'text') {
        properties[prop].fielddata = 'true';
        properties[prop].doc_values = false;
      }
      else if (object[prop] === 'string') {
        properties[prop].type = 'keyword';
      }
    }
  }
  return properties;
}

module.exports = correl8;
