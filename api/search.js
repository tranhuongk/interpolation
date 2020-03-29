
var sqlite3 = require('sqlite3'),
  requireDir = require('require-dir'),
  query = requireDir('../query'),
  project = require('../lib/project'),
  geodesic = require('../lib/geodesic'),
  analyze = require('../lib/analyze');

// export setup method
function setup(addressDbPath, streetDbPath) {

  // connect to db
  sqlite3.verbose();
  var db = new sqlite3.Database(addressDbPath, sqlite3.OPEN_READONLY);

  // attach street database
  query.attach(db, streetDbPath, 'street');

  // enable memmapping of database pages
  db.run('PRAGMA mmap_size=268435456;');
  db.run('PRAGMA street.mmap_size=268435456;');

  // query method
  var q = function (coord, number, street, cb) {

    var point = {
      lat: parseFloat(coord.lat),
      lon: parseFloat(coord.lon)
    };

    if ('string' !== typeof number) { return cb('invalid number'); }
    if ('string' !== typeof street) { return cb('invalid street'); }

    var normalized = {
      number: analyze.housenumber(number),
      street: analyze.street(street)
    };

    // error checking
    if (isNaN(point.lat)) { return cb('invalid latitude'); }
    if (isNaN(point.lon)) { return cb('invalid longitude'); }
    if (isNaN(normalized.number)) { return cb('invalid number'); }
    if (!normalized.street.length) { return cb('invalid street'); }

    // perform a db lookup for the specified street
    // @todo: perofmance: only query for part of the table
    query.search(db, point, normalized.number, normalized.street, function (err, res) {

      // @note: results can be from multiple different street ids.

      // an error occurred or no results were found
      if (err || !res || !res.length) { return cb(err, null); }

      // try to find an exact match
      var match = res.find(function (row) {
        if (row.source === 'VERTEX') { return false; }
        return row.housenumber === normalized.number;
      });

      // return exact match
      if (match) {
        return cb(null, [{
          type: 'exact',
          source: match.source,
          source_id: match.source_id,
          number: number,
          parity: match.parity,
          accuracy: 100,
          // number: analyze.housenumberFloatToString( match.housenumber ),
          lat: parseFloat(match.lat.toFixed(7)),
          lon: parseFloat(match.lon.toFixed(7))
        }]);
      }

      // try to find a close match with the same number (possibly an apartment)
      match = res.find(function (row) {
        if (row.source === 'VERTEX') { return false; }
        return Math.floor(row.housenumber) === Math.floor(normalized.number);
      });

      // return close match
      if (match) {
        return cb(null, [{
          type: 'close',
          source: match.source,
          source_id: match.source_id,
          number: number,
          parity: match.parity,
          accuracy: 90,
          // number: analyze.housenumberFloatToString( match.housenumber ),
          lat: parseFloat(match.lat.toFixed(7)),
          lon: parseFloat(match.lon.toFixed(7))
        }]);
      }

      // attempt to interpolate the position

      // find the records before and after the desired number (group by street segment)
      var map_r = {};
      var map_l = {};
      res.forEach(function (row) {
        if (!map_r.hasOwnProperty(row.id)) { map_r[row.id] = {}; }
        if (row.housenumber < normalized.number && row.parity == "R") { map_r[row.id].before = row; }
        if (row.housenumber > normalized.number && row.parity == "R") { map_r[row.id].after = row; }
        if (map_r[row.id].before && map_r[row.id].after) {
          map_r[row.id].diff = {
            before: map_r[row.id].before.housenumber - normalized.number,
            after: map_r[row.id].after.housenumber - normalized.number
          };
        }
      });
      res.forEach(function (row) {
        if (!map_l.hasOwnProperty(row.id)) { map_l[row.id] = {}; }
        if (row.housenumber < normalized.number && row.parity == "L") { map_l[row.id].before = row; }
        if (row.housenumber > normalized.number && row.parity == "L") { map_l[row.id].after = row; }
        if (map_l[row.id].before && map_l[row.id].after) {
          map_l[row.id].diff = {
            before: map_l[row.id].before.housenumber - normalized.number,
            after: map_l[row.id].after.housenumber - normalized.number
          };
        }
      });

      // remove segments with less than 2 points; convert map to array
      var segments_r = [];
      for (var id in map_r) {
        if (map_r[id].before && map_r[id].after) {
          segments_r.push(map_r[id]);
        }
      }
      var segments_l = [];
      for (var id in map_l) {
        if (map_l[id].before && map_l[id].after) {
          segments_l.push(map_l[id]);
        }
      }

      // could not find two rows to use for interpolation
      if (!segments_r.length && !segments_l.length) {
        return cb(null, null);
      }

      // sort by miniumum housenumber difference from target housenumber ASC
      segments_r.sort(function (a, b) {
        return Math.abs(a.diff.before + a.diff.after) - Math.abs(b.diff.before + b.diff.after);
      });
      segments_l.sort(function (a, b) {
        return Math.abs(a.diff.before + a.diff.after) - Math.abs(b.diff.before + b.diff.after);
      });

      // select before/after values to use for the interpolation
      var before_r = segments_r[0].before;
      var after_r = segments_r[0].after;
      var before_l = segments_l[0].before;
      var after_l = segments_l[0].after;

      // compute interpolated address
      var A = { lat: project.toRad(before_r.proj_lat), lon: project.toRad(before_r.proj_lon) };
      var B = { lat: project.toRad(after_r.proj_lat), lon: project.toRad(after_r.proj_lon) };
      var distance = geodesic.distance(A, B);
      var A_L = { lat: project.toRad(before_l.proj_lat), lon: project.toRad(before_l.proj_lon) };
      var B_L = { lat: project.toRad(after_l.proj_lat), lon: project.toRad(after_l.proj_lon) };
      var distance_l = geodesic.distance(A_L, B_L);

      // if distance = 0 then we can simply use either A or B (they are the same lat/lon)
      // else we interpolate between the two positions
      var point_r = A;
      if (distance > 0) {
        var ratio = ((normalized.number - before_r.housenumber) / (after_r.housenumber - before_r.housenumber));
        point_r = geodesic.interpolate(distance, ratio, A, B);
      }
      var point_l = A_L;
      if (distance_l > 0) {
        var ratio = ((normalized.number - before_l.housenumber) / (after_l.housenumber - before_l.housenumber));
        point_l = geodesic.interpolate(distance, ratio, A, B);
      }

      // return interpolated address
      return cb(null, [{
        type: 'interpolated',
        source: 'mixed',
        number: number,
        parity: "R",
        accuracy: 90,
        // number: '' + Math.floor( normalized.number ),
        lat: parseFloat(project.toDeg(point_r.lat).toFixed(7)),
        lon: parseFloat(project.toDeg(point_r.lon).toFixed(7))
      }, {
        type: 'interpolated',
        source: 'mixed',
        number: number,
        parity: "L",
        accuracy: 90,
        // number: '' + Math.floor( normalized.number ),
        lat: parseFloat(project.toDeg(point_l.lat).toFixed(7)),
        lon: parseFloat(project.toDeg(point_l.lon).toFixed(7))
      }]);
    });
  };

  // close method to close db
  var close = db.close.bind(db);

  // return methods
  return {
    query: q,
    close: close,
  };
}

module.exports = setup;
