
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
    query.extract(db, point, normalized.street, function (err, res) {

      // @note: results can be from multiple different street ids.

      // an error occurred or no results were found
      if (err || !res || !res.length) { return cb(err, []); }

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
      var res_r = []
      var res_l = []

      var before_r
      var before_l
      var after_r
      var after_l

      res.forEach(function (row) {
        if (row.parity === "R") {

          if (!before_r) {
            before_r = row
          }
          else if (before_r.housenumber < row.housenumber && row.housenumber < normalized.number) {
            before_r = row
          }

          if (!after_r) {
            after_r = row
          }
          else if (after_r.housenumber > row.housenumber && row.housenumber > normalized.number) {
            after_r = row
          }

        }
        else {
          if (!before_l) {
            before_l = row
          }
          else if (before_l.housenumber < row.housenumber && row.housenumber < normalized.number) {
            before_l = row
          }

          if (!after_l) {
            after_l = row
          }
          else if (after_l.housenumber > row.housenumber && row.housenumber > normalized.number) {
            after_l = row
          }
        }
      })
      res_r.push(before_r)
      res_r.push(after_r)
      res_l.push(before_l)
      res_l.push(after_l)
      console.log(res)

      res_r.forEach(function (row) {
        if (!map_r.hasOwnProperty(row.id)) { map_r[row.id] = {}; }
        if (row.housenumber < normalized.number) { map_r[row.id].before = row; }
        if (row.housenumber > normalized.number) { map_r[row.id].after = row; }
        if (map_r[row.id].before && map_r[row.id].after) {
          map_r[row.id].diff = {
            before: map_r[row.id].before.housenumber - normalized.number,
            after: map_r[row.id].after.housenumber - normalized.number
          };
        }
      });
      res_l.forEach(function (row) {
        if (!map_l.hasOwnProperty(row.id)) { map_l[row.id] = {}; }
        if (row.housenumber < normalized.number) { map_l[row.id].before = row; }
        if (row.housenumber > normalized.number) { map_l[row.id].after = row; }
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

      var results = []
      // could not find two rows to use for interpolation
      if (segments_r.length) {
        segments_r.sort(function (a, b) {
          return Math.abs(a.diff.before + a.diff.after) - Math.abs(b.diff.before + b.diff.after);
        });

        // select before/after values to use for the interpolation
        var before = segments_r[0].before;
        var after = segments_r[0].after;

        // compute interpolated address
        var A = { lat: project.toRad(before.proj_lat), lon: project.toRad(before.proj_lon) };
        var B = { lat: project.toRad(after.proj_lat), lon: project.toRad(after.proj_lon) };
        var distance = geodesic.distance(A, B);

        // if distance = 0 then we can simply use either A or B (they are the same lat/lon)
        // else we interpolate between the two positions
        var point = A;
        if (distance > 0) {
          var ratio = ((normalized.number - before.housenumber) / (after.housenumber - before.housenumber));
          point = geodesic.interpolate(distance, ratio, A, B);
        }

        results.push({
          type: 'interpolated',
          source: 'mixed',
          number: number,
          before: before_r.housenumber,
          after: after_r.housenumber,
          parity: "R",
          accuracy: 90,
          // number: '' + Math.floor( normalized.number ),
          lat: parseFloat(project.toDeg(point.lat).toFixed(7)),
          lon: parseFloat(project.toDeg(point.lon).toFixed(7))
        })
      }

      if (segments_l.length) {
        segments_l.sort(function (a, b) {
          return Math.abs(a.diff.before + a.diff.after) - Math.abs(b.diff.before + b.diff.after);
        });

        // select before/after values to use for the interpolation
        var before = segments_l[0].before;
        var after = segments_l[0].after;

        // compute interpolated address
        var A = { lat: project.toRad(before.proj_lat), lon: project.toRad(before.proj_lon) };
        var B = { lat: project.toRad(after.proj_lat), lon: project.toRad(after.proj_lon) };
        var distance = geodesic.distance(A, B);

        // if distance = 0 then we can simply use either A or B (they are the same lat/lon)
        // else we interpolate between the two positions
        var point = A;
        if (distance > 0) {
          var ratio = ((normalized.number - before.housenumber) / (after.housenumber - before.housenumber));
          point = geodesic.interpolate(distance, ratio, A, B);
        }

        results.push({
          type: 'interpolated',
          source: 'mixed',
          number: number,
          before: before_l.housenumber,
          after: after_l.housenumber,
          parity: "L",
          accuracy: 90,
          // number: '' + Math.floor( normalized.number ),
          lat: parseFloat(project.toDeg(point.lat).toFixed(7)),
          lon: parseFloat(project.toDeg(point.lon).toFixed(7))
        })
      }



      // return interpolated address
      return cb(null, results);
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
