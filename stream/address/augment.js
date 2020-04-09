
var through = require('through2'),
  polyline = require('@mapbox/polyline'),
  project = require('../../lib/project'),
  proximity = require('../../lib/proximity'),
  analyze = require('../../lib/analyze'),
  interpolate = require('../../lib/interpolate');

// polyline precision
var PRECISION = 6;

/**
this stream performs all the interpolation math for a road segment and pushes
downstream rows to be inserted in the 'street_address' table.
**/
function streamFactory(db, done) {

  // create a new stream
  return through.obj(function (lookup, _, next) {

    // decode polylines
    lookup.streets.forEach(function (street, i) {
      street.coordinates = project.dedupe(polyline.toGeoJSON(street.line, PRECISION).coordinates);
    });

    // process all house number entries in batch
    lookup.batch.forEach(function (address) {

      // parse housenumber
      var housenumber = analyze.housenumber(address.getNumber());

      // invalid / unusual housenumber
      if (isNaN(housenumber)) {
        console.error('could not reliably parse housenumber', address.getNumber());
        return;
      }

      // format shift point data
      var point = [address.getCoord().lon, address.getCoord().lat];
      var point_left = [];
      var point_right = [];
      var nearest_left
      var nearest_right

      // order streets by proximity from point (by projecting it on to each line string)
      var ordered = proximity.nearest.street(lookup.streets, point);

      // an error occurred
      if (!ordered || !ordered.length) {
        console.error('unable to find nearest street for point');
        console.error('streets', lookup.streets);
        console.error('address', address);
        return;
      }

      // use the closest street
      var nearest = ordered[0];

      // compute L/R parity of house on street
      var parity = project.parity(nearest.proj, point);

      if (address.getId().includes("way")) {
        point_left.push(address.getBounds().e)
        point_left.push(address.getBounds().n)
        point_right.push(address.getBounds().w)
        point_right.push(address.getBounds().s)

        // order streets by proximity from point (by projecting it on to each line string)
        var ordered_left = proximity.nearest.street(lookup.streets, point_left);

        // an error occurred
        if (!ordered_left || !ordered_left.length) {
          console.error('unable to find nearest street for point');
          console.error('streets', lookup.streets);
          console.error('address', address);
          return;
        }

        // use the closest street
        var nearest_left = ordered_left[0];

        // order streets by proximity from point (by projecting it on to each line string)
        var ordered_right = proximity.nearest.street(lookup.streets, point_right);

        // an error occurred
        if (!ordered_right || !ordered_right.length) {
          console.error('unable to find nearest street for point');
          console.error('streets', lookup.streets);
          console.error('address', address);
          return;
        }

        // use the closest street
        var nearest_right = ordered_right[0];
      }

      // push openaddresses values to db
      this.push({
        $id: nearest.street.id,
        $source: address.getSource(),
        $source_id: address.getId(),
        $housenumber: housenumber,
        $lon: point[0],
        $lat: point[1],
        $parity: parity,
        $proj_lon: nearest.proj.point[0],
        $proj_lat: nearest.proj.point[1],
        $proj_lon_left: point_left.length > 0 ? nearest_left.proj.point[0] : null,
        $proj_lat_left: point_left.length > 0 ? nearest_left.proj.point[1] : null,
        $proj_lon_right: point_right.length > 0 ? nearest_right.proj.point[0] : null,
        $proj_lat_right: point_right.length > 0 ? nearest_right.proj.point[1] : null
      });

    }, this);

    next();
  });
}

module.exports = streamFactory;
