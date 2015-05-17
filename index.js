var argv = require('minimist')(process.argv.slice(2)),
    proj4 = require("proj4"),
    moment = require('moment'),
    csv = require('csv'),
    fs = require('fs');

/* add ESRI:102645 to spatial definitions see: http://spatialreference.org/ref/esri/nad-1983-stateplane-california-v-fips-0405-feet/ */

proj4.defs("ESRI:102645", "+proj=lcc +lat_1=34.03333333333333 +lat_2=35.46666666666667 +lat_0=33.5 +lon_0=-118 +x_0=2000000 +y_0=500000.0000000002 +ellps=GRS80 +datum=NAD83 +to_meter=0.3048006096012192 +no_defs");

/* csv tranformations */

var parser = csv.parse({delimiter: ',', columns: true})
var stringifier = csv.stringify({ header: true});

/* data / time transformations */

var appendTimestamp = csv.transform(function(record){
  var date = record['Issue Date'].substr(0,10);
  var time = record['Issue time'].replace(/(\d{2})$/, "\:$1");
  var parsed = moment(date + " " + time, 'MM/DD/YYYY HH:mm')  
  record.timestamp = parsed.format();        
  record.valid_timestamp = parsed.isValid().toString();
  return record;  
},{parallel: 10});

/* geographic tranformations */

var appendWGS84Coordinates = csv.transform(function(record){
  projected = proj4('ESRI:102645','EPSG:4326',[record.Latitude,record.Longitude]);
  record.latitude = projected[1];
  record.longitude = projected[0];
  return record;
},{parallel: 10});

var annotateBadCoordinates = csv.transform(function(record){
  record.valid_coordinate = (record.latitude > 32.8007 && record.latitude < 34.8233 && record.longitude > -118.9448 && record.longitude < -117.6462).toString();
  return record;
},{parallel: 10});

/* process */

fs.createReadStream(argv.input)
  .pipe(parser)
  .pipe(appendTimestamp)
  .pipe(appendWGS84Coordinates)
  .pipe(annotateBadCoordinates)
  .pipe(stringifier)
  .pipe(process.stdout);





