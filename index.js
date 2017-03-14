/*
 * TransportCapacitySort
 * Bonial International GmbH
 * written by Jan Max Tiedemann
 *
 * This file is subject to the terms and conditions defined in
 * file 'LICENSE.txt', which is part of this source code package.
 */

// Import external libaries
const _ = require('underscore');
const numeral = require('numeral');
const JSONStream = require('JSONStream');
const clear = require('clear');
const fs = require('fs');

// Result array (makes results easy so sort)
var count_results = [];
var distinct_results = [];

// Metrics Variables
var entry_count = 0;
var data_count = 0;

// Count by transport type
var car_count = 0;
var plane_count = 0;
var train_count = 0;


// Arrays for Transport types (needed to keep track of the distint models)
var cars = [];
var planes = [];
var trains = [];

// this function can be used as a filter on an array and will only return unique items
function onlyUnique(value, index, self) {
    return self.indexOf(value) === index;
}

if (process.argv.length > 2 || process.argv.length == 1) {
  console.console.log("Usage: " + process.argv[0] + " <file spath>"});
  return;
}

// Path to input file
var path = process.argv[1];

// Init JSONStream (reads only the important parts (only the transports array) of the file)
var json = JSONStream.parse('transports.*');

// setup data recive event
json.on('data', (data) => {
  // On each entry increase entry count by 1
  entry_count++;

  // Every 100k entries give a hint how its going
  if ((entry_count - 1) % 1e5 == 0 && (entry_count - 1) != 0) {
    console.log("Parsed \t" + numeral(entry_count).format('0.0a') + " entries");
    console.log("Cars: " + cars.length + "\t | Trains: " + trains.length + "\t | Planes: " + planes.length);
  }

  // Every 10m clean the memory (requires gc to be accessible)
  if ((entry_count - 1) % 1e7 == 0 && (entry_count - 1) != 0) {

    // this works by fitering out every dupicate in every array and then running the garbage collector
    // This is not nessesary but i will otherwise pollute your ram and might run out of heap memory
    console.time('cleanup')
    console.log("Reducing car array");
    var cars_before = cars.length;
    cars = cars.filter(onlyUnique);
    console.log("Reducing trains array");
    var trains_before = trains.length;
    trains = trains.filter(onlyUnique);
    console.log("Reducing planes array");
    var planes_before = planes.length;
    planes = planes.filter(onlyUnique);
    console.log("Comencing Garbage Collection...");
    global.gc();
    console.timeEnd('cleanup')
    console.log("Reduced car array size by: " + numeral(100-((cars_before-cars.length)/cars_before*10)).format('0.0') + "%");
    console.log("Reduced train array size by: " + numeral(100-((trains_before-trains.length)/trains_before*10)).format('0.0')  + "%");
    console.log("Reduced plane array size by: " + numeral(100-((planes_before-planes.length)/planes_before*10)).format('0.0')  + "%");
  }
  // As the type of a json object in this example can be determinded by looking for certian magic values
  // this is done here and the values are calucated as given in the instruction
  if (_.has(data, 'b-passenger-capacity')){
    planes.push(data.model);
    plane_count += (data["b-passenger-capacity"]+data["e-passenger-capacity"]);
  } else if (_.has(data, 'number-wagons')) {
    trains.push(data.model);
    train_count += (data["number-wagons"] * data["w-passenger-capacity"]);
  } else {
    cars.push(data.model);
    car_count += data["passenger-capacity"];
  }
});

// This function does the final calulaitons for distinct and prints the results
function processData() {
  // Clear console
  clear();
  console.time('distinct');

  // add results to arrays
  count_results.push({
      "name": 'cars',
      "capacity": car_count
  });
  count_results.push({
      "name": 'trains',
      "capacity": train_count
  });
  count_results.push({
      "name": 'planes',
      "capacity": plane_count
  });
  console.log("Sorting count_results...");
  count_results.sort((kek, kek1) => {
      return kek1.capacity - kek.capacity;
  })

  console.log("Seatching for distinct_cars")
  var distinct_cars = cars.filter(onlyUnique).length
  console.log("Seatching for distinct_trains")
  var distinct_trains = trains.filter(onlyUnique).length
  console.log("Seatching for distinct_trains")
  var distinct_planes = planes.filter(onlyUnique).length

  // add results to array
  distinct_results.push({
      "name": 'distinct_cars',
      "capacity": distinct_cars
  });
  distinct_results.push({
      "name": 'distinct_trains',
      "capacity": distinct_trains
  });
  distinct_results.push({
      "name": 'distinct_planes',
      "capacity": distinct_planes
  });

  console.log("Sorting distinct_results...");
  distinct_results.sort((kek, kek1) => {
      return kek1.capacity - kek.capacity;
  })

  console.timeEnd('distinct');
  console.timeEnd('loadingData');

  // Print results
  console.log("Capacity by transport class");
  for (var i = 0; i < count_results.length; i++) {
      console.log("["+(i+1)+"]: " +count_results[i].name + ": " + count_results[i].capacity);
  }
  console.log();
  console.log("Distinct transport entitys");
  for (var i = 0; i < distinct_results.length; i++) {
      console.log("["+(i+1)+"]: " +distinct_results[i].name + ": " + distinct_results[i].capacity);
  }
}

// acctual start of processing
console.time('loadingData');
// Create a file reading stream
var readStream = fs.createReadStream(path);

// call final processing on EOF
readStream.on('end', () => {
  processData();
})

// Pipe read data to json stream
readStream.pipe(json);
