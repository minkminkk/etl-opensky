#!/usr/bin/bash

cur_dir=$(pwd)

if ! [ -d $1 ];
then
    mkdir -p $1
fi

cd $1

echo "Checking aircraft data..."
if ! [ -f ./aircraft-database-complete-2024-01.csv ];
then
    echo "Aircraft database not available. Downloading..."
    wget https://opensky-network.org/datasets/metadata/aircraft-database-complete-2024-01.csv
else
    echo "Aircraft database is already available."
fi

echo "Checking aircraft type data..."
if ! [ -f ./doc8643AircraftTypes.csv ];
then
    echo "Aircraft type data not available. Downloading..."
    wget https://opensky-network.org/datasets/metadata/doc8643AircraftTypes.csv
else
    echo "Aircraft type data is already available."
fi

echo "Checking manufacturer data..."
if ! [ -f ./doc8643Manufacturers.csv ];
then
    echo "Manufacturer data not available. Downloading..."
    wget https://opensky-network.org/datasets/metadata/doc8643Manufacturers.csv
else
    echo "Manufacturer data is already available."
fi

echo "Checking airport data..."
if ! [ -f ./airports.json ];
then
    echo "Airport data not available. Downloading..."
    curl https://www.flightradar24.com/_json/airports.php -o ./airports.json
else
    echo "Airport data is already available."
fi

echo "Checking airlines data..."
if ! [ -f ./airlines.json ];
then
    echo "Airline data not available. Downloading..."
    curl https://www.flightradar24.com/_json/airlines.php -o ./airlines.json
else
    echo "Airline data is already available."
fi

cd $cur_dir
echo "Finished downloading data for pipeline."