#!/usr/bin/bash

if ! [ -d data ];
then
    mkdir -p data
fi

cd data

echo "Checking aircraft data..."
if ! [ -f ./aircraft-database-complete-2023-12.csv ];
then
    echo "Aircraft database not available. Downloading..."
    wget https://opensky-network.org/datasets/metadata/aircraft-database-complete-2023-12.csv
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

cd ..
echo "Finished downloading data for pipeline"