<?php

/**
 * Script to index data in Elasticsearch from a dumped geonames' file.
 */
require 'vendor/autoload.php';

$client = new Elasticsearch\Client();

if(count($argv) != 3) {
    exit("Usage: php geolastic-import.php <path-geonames-csv> <index-name>\n");
}

createIndex($client, $argv[2]);
dumpData($client, $argv[1], $argv[2]);

/**
 * Creates an Elasticsearch index.
 *
 * @param Elasticsearch\Client $client the initialized Elasticsearch client.
 * @param string $indexName the name of the index that will be created in ES.
 * @return void
 */
function createIndex(Elasticsearch\Client $client, $indexName) {
    $indexParams['index']  = $indexName;
    $typeMapping = array(
        'properties' => array(
            'geonameId' => array(
                'type' => 'integer'
            ),
            'name' => array(
                'type' => 'string',
                'analyzer' => 'standard',
            ),
            'asciiName' => array(
                    'type' => 'string',
                    'analyzer' => 'standard',
                ),
            'alternateNames' => array(
                    'type' => 'string',
                    'analyzer' => 'standard',
                ),
            'location' => array(
                    'type' => 'geo_point'
                ),
            'featureClass' => array(
                    'type' => 'string'
                ),
            'featureCode' => array(
                    'type' => 'string'
                ),
            'countryCode' => array(
                    'type' => 'string'
                ),
            'cc2' => array(
                    'type' => 'string'
                ),
            'admin1Code' => array(
                    'type' => 'string'
                ),
            'admin2Code' => array(
                    'type' => 'string'
                ),
            'admin3Code' => array(
                    'type' => 'string'
                ),
            'admin4Code' => array(
                    'type' => 'string'
                ),
            'population' => array(
                    'type' => 'integer'
                ),
            'elevation' => array(
                    'type' => 'integer'
                ),
            'dem' => array(
                    'type' => 'integer'
                ),
            'timezone' => array(
                    'type' => 'string'
                ),
            'modificationdate' => array(
                    'type' => 'date'
                )
            )
    );

    $indexParams['body']['mappings']['geonames'] = $typeMapping;
    $client->indices()->create($indexParams);
}

/**
 * Dumps data to elasticsearch.
 *
 * @param Elasticsearch\Client $client the initialized Elasticsearch client.
 * @param string $path  with the geonames's dumped file.
 * @param string $indexName the name of the index that will be created in ES.
 * @return void
 */
function dumpData(Elasticsearch\Client $client, $path, $indexName) {
    $file = fopen($path, 'r') or exit("Impossible to open file: ($path)");

    while (($line = fgetcsv($file, 0, "\t")) !== FALSE) {
        $document = parseLine($line);

        $params = array();
        $params['body']  = $document;
        $params['index'] = $indexName;
        $params['type']  = 'geonames';
        $params['id']    = (int) $document['geonameId'];

        $returnValue = $client->index($params);
        echo "Added " . $params['id'] . " " . $document['name'] . "\n";
    }
}

/**
 * Builds the document that will be indexed in Elasticsearch.
 *
 * @param array $path  with one line readed from the file.
 * @return array $document with the built document.
 */
function parseLine($line) {
    $document = array();

    $document['geonameId'] = (int) $line[0];
    $document['name'] = $line[1];
    $document['asciiName'] = $line[2];

    if($line[3] !== NULL && $line[3] !== "") {
        $document['alternateNames'] = split(",", $line[3]);
    }

    $document['location'] = array('lat' => (float)($line[4]), 'lon' => (float)($line[5]));

    $document['featureClass'] = $line[6];
    $document['featureCode'] = $line[7];
    $document['countryCode'] = $line[8];

    if($line[8] !== NULL && $line[9] !== "") {
        $document['cc2'] = split(",", $line[9]);
    }

    $document['admin1Code'] = $line[10];
    $document['admin2Code'] = $line[11];
    $document['admin3Code'] = $line[12];
    $document['admin4Code'] = $line[13];
    $document['population'] = (int)$line[14];
    $document['elevation'] = (int)$line[15];
    $document['dem'] = (int)$line[16];
    $document['timezone'] = $line[17];
    $document['modificationDate'] = $line[18];

    return $document;
}
