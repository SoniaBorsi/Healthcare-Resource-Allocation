-- init.sql

CREATE TABLE IF NOT EXISTS datasets (
    reportingstartdate DATE,
    reportedmeasurecode VARCHAR,
    datasetid INT PRIMARY KEY,
    measurecode VARCHAR,
    datasetname TEXT,
    stored BOOLEAN DEFAULT FALSE,
    totalrecords INTEGER DEFAULT 0,
    processedrecords INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS hospitals (
    code VARCHAR(255) PRIMARY KEY,
    name TEXT,
    type TEXT,
    latitude FLOAT,
    longitude FLOAT,
    sector TEXT,
    open_closed TEXT,
    state TEXT,
    lhn TEXT,
    phn TEXT
);

CREATE TABLE IF NOT EXISTS measurements (
    measurecode VARCHAR PRIMARY KEY,
    measurename TEXT
);

CREATE TABLE IF NOT EXISTS reported_measurements (
    reportedmeasurecode VARCHAR PRIMARY KEY,
    reportedmeasurename TEXT
);

CREATE TABLE IF NOT EXISTS info (
    datasetid INT,
    reportingunitcode VARCHAR,
    value FLOAT,
    caveats TEXT,
    id VARCHAR PRIMARY KEY
);
