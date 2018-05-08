# Scavenger Hunt
This is a multiplayer online game in which players use clues to identify and travel to target locations around them.

## Project Background
**Pokemon Go** In July 2016, the location-based augmented-reality game Pokemon Go was released and became a global phenomenon. The rapid adoption of this game demonstrates the space for social location-based gaming. Soon after its inception, technical challenges arose in its infrastructure. Due to the large scale of usage, Pokemon go was unable to calculate approximate distances between individual users and points in space in a fast and reliable way, and eventually removed the feature entirely.

## Data Engineering Challenge
Data engineering challenges occur when the database scales with the number of target locations and concurrent players. The question this project aims to answer is: how do we serve new targets as players enter new areas, rank targets based on proximity, and remove targets when players walk too far away or solve them, all while doing this in a fast and reliable way?

### Tags
data engineering, Insight, data pipeline

## How it works
**File System**: The file system used must be able to scale quickly as the project grows. Possible file systems that would work well would be managed systems like AWS S3 or GCP Storage.

**Database**: This project must use a database that is highly available and has high partition tolerance. Players will not necessarily need consistency because it doesn't really matter if what they see differs from what other players nearby sees. Possible data stores include CouchDB or Cassandra. 

**Data Injestion**: This pipeline must accept simultaneous streams of inputs from its player base, a fast, distributed messaging system like Kafka or Amazon Kinesis will be used.

**Stream Processing**: Because this application will be processing real-time analytics, stream processing technology like Spark Streaming, Storm, Kafka Streams or Kinesis Streams may be used.


### Pipeline Implementation Process
[TBD]

## Getting started
[TBD]

### Prerequisites
[TBD]

### Installing
[TBD]

### Running Tests
[TBD]

## Author
Steven Jin
