# marry_me_organizer

## Objective

To design, develop, and test an application capable of receiving multiple events, and delivering them to the appropriate teams, according to priority and team members availability.

## Stack/Technologies

- Python
- Kafka
- Zookeeper
- Docker
- kSQLdb

## Usage

Git clone to download the repo to your local environment:
```
git clone https://github.com/alannchang/marry_me_organizer.git
```

Assuming you have [Docker Engine](https://docs.docker.com/engine/) installed and running:

To build:
```
docker compose build
```

To start the simulation:
```
docker compose up
```

To stop the simulation and remove all containers, images, and builds: 
```
docker-compose down --volumes --rmi all
```



### Examples:



## Results
No workers
| |Happy|Unhappy|
|-|-|-|
|1|0|0|
|2|0|0|
|3|0|0|
|4|0|0|
|5|0|0|
|6|0|0|

Workers (no optimizations)
| |Happy|Unhappy|
|-|-|-|
|1|0|0|
|2|0|0|
|3|0|0|
|4|0|0|
|5|0|0|
|6|0|0|

Workers with bucket priority pattern
| |Happy|Unhappy|
|-|-|-|
|1|0|0|
|2|0|0|
|3|0|0|
|4|0|0|
|5|0|0|
|6|0|0|
