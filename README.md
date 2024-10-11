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

Workers (no optimizations)
|Dataset|Happy|Unhappy|
|-|-|-|
|1|45|955|
|2|47|953|
|3|44|956|
|4|42|958|
|5|48|952|

Workers (bucket priority pattern)
|Dataset|Happy|Unhappy|
|-|-|-|
|1|328|672|
|2|286|714|
|3|270|730|
|4|304|696|
|5|305|695|
