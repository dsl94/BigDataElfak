## Pre pokretanja
Potrebno je da se završi treniranje koristeci projekat `project-3-train`

## Podizanje env
### Pokrenuti u folder iznad
`docker-compose -f docker-compose-3.yaml up -d`

## Pokretanje u clusteru
`docker build --rm -t bde/spark-app3-classify .`

`docker run --name p3-classify --net bde -p 4040:4040 -d bde/spark-app3-classify`

## Pokretanje producera
`python producer.py`
