## Pre pokretanja
Zbog githuba nije moguce da se okaci veliki dataset fajl, skinuti ga sa linka ispod
https://drive.google.com/file/d/1rOAQpjIT_IWpuqFhTpTAFDFEZ71P0oq6/view?usp=sharing

## Podizanje env
### Pokrenuti u folder iznad
`docker-compose -f docker-compose-2.yaml up -d`

## Pokretanje u clusteru
`docker build --rm -t bde/spark-app2 .`

`docker run --name spark-stream --net bde -p 4040:4040 -d bde/spark-app2`

## Pokretanje producera
`python producer.py`
