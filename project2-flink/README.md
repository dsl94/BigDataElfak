## Podizanje env
### Pokrenuti u folder iznad
`docker-compose -f docker-compose-2/flink.yaml up -d`

## Izbildovati projekat koristeći maven, ako se ne menjaju parametri preskočiti ovaj korak
`mvn clean install`

## Pokretanje producera
U folderu project2 pokrenuti
`python producer-flink.py`

## Pokretanje u Flink clusteru
Otvoriti http://localhost:8081/#/submit
Uploadovati jar file `flink-1.0-SNAPSHOT-jar-with-dependencies.jar` iz foldera `target`
Za ime main klase unetu `rs.elfak.Main` i pokrenuti job
