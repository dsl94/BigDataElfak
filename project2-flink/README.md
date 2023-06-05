## Podizanje env
### Pokrenuti u folder iznad
`sh start-deps.sh`

## Izbildovati projekat koristeći maven, ako se ne menjaju parametri preskočiti ovaj korak
`sh build.sh`

## Pokretanje producera
`sh start-producer.sh`

## Pokretanje u Flink clusteru
Otvoriti http://localhost:8081/#/submit
Uploadovati jar file `flink-1.0-SNAPSHOT-jar-with-dependencies.jar` iz foldera `target`
Za ime main klase unetu `rs.elfak.Main` i pokrenuti job
