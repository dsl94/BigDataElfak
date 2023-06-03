## Pre pokretanja
Mali dataset se nalazi u fajlu oslo-bikes-small
Zbog githuba nije moguce da se okaci veliki dataset fajl, skinuti ga sa linka ispod ako je potrebno pokretanje nad velikim datasetom
https://drive.google.com/file/d/1rOAQpjIT_IWpuqFhTpTAFDFEZ71P0oq6/view?usp=sharing

## Podizanje env
### Opcija 1: Pokrenuti skriptu
`sh start-deps.sh`

### Ubaciti dataset u hdfs

`docker cp oslo-bikes.csv namenode:/data`

`docker exec -it namenode bash`

`hdfs dfs -mkdir /dir`

`hdfs dfs -put /data/oslo-bikes.csv /dir`


## Pokretanje u clusteru
`sh docker-build.sh`

`sh cluster-run.sh`
