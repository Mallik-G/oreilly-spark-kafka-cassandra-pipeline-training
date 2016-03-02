## Usefull commands

Run docker container on Mac

```
docker run --rm -it -m 8g -p 19000:9000 -p 14040:4040 -p 14041:14041 -p 14042:4042 -p 14043:14043 datafellas/oreilly-pipeline:1.0 bash
```


Create Kafka Topic *(On Docker conatiner)*

```
kafka-topics --zookeeper localhost:2181 --create --topic twitter --partitions 1 --replication-factor 1
```


Output topic messages *(On Docker conatiner)*

```
kafka-console-consumer --topic twitter --from-beginning --zookeeper localhost:2181
```


Docker show containers

```
docker ps
```


Docker attach to running docker

```
docker attach [name]
```


## SparkNotebook info

* To see debug output show console in Developer Tools in browser
* Shutdown notebook from notebooks overview oir Kernel -> Stop Kernel
* To see config Edit-> Edit Notebook Metadata (to see where what is stored etc)

