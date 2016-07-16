# distributed-pipeline

## Build
```bash
docker build -t datafellas/distributed-pipeline:0.0.1 .
```

## Run
This uses a variable `LOCAL_NOTEBOOKS` which refers to a local directory containing the notebooks you want to include and keep up to date during the session.


```bash
export LOCAL_NOTEBOOKS=<path to local dir>
docker run -v $LOCAL_NOTEBOOKS:/root/spark-notebook/notebooks/pipeline --rm -it -m 8g -p 19000:9000 -p 14040:4040 -p 14041:14041 -p 14042:4042 -p 14043:14043 datafellas/distributed-pipeline:0.0.1 bash
```

Then
```bash
source var.sh
source start.sh
source create.sh
```

## Access
Open browser at [http://localhost:19000/tree/pipeline](http://localhost:19000/tree/pipeline).

> WARN:
> 
> If you're running docker via a VM (Mac, Msft, ...) then you need to replace `localhost` by the VM's IP.

