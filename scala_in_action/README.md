# Scala In Action 

### Run Polynote container 
```shell
docker run --rm -it -p 127.0.0.1:8192:8192 -p 127.0.0.1:4040-4050:4040-4050 -v `pwd`/config.yml:/opt/config/config.yml -v `pwd`/notebooks:/opt/notebooks/ polynote/polynote:latest --config /opt/config/config.yml
```