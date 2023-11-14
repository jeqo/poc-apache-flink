# PoC

## Commands

```shell
 make create_topic_by_size_ts topic=t2
 kfk-producer-datagen perf --kafka default -t t2 -n 100000 -k 1000 -q TRANSACTIONS --sr default -f AVRO 
 
```

Run Java App with `--add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED`