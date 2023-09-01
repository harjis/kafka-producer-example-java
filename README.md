# How to run

```shell
docker-compose up --build
```

When you run this you should see this output:
```
producer_1   | sent message to topic:test-topic partition:0  offset:0
producer_1   | sent message to topic:test-topic partition:0  offset:1
producer_1   | sent message to topic:test-topic partition:0  offset:2
...
```

After this you can verify that messages really are in the topic:
```shell
./print-topic-from-beginning.sh
```
