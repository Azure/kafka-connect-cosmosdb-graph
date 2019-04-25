 Kafka Connect Cosmos DB Graph
This is a sink connector from [Apache Kafka](https://kafka.apache.org/documentation/#connect) into [Microsoft Azure Cosmos DB Graph](https://docs.microsoft.com/en-us/azure/cosmos-db/graph-introduction) account. It allows modelling events as vertices and edges of a graph and manipulating them using [Apache Tinkerpop Gremlin](https://tinkerpop.apache.org/gremlin.html) language.

This connector supports primitive, Binary, Json and Avro serializers.

# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

# Setup
Setup instructions are applicable after Confluent or Apache Kafka are up and running.
1. Clone the repository
2. Open root folder in terminal and execute
```maven
mvn package
```
This command will produce connector and dependencies. 
```
/target/dependencies/*.*
/target/kafka-connect-cosmosdb-graph-0.1.jar
```
3. Copy these dependencies into your Kafka cluster plugin folder. For Confluent platform please create a folder and copy all dependencies and connector:
```
<kafka path>/share/java/kafka-connect-gremlin
```
4. Restart your Connect worker process. It will discover new connector automatically by inspecting plugin folder.

# Configuration
To start using connector please open your Confluence Control Center and navigate to **Management** -> **Kafka Connect** -> **Send data out** -> **Add Connector**

![Confluence Command Center Add Connector](/doc/confluentaddconnector.JPG)

On the next page please select **KafkaGremlinSinkConnector**. If this connector is not available, likely Connect worker did not pick up the changes and it is recommended to restart worker again and let him finish directory scan before trying to add a connector again.

![Cosmos DB Graph Connector Configuration](/doc/gremlinconnectorconfig.png)

**host** - fully qualified domain name of gremlin account. Please specify DNS record in zone **gremlin.cosmos.azure.com** for public Azure. Please do not put **documents.azure.com**, it will not work.

**port** - default HTTPS port 443

**database** - this is database resource in Cosmos DB, not to be confused with global database account. This value appears in Data Explorer after "New Graph" is created. 

**container** - name of Cosmos DB collection that contains graph data.

![Cosmos DB Graph Connector Configuration](/doc/azureportaldatabasecontainer.jpg)

**traversal** - gremlin traversal to execute for every Kafka message published to the Kafka Topic and received by connector. Sample traversal could be adding a vertex for every event
```
g.addV()
 .property('id', '${value.uid}')
 .property('email', '${value.emailAddress}')
 .property('language', '${value.language}')
```

# References
It is worth looking through this material to get better understanding how this connector works and how to use it

[Kafka Connect Deep Dive](https://www.confluent.io/blog/kafka-connect-deep-dive-error-handling-dead-letter-queues)

[Kafka, Avro Serialization, and the Schema Registry](https://dzone.com/articles/kafka-avro-serialization-and-the-schema-registry)

[Spring Kafka - JSON Serializer Deserializer Example](https://codenotfound.com/spring-kafka-json-serializer-deserializer-example.html)
