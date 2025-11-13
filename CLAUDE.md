# CLAUDE.md - AI Assistant Guide

## Project Overview

This is the companion repository for the "Kafka for Beginners" course by Conduktor. It contains comprehensive Java examples demonstrating Apache Kafka client APIs, including producers, consumers, and Kafka Streams applications.

**Course Link**: https://links.datacumulus.com/apache-kafka-coupon

**Repository Purpose**: Educational code examples for learning Apache Kafka fundamentals

## Technology Stack

- **Language**: Java
- **Build Tool**: Gradle
- **Kafka Version**: 3.3.1 (kafka-clients)
- **Java Version**: 11+ (recommended: Amazon Corretto 11.0.21)
- **Logging**: SLF4J (1.7.36 / 2.0.5)
- **External Services**: OpenSearch, Wikimedia SSE API

## Repository Structure

```
udemy-kafka-for-beginners-code/
├── kafka-basics/                      # Core Kafka producer/consumer examples
│   └── src/main/java/io/conduktor/demos/kafka/
│       ├── ProducerDemo.java          # Basic producer
│       ├── ProducerDemoWithCallback.java
│       ├── ProducerDemoKeys.java      # Producer with message keys
│       ├── ConsumerDemo.java          # Basic consumer
│       ├── ConsumerDemoCooperative.java
│       ├── ConsumerDemoWithShutdown.java
│       └── advanced/                  # Advanced consumer patterns
│           ├── ConsumerDemoAssignSeek.java
│           ├── ConsumerDemoThreads.java
│           └── ConsumerDemoRebalanceListener.java
│
├── kafka-producer-wikimedia/          # Real-world producer example
│   └── src/main/java/io/conduktor/demos/kafka/wikimedia/
│       ├── WikimediaChangesProducer.java
│       └── WikimediaChangeHandler.java
│
├── kafka-consumer-opensearch/         # Real-world consumer example
│   ├── src/main/java/io/conduktor/demos/kafka/opensearch/
│   │   └── OpenSearchConsumer.java
│   └── docker-compose.yml             # OpenSearch + Dashboards setup
│
├── kafka-streams-wikimedia/           # Kafka Streams examples
│   └── src/main/java/io/conduktor/demos/kafka/streams/wikimedia/
│       ├── WikimediaStreamsApp.java
│       └── processor/
│           ├── BotCountStreamBuilder.java
│           ├── WebsiteCountStreamBuilder.java
│           └── EventCountTimeseriesBuilder.java
│
├── code-config/                       # Setup scripts and configurations
│   ├── 0-start-kafka/                 # Platform-specific Kafka setup
│   │   ├── linux/
│   │   ├── macosx/
│   │   └── windows/
│   ├── 1-kafka-cli/                   # Kafka CLI command examples
│   ├── 2-kafka-extended/              # Kafka Connect configurations
│   └── 3-kafka-topic-configurations/  # Topic config examples
│
├── conduktor-platform/                # Conduktor Platform docker setup
│   ├── docker-compose.yml             # Full Kafka cluster + Conduktor
│   └── platform-config.yml
│
├── build.gradle                       # Root build configuration
├── settings.gradle                    # Multi-module project settings
└── info.txt                          # Kafka setup commands reference
```

## Module Descriptions

### 1. kafka-basics
**Purpose**: Foundational Kafka producer and consumer examples

**Key Files**:
- `ProducerDemo.java` - Basic fire-and-forget producer
- `ProducerDemoWithCallback.java` - Producer with callback for metadata
- `ProducerDemoKeys.java` - Producer demonstrating message key usage for partitioning
- `ConsumerDemo.java` - Basic consumer with auto-commit
- `ConsumerDemoCooperative.java` - Cooperative rebalancing consumer
- `ConsumerDemoWithShutdown.java` - Graceful shutdown handling
- `advanced/ConsumerDemoAssignSeek.java` - Manual partition assignment and seeking
- `advanced/ConsumerDemoThreads.java` - Multi-threaded consumer pattern
- `advanced/ConsumerDemoRebalanceListener.java` - Custom rebalance listener

**Dependencies**:
- kafka-clients:3.3.1
- slf4j-api:1.7.36
- slf4j-simple:1.7.36

### 2. kafka-producer-wikimedia
**Purpose**: Real-world producer example consuming Wikimedia's SSE stream

**Architecture**:
- Connects to Wikimedia's real-time recent changes stream
- Uses Server-Sent Events (SSE) to receive live updates
- Produces each change event to a Kafka topic

**Key Files**:
- `WikimediaChangesProducer.java` - Main producer application
- `WikimediaChangeHandler.java` - SSE event handler

**Dependencies**:
- kafka-clients:3.3.1
- okhttp:4.9.3 (HTTP client)
- okhttp-eventsource:2.5.0 (SSE support)

### 3. kafka-consumer-opensearch
**Purpose**: Real-world consumer example writing to OpenSearch

**Architecture**:
- Consumes from Kafka topic (e.g., wikimedia.recentchange)
- Indexes documents into OpenSearch
- Demonstrates idempotent consumer pattern

**Key Files**:
- `OpenSearchConsumer.java` - Consumer with OpenSearch integration
- `docker-compose.yml` - OpenSearch + Dashboards local setup

**Dependencies**:
- kafka-clients:3.3.1
- opensearch-rest-high-level-client:1.3.2
- gson:2.9.0 (JSON parsing)

**OpenSearch Access**:
- OpenSearch: http://localhost:9200
- Dashboards: http://localhost:5601

### 4. kafka-streams-wikimedia
**Purpose**: Kafka Streams processing examples

**Processors**:
- `BotCountStreamBuilder.java` - Count bot vs. non-bot edits
- `WebsiteCountStreamBuilder.java` - Count edits by website
- `EventCountTimeseriesBuilder.java` - Time-series event counting

**Dependencies**:
- kafka-streams:3.3.1
- jackson-databind:2.13.3 (JSON processing)

## Development Setup

### Prerequisites

1. **Java 11+** (Amazon Corretto recommended)
   ```bash
   sdk install java 11.0.21-amzn
   sdk use java 11.0.21-amzn
   ```

2. **Apache Kafka** (3.6.1 or compatible)
   - See `code-config/0-start-kafka/` for platform-specific setup
   - Supports both KRaft and ZooKeeper modes

3. **Gradle** (wrapper included - use `./gradlew`)

### Kafka Setup Options

#### Option 1: Local Kafka (Recommended for learning)
```bash
# Follow scripts in code-config/0-start-kafka/{linux|macosx|windows}/
# Example for Linux:
./code-config/0-start-kafka/linux/linux-start-kafka-kraft.sh
```

#### Option 2: Conduktor Platform (Docker)
```bash
cd conduktor-platform
docker-compose up -d
```
This starts:
- Kafka broker (localhost:9092)
- ZooKeeper (localhost:2181)
- Schema Registry (localhost:8081)
- Conduktor Platform UI (localhost:8080)

#### Option 3: Conduktor Playground (Cloud)
- Sign up at https://www.conduktor.io
- Use credentials in Java code (replace placeholders in producer/consumer)

### Building the Project

```bash
# Build all modules
./gradlew build

# Build specific module
./gradlew :kafka-basics:build

# Clean build
./gradlew clean build

# List all tasks
./gradlew tasks
```

### Running Examples

```bash
# Run from IDE (IntelliJ IDEA, Eclipse, VS Code)
# or use Gradle:

./gradlew :kafka-basics:run
./gradlew :kafka-producer-wikimedia:run
./gradlew :kafka-consumer-opensearch:run
./gradlew :kafka-streams-wikimedia:run
```

## Key Conventions and Patterns

### 1. Package Structure
All Java code follows the package naming:
```
io.conduktor.demos.kafka[.module][.component]
```

### 2. Logger Pattern
Every class uses SLF4J logger:
```java
private static final Logger log = LoggerFactory.getLogger(ClassName.class.getSimpleName());
```

### 3. Configuration Pattern
Kafka properties are configured programmatically using `java.util.Properties`:
```java
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("key.serializer", StringSerializer.class.getName());
properties.setProperty("value.serializer", StringSerializer.class.getName());
```

### 4. Connection Patterns
Code demonstrates two connection methods:

**Localhost** (default, commented):
```java
properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
```

**Conduktor Playground** (SASL_SSL):
```java
properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
properties.setProperty("security.protocol", "SASL_SSL");
properties.setProperty("sasl.jaas.config", "...");
properties.setProperty("sasl.mechanism", "PLAIN");
```

### 5. Resource Management
Always close resources properly:
```java
producer.flush();
producer.close();

consumer.close();
```

### 6. Error Handling
Use callbacks for async operations:
```java
producer.send(record, (metadata, exception) -> {
    if (exception != null) {
        log.error("Error producing", exception);
    } else {
        log.info("Received metadata: " + metadata);
    }
});
```

## Configuration Files

### code-config/1-kafka-cli/
Contains shell scripts demonstrating Kafka CLI commands:
- `0-kafka-topics.sh` - Topic creation, listing, describing, deletion
- `1-kafka-console-producer.sh` - Console producer examples
- `2-kafka-console-consumer.sh` - Console consumer examples
- `3-kafka-console-consumer-in-groups.sh` - Consumer group examples
- `4-kafka-consumer-groups.sh` - Consumer group management
- `5-reset-offsets.sh` - Offset reset strategies

### code-config/2-kafka-extended/
Kafka Connect configurations:
- `config/connect-standalone.properties`
- `config/wikimedia.properties` - Wikimedia source connector
- `config/elasticsearch.properties` - Elasticsearch sink connector

### info.txt
Quick reference for Kafka installation and startup commands.

## Common AI Assistant Tasks

### Adding New Producer Example
1. Create new Java class in `kafka-basics/src/main/java/io/conduktor/demos/kafka/`
2. Follow naming convention: `ProducerDemo<Feature>.java`
3. Copy configuration pattern from existing producers
4. Add appropriate dependencies to `kafka-basics/build.gradle` if needed
5. Use SLF4J logger pattern
6. Ensure proper resource cleanup (flush/close)

### Adding New Consumer Example
1. Create in `kafka-basics/src/main/java/io/conduktor/demos/kafka/`
2. Follow naming: `ConsumerDemo<Feature>.java`
3. Set appropriate consumer properties (group.id, auto.offset.reset)
4. Implement graceful shutdown if needed
5. Consider advanced patterns (in `advanced/` subdirectory)

### Modifying Kafka Streams Application
1. Work in `kafka-streams-wikimedia/src/main/java/`
2. Create new stream builder in `processor/` directory
3. Register in `WikimediaStreamsApp.java`
4. Test with wikimedia.recentchange topic
5. Ensure JSON parsing with Jackson

### Updating Dependencies
1. Modify relevant `build.gradle` file
2. Keep kafka-clients version consistent across modules
3. Test compatibility with Kafka broker version
4. Run `./gradlew dependencies` to verify resolution

### Debugging Connection Issues
1. Check bootstrap.servers configuration
2. Verify Kafka broker is running:
   ```bash
   netstat -tulnp | grep 9092
   ps -ef | grep kafka
   ```
3. For Conduktor Playground, verify credentials
4. Check firewall/network settings
5. Review logs with SLF4J simple logger

## Testing

### Manual Testing
1. Start Kafka cluster (local or Docker)
2. Create topics using CLI scripts in `code-config/1-kafka-cli/`
3. Run producer application
4. Run consumer application in separate terminal
5. Verify message flow

### OpenSearch Consumer Testing
1. Start OpenSearch:
   ```bash
   cd kafka-consumer-opensearch
   docker-compose up -d
   ```
2. Run wikimedia producer to generate data
3. Run OpenSearch consumer
4. Verify in Dashboards: http://localhost:5601

### Kafka Streams Testing
1. Ensure source topic has data (run wikimedia producer)
2. Run streams application
3. Consume from output topics to verify transformations

## Common Issues and Solutions

### Issue: Connection refused to localhost:9092
**Solution**: Start Kafka broker using scripts in `code-config/0-start-kafka/`

### Issue: Topic not found
**Solution**: Create topic using CLI:
```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create --topic <topic-name>
```

### Issue: Consumer not receiving messages
**Solution**:
- Check consumer group offset (might be at end)
- Set `auto.offset.reset=earliest` for testing
- Verify topic has messages: `kafka-console-consumer.sh --from-beginning`

### Issue: Gradle build fails
**Solution**:
- Verify Java version: `java -version`
- Clean build: `./gradlew clean build`
- Check internet connection (Maven Central access)

### Issue: OpenSearch container fails
**Solution**:
- Increase Docker memory (4GB minimum)
- Check port conflicts (9200, 5601)
- Review logs: `docker-compose logs opensearch`

## Important Notes for AI Assistants

1. **Credentials**: Replace placeholder credentials in code before running:
   - Search for `"your-username"` and `"your-password"`
   - Update with actual Conduktor Playground credentials

2. **Bootstrap Servers**: Code examples show multiple connection options (commented/uncommented):
   - Localhost: `127.0.0.1:9092`
   - Conduktor Playground: `cluster.playground.cdkt.io:9092`
   - Choose appropriate one for environment

3. **Idempotent Patterns**: When implementing consumers:
   - Use unique document IDs for idempotency
   - Handle duplicates gracefully
   - Consider at-least-once vs. exactly-once semantics

4. **Resource Cleanup**: Always ensure:
   - Producers call `flush()` and `close()`
   - Consumers call `close()` (preferably in try-finally or try-with-resources)
   - Proper shutdown hooks for graceful termination

5. **Module Isolation**: Each module is independent:
   - Has its own `build.gradle`
   - Can be built/run separately
   - Dependencies are not shared (intentionally duplicated)

6. **Configuration Management**:
   - No external config files used (all programmatic)
   - Properties set directly in Java code
   - This is for educational clarity, not production practice

7. **Logging**: All logging uses SLF4J Simple:
   - Console output only
   - No log files
   - Adjust log level in code if needed

## External Resources

- **Kafka Documentation**: https://kafka.apache.org/documentation/
- **Conduktor**: https://www.conduktor.io
- **Kafkademy**: https://kafkademy.com/ (Free Kafka learning)
- **Course**: https://links.datacumulus.com/apache-kafka-coupon
- **OpenSearch Docs**: https://opensearch.org/docs/latest/
- **Wikimedia Stream**: https://stream.wikimedia.org/v2/stream/recentchange

## Git Workflow

This repository follows a simple educational structure:
- Main branch contains stable course code
- Feature branches for experiments prefixed with `claude/`
- Commit messages should be descriptive of learning examples
- No CI/CD pipeline (educational repo)

## Version Information

- **Project Version**: 1.0-SNAPSHOT
- **Kafka Clients**: 3.3.1
- **Kafka Streams**: 3.3.1
- **Conduktor Platform**: 1.18.1
- **OpenSearch**: 1.2.4 / 1.3.2
- **Last Updated**: 2025 (Based on recent commits)

---

**Maintained by**: Conduktor
**License**: Check repository LICENSE file
**Support**: Course Q&A, GitHub issues
