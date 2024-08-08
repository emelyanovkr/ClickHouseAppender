# ClickHouseAppender

This is a custom Log4j 2 appender that sends LOGGER messages to a clickhouse database in a specified table in JSON format. Table must have 2 columns:
```
CREATE TABLE log_table (
	timestamp DateTime PRIMARY KEY,
	log String
	) Engine = MergeTree()
```

All messages are stored in the [buffer](https://github.com/emelyanovkr/ClickHouseAppender/blob/main/src/main/java/com/clickhouse/appender/manager/LogBufferManager.java#L13C42-L13C56).
Log messages will be flushed straight to the ClickHouse DB after **one of the conditions**:
- timeout;
- buffer size limit exceeded;
- shutdown JVM ([SHUTDOWN-THREAD](https://github.com/emelyanovkr/ClickHouseAppender/blob/main/src/main/java/com/clickhouse/appender/manager/LogBufferManager.java#L53C7-L56C25)

For acquiring a connection to the ClickHouse DB used a JavaClient API ([ClickHouse Java API](https://github.com/ClickHouse/clickhouse-java)).

### Configuring example
```
        <ClickHouseAppender name="ClickHouseAppender" ignoreExceptions="false"
                            bufferSize="8192"
                            timeoutSec="5"
                            tableName="log_table">

            <ConnectionSettings HOST=""
                                PORT=""
                                USERNAME=""
                                PASSWORD=""
                                DATABASE="log_db"
                                SSL="true" --> must be true for a ClickHouse Connection
                                SOCKET_TIMEOUT="300000"
            />
```
