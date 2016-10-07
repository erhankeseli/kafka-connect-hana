Configuration for Source & Sink
===============================

```
name: connection.url
description: HANA DB Jdbc url for source & sink
This is a mandatory parameter.
```

```
name: connection.user
description: HANA DB Jdbc user for source & sink
This is a mandatory parameter.
```

```
name: connection.password
description: HANA DB Jdbc password for source & sink
This is a mandatory parameter.
```

```
name: topics
description: List of topics to be published or subscribed by source or sink
This is a mandatory parameter.
```

```
name: batch.size
description: Batch size for sink. Should be an integer.
Default is 3000.
```

```
name: max.retries
description: Max retries for sink. Should be an integer.
Default is 10.
```

```
name: auto.create
description: Auto create HANA tables if table is not found for Sink.
Default is false.
```

```
name: batch.max.rows
description: Max rows to include in a single batch call for source. Should be an integer.
Default is 100.
```

```
name: mode
description: The mode for updating a table each time it is polled.
Default is 'bulk'.
Valid values are 'bulk', 'incrementing'.
```

```
name: {topic}.table.name
description: table name to fetch from or write to by source & sink
This is a mandatory parameter.
```

```
name: {topic}.pk.mode
description: primary key mode to be used by sink.
Default is 'none'.
Valid values are 'record_key', record_value'.
```

```
name: {topic}.pk.fields
description: comma-separated primary key fields to be used by sink
Default is ""
```

```
name: {topic}.poll.interval.ms
description: poll interval time to be used by source
Default value is 60000
```

```
name: {topic}.incrementing.column.name
description: incrementing column name to be used by source
This is mandatory when 'mode' is 'incrementing'.
```

```
name: {topic}.partition.count
description: topic partition count to be used by source.
Default value is 1.
```

```
name: {topic}.table.partition.mode
description: table partition mode to be used by sink
Default value is 'none'.
Valid values are 'none', 'hash', 'round_robin'.
```

```
name: {topic}.table.partition.count
description: table partition count to be used by sink
Default value is 0.
```

```
name: {topic}.table.type
description: table type to be used by sink
Default value is 'column'.
Valid values are 'column', 'row'.
```

Example Configurations
======================

- [Sink Connector Config](https://github.wdf.sap.corp/i033085/kafka-connect-hana/blob/master/config/kafka-connect-hana-sink.properties)
- [Source Connector Config](https://github.wdf.sap.corp/i033085/kafka-connect-hana/blob/master/config/kafka-connect-hana-source.properties)