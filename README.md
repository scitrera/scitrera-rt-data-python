# scitrera-rt-data

Various utilities to support various data applications, including key-value stores, locks, serialization, and SQL helpers.

## Features

- **Key-Value Stores**: Redis compatible data stores (and In-Memory implementations for tests).
- **TKVT (Topic-Key-Value-Timestamp)**: A common abstraction over Kafka-like streams (Kafka, RabbitMQ Streams, Redis Streams, etc.) providing a unified interface for stream-based data.
- **Locks**: Distributed locks using Redis (and In-Process locks for tests).
- **SQL Utilities**: Helpers for SQLAlchemy and asyncpg.
- **Serialization**: Quick and dirty serialization/deserialization support for JSON, Msgpack, and Pickle.
- **Date/Time Utilities**: Convenient wrappers around Pandas, pytz, and dateutil. The pandas Timestamp is the considered the authoritative datetime type--and various functions help convert between it and other formats.

## Notes
- The different TKVT implementations are at different stages of development.
- A lot of functionality is still rough around the edges; just enough to get the job done for now.

## Installation

```bash
pip install scitrera-rt-data
```

For specific features, you can install extras:

```bash
pip install scitrera-rt-data[all]
pip install scitrera-rt-data[redis]
pip install scitrera-rt-data[sql]
pip install scitrera-rt-data[rmq]
pip install scitrera-rt-data[kafka]

pip install scitrera-rt-data[dev]
```

## License

3-Clause BSD License. See [LICENSE](LICENSE) for details.
