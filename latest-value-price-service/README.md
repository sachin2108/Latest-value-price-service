
# Latest-Value Price Service

## Overview
In-memory Java service that tracks last prices for financial instruments with atomic batch visibility.

## Build & Test
```bash
mvn clean test
```

## Key Guarantees
- Atomic batch publication
- No partial reads
- Thread-safe parallel uploads
- Last-value semantics based on asOf timestamp

## Java Version
Java 17+
