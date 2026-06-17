# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build

Requires JDK 21+ to compile (links against Elasticsearch 9.x). Output bytecode targets Java 17.

```bash
mvn clean package        # compile + create shaded JAR + ZIP distribution
mvn clean install        # same, plus install to local Maven repo
```

The release ZIP is written to `target/releases/germainapm-es-plugin.zip`. The maven-shade-plugin bundles the `t-digest` library directly into the JAR (elasticsearch is `provided` scope).

There are no tests in this repository — JUnit is declared as a dependency but no test classes exist.

## Architecture

This is an Elasticsearch aggregation plugin that adds two custom aggregation types for computing percentile values using [t-digest](https://github.com/tdunning/t-digest) data structures.

### Plugin Entry Point

`GermainPlugin` implements `SearchPlugin` and registers both aggregation specs with Elasticsearch at startup.

### Two Aggregation Types

**`digest`** — operates on `binary` fields with `doc_values: true`. Reads pre-serialized t-digest byte arrays from each document, deserializes them via `DigestByteMapper`, and merges them into a per-bucket `MergingDigest`.

**`rawdigest`** — operates on numeric fields. Reads raw `double` values and adds them directly to a per-bucket `MergingDigest`, building the digest from scratch during aggregation.

Both aggregations accept an optional `compression` parameter (default `100.0`) controlling the accuracy/size tradeoff of the t-digest.

### Standard ES Plugin Pattern

Each aggregation type follows the same four-class structure:

```
*AggregationBuilder   — parses the query DSL, holds configuration (field, compression)
*AggregatorFactory    — registered in ValuesSourceRegistry; creates aggregators
*Aggregator           — per-shard leaf reader; collects values into ObjectArray<MergingDigest>
InternalDigest        — shared result type; handles cross-shard reduction (merging digests)
```

`InternalDigest` is the single result class for both aggregation types. It wraps a `MergingDigest` and serializes/deserializes it as a base64-encoded binary value returned in the aggregation response.

`DigestByteMapper` is the serialization utility: converts between `byte[]`/base64 strings and `MergingDigest` instances.

### Versioning Convention

The plugin version tracks the Elasticsearch version it targets (e.g., `9.3.5` = ES 9.3.5). When upgrading ES versions, update `elasticsearch.version` in `pom.xml` and the `<version>` tag together.
