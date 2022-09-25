# spark-osmpbf
Spark Data Source for OpenStreetMap Protobuf (aka [OSM PBF](https://wiki.openstreetmap.org/wiki/PBF_Format)) files.

This library was heavily inspired by:
* [spark-osm-datasource](https://github.com/woltapp/spark-osm-datasource)
* [osm-parquetizer](https://github.com/adrianulbona/osm-parquetizer)
* [Osmosis](https://github.com/openstreetmap/osmosis)

The main differences between `spark-osmpbf` and `spark-osm-datasource` are:
* `spark-osmpbf` processes the input in the main thread while
`spark-osm-datasource` is multithreaded.
* `spark-osmpbf` applies early stage filtering based on selected OSM entities.

## Usage
Include `spark-osmpbf` dependency and write:
```scala
val df = spark.read
    .format("io.github.igorgatis.spark.osmpbf")
    .load("path/to/file.pbf")
```

## Schema
The `spark-osmpbf` default schema was based on `osm-parquetizer`. It includes
an extra `entity_type` string field which is self-explanatory:

```
root
 |-- entity_type: string (nullable = false)
 |-- id: long (nullable = false)
 |-- version: integer (nullable = true)
 |-- timestamp: long (nullable = true)
 |-- changeset: long (nullable = true)
 |-- uid: integer (nullable = true)
 |-- user_sid: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- nodes: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- index: integer (nullable = false)
 |    |    |-- nodeId: long (nullable = false)
 |-- members: array (nullable = true)
 |    |-- element: struct (containsNull = false)
 |    |    |-- member_id: long (nullable = false)
 |    |    |-- role: string (nullable = true)
```

If you use `osm-parquetizer`, you can probably use `spark-osmpbf` without
changing much code, reading directly from `.pbf` files.

## OsmPbfOptions
`spark-osmpbf` comes with `OsmPbfOptions` class which allows easy
configuration. In the example below, dataset will read only `Node` entities,
it will ignore metadata fields such as `version`, `changeset`, etc. It will
also read tags as a map.

```scala
val df = spark.read
   .format(OsmPbfOptions.FORMAT)
   .options(new OsmPbfOptions
      .withType("node")
      .withTagsAsMap(true)
      .withExcludeMetadata(true)
      .toMap)
   .load("path/to/file.pbf")

df.printSchema
```
Here is the output schema:
```
root
 |-- entity_type: string (nullable = false)
 |-- id: long (nullable = false)
 |-- tags: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
```

