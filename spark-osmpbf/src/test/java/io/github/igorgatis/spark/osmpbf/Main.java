// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public final class Main {

  private Main() {
  }

  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("spark-osmpbf")
        .config("spark.master", "local[4]")
        .config("spark.executor.memory", "4gb")
        .getOrCreate();

    Dataset<Row> df = spark.read()
        .format(OsmPbfOptions.FORMAT)
        .options(new OsmPbfOptions()
            .withTagsAsMap(true)
            .withExcludeMetadata(true)
            .toMap())
        .load("spark-osmpbf/src/test/resources/sample.pbf");

    df.printSchema();

    df.groupBy("entity_type").count().show();
  }
}
