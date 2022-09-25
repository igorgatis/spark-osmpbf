// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import org.apache.spark.sql.SparkSession;

public final class Main {

  private Main() {
  }

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
        .builder()
        .appName("spark-osmpbf")
        .config("spark.master", "local[4]")
        .config("spark.executor.memory", "4gb")
        .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    spark.read()
        .format(OsmPbfOptions.FORMAT)
        .options(new OsmPbfOptions().toMap())
        .load("spark-osmpbf/src/test/resources/sample.pbf")
        .groupBy("entity_type").count().show();

    spark.read()
        .format(OsmPbfOptions.FORMAT)
        .options(new OsmPbfOptions().toMap())
        .load("spark-osmpbf/src/test/resources/district-of-columbia-latest.osm.pbf")
        .groupBy("entity_type").count().show();
  }
}
