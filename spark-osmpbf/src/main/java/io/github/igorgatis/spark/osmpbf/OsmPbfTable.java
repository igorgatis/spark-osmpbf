// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import java.util.Collections;
import java.util.Set;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

class OsmPbfTable implements Table, SupportsRead, ScanBuilder, SupportsPushDownRequiredColumns {

  private final SparkSession sparkSession;
  private CaseInsensitiveStringMap optionsMap;
  private StructType requiredSchema;

  OsmPbfTable(SparkSession sparkSession, CaseInsensitiveStringMap optionsMap,
      StructType schema) {
    this.sparkSession = sparkSession;
    this.optionsMap = optionsMap;
    this.requiredSchema = schema;
  }

  @Override
  public String name() {
    return "osmpbf";
  }

  @Override
  public StructType schema() {
    return requiredSchema;
  }

  @Override
  public Set<TableCapability> capabilities() {
    return Collections.singleton(TableCapability.BATCH_READ);
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    optionsMap = options;
    return this;
  }

  @Override
  public void pruneColumns(StructType schema) {
    this.requiredSchema = schema;
  }

  @Override
  public Scan build() {
    return new OsmPbfScan(sparkSession, optionsMap, requiredSchema);
  }
}
