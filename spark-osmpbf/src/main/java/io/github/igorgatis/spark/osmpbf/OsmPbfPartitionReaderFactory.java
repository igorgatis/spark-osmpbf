// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.util.SerializableConfiguration;

class OsmPbfPartitionReaderFactory implements PartitionReaderFactory {

  private final SerializableConfiguration configuration;

  OsmPbfPartitionReaderFactory(SerializableConfiguration configuration) {
    this.configuration = configuration;
  }

  @Override
  public PartitionReader<InternalRow> createReader(InputPartition partition) {
    return new OsmPbfPartitionReader(configuration, (OsmPbfPartition) partition);
  }
}
