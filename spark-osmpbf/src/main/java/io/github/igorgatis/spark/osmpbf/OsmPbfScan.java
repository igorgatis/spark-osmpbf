// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.util.SerializableConfiguration;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import scala.reflect.ClassTag;

class OsmPbfScan implements Scan, Batch {

  private final SparkSession sparkSession;
  private final CaseInsensitiveStringMap optionsMap;
  private final StructType schema;
  private final Broadcast<SerializableConfiguration> broadcastedConf;

  private InputPartition[] plannedPartitions;

  OsmPbfScan(SparkSession sparkSession, CaseInsensitiveStringMap optionsMap,
      StructType requiredSchema) {
    this.sparkSession = sparkSession;
    this.optionsMap = optionsMap;
    schema = requiredSchema;

    Configuration hadoopConf = this.sparkSession.sessionState().newHadoopConf();
    optionsMap.forEach((k, v) -> {
      if ((v != null) && k != OsmPbfOptions.PATH && k != OsmPbfOptions.PATHS) {
        hadoopConf.set(k, v);
      }
    });

    broadcastedConf = this.sparkSession.sparkContext()
        .broadcast(new SerializableConfiguration(hadoopConf),
            classTag(SerializableConfiguration.class));
  }

  @Override
  public StructType readSchema() {
    return schema;
  }

  @Override
  public Batch toBatch() {
    return this;
  }

  // Batch

  private long maxSplitBytes(FileStatus status) {
    SQLConf conf = sparkSession.sessionState().conf();
    long defaultMaxSplitBytes = conf.filesMaxPartitionBytes();
    long openCostInBytes = conf.filesOpenCostInBytes();
    long minPartitionNum = conf.filesMinPartitionNum()
        .getOrElse(() -> sparkSession.leafNodeDefaultParallelism());
    long totalBytes = status.getLen() + openCostInBytes;
    long bytesPerCore = totalBytes / minPartitionNum;
    return Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore));
  }

  private static <T> ClassTag<T> classTag(Class<T> clazz) {
    return scala.reflect.ClassManifestFactory.fromClass(clazz);
  }

  @Override
  public InputPartition[] planInputPartitions() {
    if (plannedPartitions != null) {
      return plannedPartitions;
    }

    ArrayList<OsmPbfPartition> partitions = new ArrayList<>();
    OsmPbfOptions options = OsmPbfOptions.parse(optionsMap);
    for (String filePath : options.getPaths()) {
      appendPartitions(options, filePath, partitions);
    }
    this.plannedPartitions = partitions.toArray(new InputPartition[partitions.size()]);
    return this.plannedPartitions;
  }

  private void appendPartitions(OsmPbfOptions options, String filePath,
      ArrayList<OsmPbfPartition> partitions) {
    FSDataInputStream dis = null;
    try {
      Path path = new Path(new URI(filePath));
      FileSystem fs = path.getFileSystem(broadcastedConf.getValue().value());
      FileStatus status = fs.getFileStatus(path);
      long splitSize = Math.min(maxSplitBytes(status), status.getLen() / options.getPartitions());
      dis = fs.open(status.getPath());

      boolean end = false;
      long offset = 0;
      while (!end) {
        OsmPbfPartition partition = new OsmPbfPartition();
        partition.id = partitions.size();
        partition.schema = schema;
        partition.types = options.getTypes();
        partition.filePath = filePath;
        partition.offset = offset;
        partition.tagsAsMap = options.getTagsAsMap();
        partition.wayNodesAsIdList = options.getWayNodesAsIdList();

        do {
          int headerLength = 0;
          try {
            headerLength = dis.readInt();
          } catch (EOFException e) {
            end = true;
            break;
          }
          byte[] headerBuffer = new byte[headerLength];
          dis.readFully(headerBuffer);

          Fileformat.BlobHeader blobHeader = Fileformat.BlobHeader.parseFrom(headerBuffer);
          dis.skipBytes(blobHeader.getDatasize());

          long newOffset = dis.getPos();
          partition.length = (int) (newOffset - offset);
        } while (partition.length < splitSize);

        partitions.add(partition);
        offset += partition.length;
      }
    } catch (IOException | URISyntaxException e) {
      throw new RuntimeException(e);
    } finally {
      if (dis != null) {
        try {
          dis.close();
        } catch (IOException e) {
        }
      }
    }
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    return new OsmPbfPartitionReaderFactory(broadcastedConf.getValue());
  }
}

