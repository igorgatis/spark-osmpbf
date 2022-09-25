// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.util.SerializableConfiguration;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.StreamSplitter;

class OsmPbfPartitionReader implements PartitionReader<InternalRow> {

  private static final String PRIMITIVE_TYPE = "OSMData";

  private final SerializableConfiguration configuration;
  private final OsmPbfPartition partition;

  private Iterator<InternalRow> queue;

  private final RawBlobParser parser;
  private FSDataInputStream stream;
  private StreamSplitter streamSplitter;

  OsmPbfPartitionReader(SerializableConfiguration configuration, OsmPbfPartition partition) {
    this.configuration = configuration;
    this.partition = partition;
    this.queue = Collections.emptyIterator();
    RowConverter converter = new RowConverter(
        partition.schema, partition.filePath, partition.tagsAsMap, partition.wayNodesAsIdList);
    this.parser = new RawBlobParser(partition.types, converter);
  }

  private void openFile() throws IOException {
    Path path;
    try {
      path = new Path(new URI(partition.filePath));
    } catch (URISyntaxException e) {
      throw new IOException(e.getMessage());
    }
    FileSystem fs = path.getFileSystem(configuration.value());
    FileStatus status = fs.getFileStatus(path);
    stream = fs.open(status.getPath());

    stream.seek(partition.offset);
    streamSplitter = new StreamSplitter(stream) {
      @Override
      public boolean hasNext() {
        try {
          if (stream.getPos() < partition.offset + partition.length) {
            return super.hasNext();
          }
        } catch (IOException e) {
        }
        return false;
      }
    };
  }

  @Override
  public boolean next() throws IOException {
    if (stream == null) {
      openFile();
    }
    if (!queue.hasNext()) {
      while (streamSplitter.hasNext()) {
        RawBlob blob = streamSplitter.next();
        if (blob.getType().equalsIgnoreCase(PRIMITIVE_TYPE)) {
          queue = parser.parse(blob.getData());
          if (queue.hasNext()) {
            break;
          }
        }
      }
    }
    return queue.hasNext();
  }

  @Override
  public InternalRow get() {
    return queue.next();
  }

  @Override
  public void close() throws IOException {
    if (stream != null) {
      stream.close();
    }
  }
}

