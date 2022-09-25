// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.StreamSplitter;

/** General Benchmarks. */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1)
@State(Scope.Benchmark)
public class Benchmarks {

  private static class TestFile {

    String path;
    byte[] content;
  }

  private TestFile smallFile;
  private TestFile mediumFile;

  TestFile loadFileOrDie(String path) {
    TestFile file = new TestFile();
    file.path = path;
    try {
      file.content = Files.readAllBytes(new File(file.path).toPath());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return file;
  }

  /** Benchmark cases. */
  @Param({"RelationsOnly", "WaysOnly", "NodesOnly", "AllEntities"})
  public String optionsName;

  OsmPbfOptions newOptions() {
    switch (optionsName) {
      case "AllEntities":
        return new OsmPbfOptions();
      case "NodesOnly":
        return new OsmPbfOptions().withType(EntityType.Node);
      case "WaysOnly":
        return new OsmPbfOptions().withType(EntityType.Way);
      case "RelationsOnly":
        return new OsmPbfOptions().withType(EntityType.Relation);
      default:
        throw new RuntimeException("Unknown options: " + optionsName);
    }
  }

  @Setup
  public void setup() throws IOException {
    smallFile = loadFileOrDie("src/test/resources/sample.pbf");
    mediumFile = loadFileOrDie("src/test/resources/district-of-columbia-latest.osm.pbf");
  }

  @Benchmark
  @Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
  @Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS, time = 1000)
  public void testParsersSmallFile(Blackhole blackhole) {
    OsmPbfOptions options = newOptions();
    RawBlobParser parser = new RawBlobParser(options.getTypes(), newRowConverter(options));
    parseFile(parser, smallFile, blackhole);
  }

  @Benchmark
  @Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS, time = 10000)
  @Measurement(iterations = 10, timeUnit = TimeUnit.MILLISECONDS, time = 10000)
  public void testParsersMediumFile(Blackhole blackhole) {
    OsmPbfOptions options = newOptions();
    RawBlobParser parser = new RawBlobParser(options.getTypes(), newRowConverter(options));
    parseFile(parser, smallFile, blackhole);
  }

  private static void parseFile(RawBlobParser parser, TestFile file, Blackhole blackhole) {
    DataInputStream stream = new DataInputStream(
        new ByteArrayInputStream(file.content));
    StreamSplitter splitter = new StreamSplitter(stream);
    while (splitter.hasNext()) {
      RawBlob blob = splitter.next();
      if (blob.getType().equalsIgnoreCase("OSMData")) {
        Iterator<InternalRow> itr = parser.parse(blob.getData());
        while (itr.hasNext()) {
          blackhole.consume(itr.next());
        }
      }
    }
  }

  private static RowConverter newRowConverter(OsmPbfOptions options) {
    StructType schema = DefaultSource.getSchema(options);
    return new RowConverter(schema, "testfile",
        options.getTagsAsMap(), options.getWayNodesAsIdList());
  }

  public static void main(String[] args) throws Exception {
    Options opt = new OptionsBuilder()
        .include(Main.class.getSimpleName())
        .build();

    new Runner(opt).run();
  }
}
