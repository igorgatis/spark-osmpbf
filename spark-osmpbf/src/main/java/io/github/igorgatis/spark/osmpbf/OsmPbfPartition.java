// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import java.util.HashSet;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.types.StructType;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;

class OsmPbfPartition implements InputPartition {

  /** Identifies current partition among all planned partition. */
  int id;

  /** Fields to read. */
  StructType schema;

  /** Entity types to read. */
  HashSet<EntityType> types;

  /** Path of the file to be read. */
  String filePath;

  /** Partition starting point in bytes. */
  long offset;

  /** Partition size in bytes. */
  int length;

  /** Whether to load tags as map instead of an array. */
  boolean tagsAsMap;

  /** Whether to load nodes as a list of ids. */
  boolean wayNodesAsIdList;
}
