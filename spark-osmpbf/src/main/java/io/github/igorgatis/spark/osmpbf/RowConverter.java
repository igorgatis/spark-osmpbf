// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import io.github.igorgatis.spark.osmpbf.OsmPbfOptions.Columns;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapBuilder;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;

class RowConverter {

  private final ArrayList<Function<Entity, Object>> getters;

  RowConverter(StructType schema, String filePath, boolean tagsAsMap, boolean wayNodesAsIdList) {
    getters = new ArrayList<>();
    HashSet<String> fieldNames = new HashSet<>(Arrays.asList(schema.fieldNames()));
    if (fieldNames.contains(Columns.ENTITY_TYPE)) {
      getters.add(e -> UTF8String.fromString(e.getType().toString()));
    }
    if (fieldNames.contains(Columns.ID)) {
      getters.add(e -> e.getId());
    }
    if (fieldNames.contains(Columns.VERSION)) {
      getters.add(e -> e.getVersion());
    }
    if (fieldNames.contains(Columns.TIMESTAMP)) {
      getters.add(e -> e.getTimestamp().getTime());
    }
    if (fieldNames.contains(Columns.CHANGESET)) {
      getters.add(e -> e.getChangesetId());
    }
    if (fieldNames.contains(Columns.UID)) {
      getters.add(e -> e.getUser().getId());
    }
    if (fieldNames.contains(Columns.USER_SID)) {
      getters.add(e -> UTF8String.fromString(e.getUser().getName()));
    }
    if (fieldNames.contains(Columns.TAGS)) {
      if (tagsAsMap) {
        getters.add(e -> convertTagsAsMap(e));
      } else {
        getters.add(e -> convertTagsAsArray(e));
      }
    }
    if (fieldNames.contains(Columns.LATITUDE)) {
      getters.add(e -> {
        if (e.getType() == EntityType.Node) {
          return ((Node) e).getLatitude();
        }
        return null;
      });
    }
    if (fieldNames.contains(Columns.LONGITUDE)) {
      getters.add(e -> {
        if (e.getType() == EntityType.Node) {
          return ((Node) e).getLongitude();
        }
        return null;
      });
    }
    if (fieldNames.contains(Columns.NODES)) {
      if (wayNodesAsIdList) {
        getters.add(e -> convertNodesToIdList(e));
      } else {
        getters.add(e -> convertNodes(e));
      }
    }
    if (fieldNames.contains(Columns.MEMBERS)) {
      getters.add(e -> convertMembers(e));
    }
    if (fieldNames.contains(Columns.ORIGIN_FILE)) {
      getters.add(e -> UTF8String.fromString(filePath));
    }
  }

  public InternalRow convert(Entity entity) {
    GenericInternalRow row = new GenericInternalRow(getters.size());
    for (int i = 0; i < getters.size(); ++i) {
      row.update(i, getters.get(i).apply(entity));
    }
    return row;
  }

  private static GenericArrayData convertTagsAsArray(Entity entity) {
    if (entity.getTags().size() > 0) {
      ArrayList<GenericInternalRow> list = new ArrayList<>(entity.getTags().size());
      for (Tag tag : entity.getTags()) {
        GenericInternalRow row = new GenericInternalRow(2);
        row.update(0, UTF8String.fromString(tag.getKey()));
        row.update(1, UTF8String.fromString(tag.getValue()));
        list.add(row);
      }
      return new GenericArrayData(list.toArray());
    }
    return null;
  }

  private static ArrayBasedMapData convertTagsAsMap(Entity entity) {
    if (entity.getTags().size() > 0) {
      ArrayBasedMapBuilder mapBuilder =
          new ArrayBasedMapBuilder(DataTypes.StringType, DataTypes.StringType);
      for (Tag tag : entity.getTags()) {
        mapBuilder.put(UTF8String.fromString(tag.getKey()),
            UTF8String.fromString(tag.getValue()));
      }
      return mapBuilder.build();
    }
    return null;
  }

  private static GenericArrayData convertNodes(Entity entity) {
    if (entity.getType() == EntityType.Way) {
      List<WayNode> nodes = ((Way) entity).getWayNodes();
      ArrayList<GenericInternalRow> list = new ArrayList<>(nodes.size());
      int index = 0;
      for (WayNode node : nodes) {
        GenericInternalRow row = new GenericInternalRow(2);
        row.update(0, index++);
        row.update(1, node.getNodeId());
        list.add(row);
      }
      return new GenericArrayData(list.toArray());
    }
    return null;
  }

  private static GenericArrayData convertNodesToIdList(Entity entity) {
    if (entity.getType() == EntityType.Way) {
      List<WayNode> nodes = ((Way) entity).getWayNodes();
      long[] array = new long[nodes.size()];
      int index = 0;
      for (WayNode node : nodes) {
        array[index++] = node.getNodeId();
      }
      return new GenericArrayData(array);
    }
    return null;
  }

  private static GenericArrayData convertMembers(Entity entity) {
    if (entity.getType() == EntityType.Relation) {
      List<RelationMember> members = ((Relation) entity).getMembers();
      ArrayList<GenericInternalRow> list = new ArrayList<>(members.size());
      for (RelationMember member : members) {
        GenericInternalRow row = new GenericInternalRow(3);
        row.update(0, member.getMemberId());
        row.update(1, UTF8String.fromString(member.getMemberType().name()));
        row.update(2, UTF8String.fromString(member.getMemberRole()));
        list.add(row);
      }
      return new GenericArrayData(list.toArray());
    }
    return null;
  }
}
