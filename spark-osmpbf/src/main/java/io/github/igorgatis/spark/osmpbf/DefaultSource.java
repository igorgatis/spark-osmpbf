// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import java.util.ArrayList;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableProvider;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import io.github.igorgatis.spark.osmpbf.OsmPbfOptions.Columns;
import io.github.igorgatis.spark.osmpbf.OsmPbfOptions.Columns.RelationFields;
import io.github.igorgatis.spark.osmpbf.OsmPbfOptions.Columns.TagFields;
import io.github.igorgatis.spark.osmpbf.OsmPbfOptions.Columns.WayFields;

public class DefaultSource implements TableProvider {

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap optionsMap) {
    return getSchema(OsmPbfOptions.parse(optionsMap));
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning,
      Map<String, String> properties) {
    return new OsmPbfTable(SparkSession.active(), new CaseInsensitiveStringMap(properties), schema);
  }

  private static StructType getSchema(OsmPbfOptions options) {
    ArrayList<StructField> fields = new ArrayList<>();

    Metadata noMeta = Metadata.empty();

    fields.add(new StructField(Columns.ENTITY_TYPE, DataTypes.StringType, false, noMeta));
    fields.add(new StructField(Columns.ID, DataTypes.LongType, false, noMeta));

    if (!options.getExcludeMetadata()) {
      fields.add(new StructField(Columns.VERSION, DataTypes.IntegerType, true, noMeta));
      fields.add(new StructField(Columns.TIMESTAMP, DataTypes.LongType, true, noMeta));
      fields.add(new StructField(Columns.CHANGESET, DataTypes.LongType, true, noMeta));
      fields.add(new StructField(Columns.UID, DataTypes.IntegerType, true, noMeta));
      fields.add(new StructField(Columns.USER_SID, DataTypes.StringType, true, noMeta));
    }

    if (!options.getExcludeTags()) {
      DataType tagsType;
      if (options.getTagsAsMap()) {
        tagsType = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
      } else {
        tagsType = DataTypes.createArrayType(
            new StructType(new StructField[]{
                new StructField(TagFields.KEY, DataTypes.StringType, false, noMeta),
                new StructField(TagFields.VALUE, DataTypes.StringType, true, noMeta),
            }), false);
      }
      fields.add(new StructField(Columns.TAGS, tagsType, true, noMeta));
    }

    if (options.containsType(EntityType.Node)) {
      fields.add(new StructField(Columns.LATITUDE, DataTypes.DoubleType, true, noMeta));
      fields.add(new StructField(Columns.LONGITUDE, DataTypes.DoubleType, true, noMeta));
    }

    if (options.containsType(EntityType.Way)) {
      DataType nodesType;
      if (options.getWayNodesAsIdList()) {
        nodesType = DataTypes.createArrayType(DataTypes.LongType);
      } else {
        nodesType = DataTypes.createArrayType(
            new StructType()
                .add(new StructField(WayFields.INDEX, DataTypes.IntegerType, false, noMeta))
                .add(new StructField(WayFields.NODE_ID, DataTypes.LongType, false, noMeta)),
            false);
      }
      fields.add(new StructField(Columns.NODES, nodesType, true, noMeta));
    }

    if (options.containsType(EntityType.Relation)) {
      ArrayType membersArrayType = DataTypes.createArrayType(
          new StructType()
              .add(new StructField(RelationFields.MEMBER_ID, DataTypes.LongType, false, noMeta))
              .add(new StructField(RelationFields.ROLE, DataTypes.StringType, true, noMeta))
              .add(new StructField(RelationFields.TYPE, DataTypes.StringType, true, noMeta)),
          false);
      fields.add(new StructField(Columns.MEMBERS, membersArrayType, true, noMeta));
    }

    if (options.getIncludeOriginFile()) {
      fields.add(new StructField(Columns.ORIGIN_FILE, DataTypes.StringType, true, noMeta));
    }

    return new StructType(fields.toArray(new StructField[0]));
  }
}

