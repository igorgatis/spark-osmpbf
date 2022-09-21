// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;

public class OsmPbfOptions implements Serializable {

  /** Fully qualified package name of this data source. */
  public static final String FORMAT = OsmPbfOptions.class.getPackage().getName();

  /** Option: file path. */
  public static final String PATH = "path";

  /** Option: file path. */
  public static final String PATHS = "paths";

  /** Option: number of partitions (min: 1, default: 1). */
  public static final String PARTITIONS = "partitions";

  /** Option: comma separated list of entity types (default: node,way,relation). */
  public static final String TYPES = "types";

  /** Option: whether to exclude metadata fields (default: false). */
  public static final String EXCLUDE_METADATA = "exclude-metadata";

  /** Option: whether to exclude tags (default: false). */
  public static final String EXCLUDE_TAGS = "exclude-tags";

  /** Option: whether to load tags as map (default: false). */
  public static final String TAGS_AS_MAP = "tags-as-map";

  /** Option: whether to load way nodes as id list (default: false). */
  public static final String WAY_NODES_AS_ID_LIST = "way-nodes-as-id-list";

  /** Option: whether to include origin file name (default: false). */
  public static final String INCLUDE_ORIGIN_FILE = "include-origin-file";

  /** List of Entity columns. */
  public interface Columns {
    /** Entity Type. */
    String ENTITY_TYPE = "entity_type";
    /** Entity Id. */
    String ID = "id";

    /** Entity version. */
    String VERSION = "version";
    /** Entity version timestamp. */
    String TIMESTAMP = "timestamp";
    /** Entity version changeset. */
    String CHANGESET = "changeset";
    /** Entity version user id. */
    String UID = "uid";
    /** Entity version user name. */
    String USER_SID = "user_sid";

    /** Entity tags. */
    String TAGS = "tags";

    /** Tag fields. */
    interface TagFields {
      /** Tag key. */
      String KEY = "key";
      /** Tag value. */
      String VALUE = "value";

      /** stylecheck ignore. */
      void stylecheckIgnore();
    }

    /** Node latitude column. */
    String LATITUDE = "latitude";
    /** Node longitude column. */
    String LONGITUDE = "longitude";

    /** Way column. */
    String NODES = "nodes";

    /** Way fields. */
    interface WayFields {
      /** Node index. */
      String INDEX = "index";
      /** Node ID. */
      String NODE_ID = "nodeId";
      /** Node latitude. */
      String LATITUDE = "latitude";
      /** Node longitude. */
      String LONGITUDE = "longitude";

      /** stylecheck ignore. */
      void stylecheckIgnore();
    }

    /** Relation column. */
    String MEMBERS = "members";

    /** Relation fields. */
    interface RelationFields {
      /** Entity member ID. */
      String MEMBER_ID = "member_id";
      /** Relation role. */
      String ROLE = "role";
      /** Relation type. */
      String TYPE = "type";

      /** stylecheck ignore. */
      void stylecheckIgnore();
    }

    /** Entity version. */
    String ORIGIN_FILE = "origin-file";

    /** stylecheck ignore. */
    void stylecheckIgnore();
  }

  private static final HashSet<EntityType> ALLOWED_TYPES;

  static {
    ALLOWED_TYPES = new HashSet<>();
    ALLOWED_TYPES.add(EntityType.Node);
    ALLOWED_TYPES.add(EntityType.Way);
    ALLOWED_TYPES.add(EntityType.Relation);
  }

  /** Comma Separated List of default Entity Types. */
  public static final String DEFAULT_TYPES = toCsv(ALLOWED_TYPES);

  private ArrayList<String> paths;
  private int partitions;
  private final HashSet<EntityType> types;
  private boolean excludeMetadata;
  private boolean excludeTags;
  private boolean tagsAsMap;
  private boolean wayNodesAsIdList;
  private boolean includeOriginFile;

  public OsmPbfOptions() {
    paths = new ArrayList<>();
    partitions = 1;
    types = new HashSet<>();
    excludeMetadata = false;
    excludeTags = false;
  }

  public ArrayList<String> getPaths() {
    return paths;
  }

  public OsmPbfOptions withPartitions(int value) {
    partitions = Math.max(1, value);
    return this;
  }

  public int getPartitions() {
    return partitions;
  }

  public HashSet<EntityType> getTypes() {
    return types;
  }

  public OsmPbfOptions withType(EntityType value) {
    types.add(value);
    return this;
  }

  public OsmPbfOptions withType(String value) {
    types.add(findEntityTypeOrDie(value));
    return this;
  }

  public boolean containsType(EntityType type) {
    return types.isEmpty() || types.contains(type);
  }

  public OsmPbfOptions withExcludeMetadata(boolean value) {
    excludeMetadata = value;
    return this;
  }

  public boolean getExcludeMetadata() {
    return excludeMetadata;
  }

  public OsmPbfOptions withExcludeTags(boolean value) {
    excludeTags = value;
    return this;
  }

  public boolean getExcludeTags() {
    return excludeTags;
  }

  public OsmPbfOptions withTagsAsMap(boolean value) {
    tagsAsMap = value;
    return this;
  }

  public boolean getTagsAsMap() {
    return tagsAsMap;
  }

  public OsmPbfOptions withWayNodesAsIdList(boolean value) {
    wayNodesAsIdList = value;
    return this;
  }

  public boolean getWayNodesAsIdList() {
    return wayNodesAsIdList;
  }

  public OsmPbfOptions withIncludeOriginFile(boolean value) {
    includeOriginFile = value;
    return this;
  }

  public boolean getIncludeOriginFile() {
    return includeOriginFile;
  }

  public Map<String, String> toMap() {
    HashMap<String, String> map = new HashMap<>();
    if (paths != null && paths.size() > 0) {
      map.put(PATHS, paths.stream().collect(Collectors.joining(",")));
    }
    if (partitions > 0) {
      map.put(PARTITIONS, Integer.toString(partitions));
    }
    if (types.size() > 0) {
      map.put(TYPES, toCsv(types));
    }
    if (excludeMetadata) {
      map.put(EXCLUDE_METADATA, Boolean.toString(excludeMetadata));
    }
    if (excludeTags) {
      map.put(EXCLUDE_TAGS, Boolean.toString(excludeTags));
    }
    if (tagsAsMap) {
      map.put(TAGS_AS_MAP, Boolean.toString(tagsAsMap));
    }
    if (wayNodesAsIdList) {
      map.put(WAY_NODES_AS_ID_LIST, Boolean.toString(wayNodesAsIdList));
    }
    if (includeOriginFile) {
      map.put(INCLUDE_ORIGIN_FILE, Boolean.toString(includeOriginFile));
    }
    return map;
  }

  private static String toCsv(HashSet<EntityType> types) {
    return types.stream().map(t -> t.name())
        .collect(Collectors.joining(","));
  }

  public static OsmPbfOptions parse(Map<String, String> map) {
    OsmPbfOptions options = new OsmPbfOptions();
    for (Entry<String, String> entry : map.entrySet()) {
      if (PATH.equalsIgnoreCase(entry.getKey())) {
        options.paths.add(entry.getValue());
      } else if (PATHS.equalsIgnoreCase(entry.getKey())) {
        try {
          options.paths.addAll(new ObjectMapper()
              .readValue(entry.getValue(), options.paths.getClass()));
        } catch (JsonProcessingException e) {
          throw new RuntimeException("Failed to parse paths: " + e.getMessage());
        }
      } else if (PARTITIONS.equalsIgnoreCase(entry.getKey())) {
        options.withPartitions(Integer.parseInt(entry.getValue()));
      } else if (TYPES.equalsIgnoreCase(entry.getKey())) {
        for (String type : entry.getValue().split("[,\\s]+")) {
          options.withType(findEntityTypeOrDie(type));
        }
      } else if (EXCLUDE_METADATA.equalsIgnoreCase(entry.getKey())) {
        options.withExcludeMetadata(Boolean.parseBoolean(entry.getValue()));
      } else if (EXCLUDE_TAGS.equalsIgnoreCase(entry.getKey())) {
        options.withExcludeTags(Boolean.parseBoolean(entry.getValue()));
      } else if (TAGS_AS_MAP.equalsIgnoreCase(entry.getKey())) {
        options.withTagsAsMap(Boolean.parseBoolean(entry.getValue()));
      } else if (WAY_NODES_AS_ID_LIST.equalsIgnoreCase(entry.getKey())) {
        options.withWayNodesAsIdList(Boolean.parseBoolean(entry.getValue()));
      } else if (INCLUDE_ORIGIN_FILE.equalsIgnoreCase(entry.getKey())) {
        options.withIncludeOriginFile(Boolean.parseBoolean(entry.getValue()));
      }
    }
    return options;
  }

  private static EntityType findEntityTypeOrDie(String name) {
    for (EntityType type : ALLOWED_TYPES) {
      if (type.name().equalsIgnoreCase(name)) {
        return type;
      }
    }
    throw new RuntimeException(
        "Invalid entity type: '" + name + "'. Valid types: " + DEFAULT_TYPES);
  }
}
