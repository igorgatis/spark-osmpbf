// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.WireFormat;
import crosby.binary.osmosis.OsmosisBinaryParser;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.InflaterInputStream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.osmbinary.Fileformat.Blob;
import org.openstreetmap.osmosis.osmbinary.Osmformat.PrimitiveBlock;
import org.openstreetmap.osmosis.osmbinary.Osmformat.PrimitiveGroup;

class RawBlobParser {

  private final HashSet<Integer> primitiveGroupFieldsToRemove;
  private final RowConverter converter;

  RawBlobParser(HashSet<EntityType> typesToKeep, RowConverter converter) {
    this.converter = converter;
    this.primitiveGroupFieldsToRemove = new HashSet<>();
    if (typesToKeep.size() > 0) {
      if (!typesToKeep.contains(EntityType.Node)) {
        this.primitiveGroupFieldsToRemove.add(PrimitiveGroup.NODES_FIELD_NUMBER);
        this.primitiveGroupFieldsToRemove.add(PrimitiveGroup.DENSE_FIELD_NUMBER);
      }
      if (!typesToKeep.contains(EntityType.Way)) {
        this.primitiveGroupFieldsToRemove.add(PrimitiveGroup.WAYS_FIELD_NUMBER);
      }
      if (!typesToKeep.contains(EntityType.Relation)) {
        this.primitiveGroupFieldsToRemove.add(PrimitiveGroup.RELATIONS_FIELD_NUMBER);
      }
    }
  }

  public Iterator<InternalRow> parse(byte[] blobBytes) {
    CodedInputStream input = CodedInputStream.newInstance(blobBytes);
    input.enableAliasing(true);

    ListSink sink = new ListSink();
    OsmosisBinaryParser parser = new OsmosisBinaryParser();
    parser.setSink(sink);

    try {
      int tag;
      while ((tag = input.readTag()) != 0) {
        switch (WireFormat.getTagFieldNumber(tag)) {
          case Blob.RAW_FIELD_NUMBER:
            parser.parse(parsePrimitiveBlock(input.readBytes().newCodedInput()));
            break;
          case Blob.RAW_SIZE_FIELD_NUMBER:
            input.skipField(tag);
            break;
          case Blob.ZLIB_DATA_FIELD_NUMBER:
            InflaterInputStream stream = new InflaterInputStream(input.readBytes().newInput());
            parser.parse(parsePrimitiveBlock(CodedInputStream.newInstance(stream)));
            break;
          case Blob.LZMA_DATA_FIELD_NUMBER:
            throw new OsmosisRuntimeException("lzma not supported.");
          case Blob.OBSOLETE_BZIP2_DATA_FIELD_NUMBER:
            throw new OsmosisRuntimeException("bzip2 not supported.");
          default:
            input.skipField(tag);
            break;
        }
      }
    } catch (IOException e) {
      throw new OsmosisRuntimeException(e);
    }

    return sink;
  }

  private PrimitiveBlock parsePrimitiveBlock(CodedInputStream input) throws IOException {
    if (primitiveGroupFieldsToRemove.size() == 0) {
      return PrimitiveBlock.parseFrom(input);
    }
    ExtensionRegistryLite registry = ExtensionRegistryLite.getEmptyRegistry();
    PrimitiveBlock.Builder block = PrimitiveBlock.newBuilder();
    int tag;
    while ((tag = input.readTag()) != 0) {
      switch (WireFormat.getTagFieldNumber(tag)) {
        case PrimitiveBlock.STRINGTABLE_FIELD_NUMBER:
          input.readMessage(block.getStringtableBuilder(), registry);
          break;
        case PrimitiveBlock.PRIMITIVEGROUP_FIELD_NUMBER:
          CodedInputStream bytes = input.readBytes().newCodedInput();
          bytes.enableAliasing(true);
          block.addPrimitivegroup(parsePrimitiveGroup(bytes));
          break;
        case PrimitiveBlock.GRANULARITY_FIELD_NUMBER:
          block.setGranularity(input.readInt32());
          break;
        case PrimitiveBlock.DATE_GRANULARITY_FIELD_NUMBER:
          block.setDateGranularity(input.readInt32());
          break;
        case PrimitiveBlock.LAT_OFFSET_FIELD_NUMBER:
          block.setLatOffset(input.readInt64());
          break;
        case PrimitiveBlock.LON_OFFSET_FIELD_NUMBER:
          block.setLonOffset(input.readInt64());
          break;
        default:
          input.skipField(tag);
          break;
      }
    }
    return block.buildPartial();
  }

  private PrimitiveGroup parsePrimitiveGroup(CodedInputStream input) throws IOException {
    ExtensionRegistryLite registry = ExtensionRegistryLite.getEmptyRegistry();
    PrimitiveGroup.Builder group = PrimitiveGroup.newBuilder();
    int tag;
    while ((tag = input.readTag()) != 0) {
      int number = WireFormat.getTagFieldNumber(tag);
      if (primitiveGroupFieldsToRemove.contains(number)) {
        input.skipField(tag);
        continue;
      }
      switch (number) {
        case PrimitiveGroup.NODES_FIELD_NUMBER:
          input.readMessage(group.addNodesBuilder(), registry);
          break;
        case PrimitiveGroup.DENSE_FIELD_NUMBER:
          input.readMessage(group.getDenseBuilder(), registry);
          break;
        case PrimitiveGroup.WAYS_FIELD_NUMBER:
          input.readMessage(group.addWaysBuilder(), registry);
          break;
        case PrimitiveGroup.RELATIONS_FIELD_NUMBER:
          input.readMessage(group.addRelationsBuilder(), registry);
          break;
        default:
          input.skipField(tag);
          break;
      }
    }
    return group.buildPartial();
  }

  private static class ListNode {

    Entity entity;
    ListNode next;
  }

  private class ListSink implements Sink, Iterator<InternalRow> {

    private ListNode head;
    private ListNode tail;

    ListSink() {
      head = new ListNode();
      tail = head;
    }

    @Override
    public boolean hasNext() {
      return head.next != null;
    }

    @Override
    public InternalRow next() {
      if (head.next == null) {
        return null;
      }
      head = head.next;
      InternalRow row = converter.convert(head.entity);
      return row;
    }

    @Override
    public void process(EntityContainer entityContainer) {
      tail.next = new ListNode();
      tail.next.entity = entityContainer.getEntity();
      tail = tail.next;
    }

    @Override
    public void initialize(Map<String, Object> metaData) {
    }

    @Override
    public void complete() {
    }

    @Override
    public void close() {
    }
  }
}
