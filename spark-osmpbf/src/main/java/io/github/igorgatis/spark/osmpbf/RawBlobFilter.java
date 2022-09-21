// This software is released into the Public Domain.  See copying.txt for details.
package io.github.igorgatis.spark.osmpbf;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import com.google.protobuf.WireFormat;
import java.io.IOException;
import java.util.HashSet;
import java.util.zip.InflaterInputStream;
import org.openstreetmap.osmosis.core.OsmosisRuntimeException;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.osmbinary.Osmformat.PrimitiveBlock;
import org.openstreetmap.osmosis.osmbinary.Osmformat.PrimitiveGroup;
import org.openstreetmap.osmosis.pbf2.v0_6.impl.RawBlob;

class RawBlobFilter {

  private final HashSet<Integer> primitiveGroupFieldsToRemove;

  RawBlobFilter(HashSet<EntityType> typesToKeep) {
    this.primitiveGroupFieldsToRemove = new HashSet<>();
    if (typesToKeep.size() > 0) {
      if (!typesToKeep.contains(EntityType.Node)) {
        this.primitiveGroupFieldsToRemove.add(PrimitiveGroup.NODES_FIELD_NUMBER);
        this.primitiveGroupFieldsToRemove.add(PrimitiveGroup.DENSE_FIELD_NUMBER);
      }
      if (!typesToKeep.contains(EntityType.Way)) {
        this.primitiveGroupFieldsToRemove.add(PrimitiveGroup.WAYS_FIELD_NUMBER);
      }
      // NOTICE: Relations are sparse compared to Nodes and Ways. Empirical data shows that
      // the time spent processing Nodes and Ways consumes any benefit of removing Relations.
      // Therefore, it is not worth filtering Relations at this stage.
    }
  }

  private UnknownFieldSet parsePrimitiveGroup(CodedInputStream input)
      throws IOException {
    UnknownFieldSet.Builder group = UnknownFieldSet.newBuilder();
    boolean end = false;
    int tag;
    while (!end && (tag = input.readTag()) != 0) {
      int field = WireFormat.getTagFieldNumber(tag);
      if (primitiveGroupFieldsToRemove.contains(field)) {
        end = !input.skipField(tag);
      } else {
        group.mergeFieldFrom(tag, input);
      }
    }
    return group.buildPartial();
  }

  private UnknownFieldSet parsePrimitiveBlock(CodedInputStream input)
      throws IOException {
    UnknownFieldSet.Builder block = UnknownFieldSet.newBuilder();
    int tag;
    boolean hasContent = false;
    while ((tag = input.readTag()) != 0) {
      int field = WireFormat.getTagFieldNumber(tag);
      if (field == PrimitiveBlock.PRIMITIVEGROUP_FIELD_NUMBER) {
        UnknownFieldSet group = parsePrimitiveGroup(input.readBytes().newCodedInput());
        if (group.getSerializedSize() > 0) {
          Field value = Field.newBuilder()
              .addLengthDelimited(group.toByteString())
              .build();
          block.mergeField(field, value);
          hasContent = true;
        }
      } else {
        block.mergeFieldFrom(tag, input);
      }
    }
    if (!hasContent) {
      return UnknownFieldSet.getDefaultInstance();
    }
    return block.buildPartial();
  }

  private CodedInputStream getCodedInput(Fileformat.Blob pbfBlob) {
    if (pbfBlob.hasRaw()) {
      return pbfBlob.getRaw().newCodedInput();
    } else if (pbfBlob.hasZlibData()) {
      return CodedInputStream.newInstance(
          new InflaterInputStream(pbfBlob.getZlibData().newInput()));
    } else {
      throw new OsmosisRuntimeException(
          "PBF blob uses unsupported compression, only raw or zlib may be used.");
    }
  }

  public RawBlob filterPrimitiveBlob(RawBlob blob) {
    if (primitiveGroupFieldsToRemove.size() == 0) {
      return blob;
    }
    try {
      Fileformat.Blob pbfBlob = Fileformat.Blob.parseFrom(blob.getData());

      CodedInputStream input = getCodedInput(pbfBlob);
      ByteString rawData = parsePrimitiveBlock(input).toByteString();
      if (input.getTotalBytesRead() == rawData.size()) {
        return blob;
      }

      Fileformat.Blob newPbfBlob = Fileformat.Blob.newBuilder()
          .setRawSize(rawData.size())
          .setRaw(rawData)
          .build();
      return new RawBlob(blob.getType(), newPbfBlob.toByteArray());
    } catch (IOException e) {
      throw new OsmosisRuntimeException(e);
    }
  }
}
