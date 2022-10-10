package org.apache.druid.data.input.protobuf;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;

import java.nio.ByteBuffer;
import java.util.Set;

public abstract class DescriptorBasedProtobufBytesDecoder implements ProtobufBytesDecoder
{
  protected Descriptors.Descriptor descriptor;
  protected final String protoMessageType;

  public DescriptorBasedProtobufBytesDecoder(
      final String protoMessageType
  )
  {
    this.protoMessageType = protoMessageType;
  }

  @JsonProperty
  public String getProtoMessageType()
  {
    return protoMessageType;
  }

  @VisibleForTesting
  void initDescriptor()
  {
    if (this.descriptor == null) {
      final DynamicSchema dynamicSchema = getDynamicSchema();
      this.descriptor = getDescriptor(dynamicSchema);
    }
  }

  protected abstract DynamicSchema getDynamicSchema();

  @Override
  public DynamicMessage parse(ByteBuffer bytes)
  {
    try {
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(bytes));
      return message;
    }
    catch (Exception e) {
      throw new ParseException(null, e, "Fail to decode protobuf message!");
    }
  }

  private Descriptors.Descriptor getDescriptor(DynamicSchema dynamicSchema)
  {
    Set<String> messageTypes = dynamicSchema.getMessageTypes();
    if (messageTypes.size() == 0) {
      throw new ParseException(null, "No message types found in the descriptor.");
    }

    String messageType = protoMessageType == null ? (String) messageTypes.toArray()[0] : protoMessageType;
    Descriptors.Descriptor desc = dynamicSchema.getMessageDescriptor(messageType);
    if (desc == null) {
      throw new ParseException(
          null,
          StringUtils.format(
              "Protobuf message type %s not found in the specified descriptor.  Available messages types are %s",
              protoMessageType,
              messageTypes
          )
      );
    }
    return desc;
  }
}
