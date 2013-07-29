package com.metamx.druid.indexer.data;

import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FileDescriptor;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.metamx.druid.input.InputRow;

/**
 * @author jan.rudert
 */
public class ProtoBufInputRowParser implements ByteBufferInputRowParser
{

	private final MapInputRowParser inputRowCreator;
	private final Descriptor descriptor;

	@JsonCreator
	public ProtoBufInputRowParser(
	    @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensions") List<String> dimensions,
	    @JsonProperty("dimensionExclusions") List<String> dimensionExclusions,
      @JsonProperty("descriptor") String descriptorFileInClasspath)
	{

		descriptor = getDescriptor(descriptorFileInClasspath);
    inputRowCreator = new MapInputRowParser(timestampSpec, dimensions, dimensionExclusions);

	}

	@Override
	public InputRow parse(ByteBuffer input)
	{

		Map<String, Object> theMap = buildStringKeyMap(input);

		return inputRowCreator.parse(theMap);
	}

	private Map<String, Object> buildStringKeyMap(ByteBuffer input)
	{
		Map<String, Object> theMap = Maps.newHashMap();

		try
		{
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(input));
			Map<Descriptors.FieldDescriptor, Object> allFields = message.getAllFields();

			for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : allFields.entrySet())
			{
				String name = entry.getKey().getName();
				if (theMap.containsKey(name))
				{
					continue;
					// TODO
					// throw new RuntimeException("dupicate key " + name + " in " +
					// message);
				}
        Object value = entry.getValue();
        if(value instanceof Descriptors.EnumValueDescriptor) {
          Descriptors.EnumValueDescriptor desc = (Descriptors.EnumValueDescriptor) value;
          value = desc.getName();
        }

				theMap.put(name, value);
			}

		} catch (InvalidProtocolBufferException e)
		{
			// TODO
			e.printStackTrace();
		}
		return theMap;
	}

	private Descriptor getDescriptor(String descriptorFileInClassPath)
	{
		try
		{
			InputStream fin = this.getClass().getClassLoader().getResourceAsStream(descriptorFileInClassPath);
			FileDescriptorSet set = FileDescriptorSet.parseFrom(fin);
			FileDescriptor file = FileDescriptor.buildFrom(set.getFile(0), new FileDescriptor[]
			{});
			return file.getMessageTypes().get(0);
		} catch (Exception e)
		{
			throw Throwables.propagate(e);
		}
	}

	@Override
	public void addDimensionExclusion(String dimension)
	{
		inputRowCreator.addDimensionExclusion(dimension);
	}
}
