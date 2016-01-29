/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.metamx.common.logger.Logger;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FileDescriptor;

@JsonTypeName("protobuf")
public class ProtoBufInputRowParser implements ByteBufferInputRowParser
{
  private static final Logger log = new Logger(ProtoBufInputRowParser.class);
  private static final int MAX_MESSAGE_DEPTH = 10;
  private static final String CHILD_NAME_SEPARATOR = ".";

  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;
  private final String descriptorFileInClasspath;
  private final String protoMessageFullName;

  @JsonCreator
  public ProtoBufInputRowParser(
          @JsonProperty("parseSpec") ParseSpec parseSpec,
          @JsonProperty("descriptor") String descriptorFileInClasspath,
          @JsonProperty("message") String protoMessageFullName
  )
  {
    this.parseSpec = parseSpec;
    this.descriptorFileInClasspath = descriptorFileInClasspath;
    this.protoMessageFullName = protoMessageFullName;
    this.mapParser = new MapInputRowParser(this.parseSpec);
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public ProtoBufInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ProtoBufInputRowParser(parseSpec, descriptorFileInClasspath, protoMessageFullName);
  }

  @Override
  public InputRow parse(ByteBuffer input)
  {
    // We should really create a ProtoBufBasedInputRow that does not need an intermediate map but accesses
    // the DynamicMessage directly...
    Map<String, Object> theMap = buildStringKeyMap(input);

    return mapParser.parse(theMap);
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
    final Descriptor descriptor = getDescriptor(descriptorFileInClasspath);
    final Map<String, Object> theMap = Maps.newHashMap();

    try {
      DynamicMessage message = DynamicMessage.parseFrom(descriptor, ByteString.copyFrom(input));
      parseMessage(theMap, null, message, MAX_MESSAGE_DEPTH);
    }
    catch (InvalidProtocolBufferException e) {
      log.warn(e, "Problem with protobuf something");
    }
    return theMap;
  }


  private void parseMessage(Map<String, Object> theMap, String parentName, DynamicMessage message, int depthLimit) {
    Map<Descriptors.FieldDescriptor, Object> allFields = message.getAllFields();
    for (Map.Entry<Descriptors.FieldDescriptor, Object> entry : allFields.entrySet()) {
      String name = glueName(parentName, entry.getKey().getName());
      if (theMap.containsKey(name)) {
        continue;
        // Perhaps throw an exception here?
        // throw new RuntimeException("dupicate key " + name + " in " + message);
      }
      Object value = entry.getValue();
      if (value instanceof DynamicMessage) {
        if (depthLimit > 0) {
          parseMessage(theMap, name, (DynamicMessage) value, depthLimit - 1);
        }
      } else if (value instanceof Collection) {
        for (Object element : (Collection) value) {
          if (element instanceof DynamicMessage) {
            if (depthLimit > 0) {
              Map<String, Object> colMap = Maps.newHashMap();
              // parse recursively
              parseMessage(colMap, name, (DynamicMessage) element, depthLimit - 1);
              // inspect parsed values and convert it to the collection
              addNewValuesToTheMapCollection(theMap, colMap);
            }
          } else if (element instanceof Descriptors.EnumValueDescriptor) {
            Descriptors.EnumValueDescriptor desc = (Descriptors.EnumValueDescriptor) element;
            addValueToTheMapCollectionIgnoreDuplicatesHandling(theMap, name, desc.getName());
          } else {
            // not a dynamic message or enum, for example it an element from the list of ints
            addValueToTheMapCollectionIgnoreDuplicatesHandling(theMap, name, element);
          }
        }
        continue;
      } else if (value instanceof Descriptors.EnumValueDescriptor) {
        Descriptors.EnumValueDescriptor desc = (Descriptors.EnumValueDescriptor) value;
        value = desc.getName();
      }
      // other types
      theMap.put(name, value);
    }
  }

  private void addNewValuesToTheMapCollection(Map<String, Object> theMap, Map<String, Object> colMap) {
    for (Map.Entry<String, Object> entrySet : colMap.entrySet()) {
      try {
        addValueToTheMapCollection (theMap, entrySet.getKey(), entrySet.getValue());
      } catch (IllegalKeyException e) {
        log.warn(e.getMessage(), e);
      }
    }
  }

  private void addValueToTheMapCollection (Map<String, Object> theMap, String key, Object value) throws IllegalKeyException {
    // get existing value for this key
    Object maybeCollection = theMap.get(key);
    Collection collection;
    if (maybeCollection == null) {
      collection = Lists.newArrayList(); // MapBasedRow uses list check instead of collection, thus we cannot use Set here
    } else if (maybeCollection instanceof Collection) {
      collection = (Collection) maybeCollection;
    } else { // duplicate names/naming collision
      throw new IllegalKeyException("not a collection for the key " + key);
    }
    // avoid list of lists of lists of ..., merge all such end-values to the top list
    if (value instanceof Collection) {
      for (Object singleValue : (Collection) value) {
        addUniqueValuesToCollection(collection, singleValue);
      }
    } else {
      addUniqueValuesToCollection(collection, value);
    }
    theMap.put(key, collection);
  }

  private void addValueToTheMapCollectionIgnoreDuplicatesHandling(Map<String, Object> theMap, String key, Object value) {
    try {
      addValueToTheMapCollection (theMap, key, value);
    } catch (IllegalKeyException e) {
      log.warn(e.getMessage(), e);
    }
  }

  private String glueName(String parentName, String name) {
    if (parentName != null) {
      return parentName + CHILD_NAME_SEPARATOR + name;
    }
    return name;
  }

  private void addUniqueValuesToCollection(Collection collection, Object value) {
    if (!collection.contains(value)) {
      collection.add(value);
    }
  }

  private Descriptor getDescriptor(String descriptorFileInClassPath)
  {
    try {
      InputStream fin = this.getClass().getClassLoader().getResourceAsStream(descriptorFileInClassPath);
      FileDescriptorSet set = FileDescriptorSet.parseFrom(fin);
      FileDescriptor file = FileDescriptor.buildFrom(
              set.getFile(0), new FileDescriptor[]
                      {}
      );
      List<Descriptor> messageTypes = file.getMessageTypes();
      final Descriptor defaultDescriptor = messageTypes.get(0);
      if (protoMessageFullName != null) {
        for (Descriptor descriptor : messageTypes) {
          String name = descriptor.getFullName();
          if (name.equals(protoMessageFullName)) {
            return descriptor;
          }
        }
      }
      return defaultDescriptor;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private class IllegalKeyException extends Exception {
    public IllegalKeyException(String message) {
      super(message);
    }
  }
}
