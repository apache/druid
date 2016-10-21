/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.data.input.orc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class OrcHadoopInputRowParser implements InputRowParser<OrcStruct>
{
  private final ParseSpec parseSpec;
  private String typeString;
  private final List<String> dimensions;
  private StructObjectInspector oip;
  private final OrcSerde serde;

  @JsonCreator
  public OrcHadoopInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("typeString") String typeString
  )
  {
    this.parseSpec = parseSpec;
    this.typeString = typeString;
    this.dimensions = parseSpec.getDimensionsSpec().getDimensionNames();
    this.serde = new OrcSerde();
    initialize();
  }

  @Override
  public InputRow parse(OrcStruct input)
  {
    Map<String, Object> map = Maps.newHashMap();
    List<? extends StructField> fields = oip.getAllStructFieldRefs();
    for (StructField field: fields) {
      ObjectInspector objectInspector = field.getFieldObjectInspector();
      switch(objectInspector.getCategory()) {
        case PRIMITIVE:
          PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector)objectInspector;
          map.put(field.getFieldName(),
              primitiveObjectInspector.getPrimitiveJavaObject(oip.getStructFieldData(input, field)));
          break;
        case LIST:  // array case - only 1-depth array supported yet
          ListObjectInspector listObjectInspector = (ListObjectInspector)objectInspector;
          map.put(field.getFieldName(),
              getListObject(listObjectInspector, oip.getStructFieldData(input, field)));
          break;
        default:
          break;
      }
    }

    TimestampSpec timestampSpec = parseSpec.getTimestampSpec();
    DateTime dateTime = timestampSpec.extractTimestamp(map);

    return new MapBasedInputRow(dateTime, dimensions, map);
  }

  private void initialize()
  {
    if (typeString == null) {
      typeString = typeStringFromParseSpec(parseSpec);
    }
    TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(typeString);
    Preconditions.checkArgument(typeInfo instanceof StructTypeInfo,
        String.format("typeString should be struct type but not [%s]", typeString));
    Properties table = getTablePropertiesFromStructTypeInfo((StructTypeInfo)typeInfo);
    serde.initialize(new Configuration(), table);
    try {
      oip = (StructObjectInspector) serde.getObjectInspector();
    } catch (SerDeException e) {
      e.printStackTrace();
    }
  }

  private List getListObject(ListObjectInspector listObjectInspector, Object listObject)
  {
    List objectList = listObjectInspector.getList(listObject);
    List list = null;
    ObjectInspector child = listObjectInspector.getListElementObjectInspector();
    switch(child.getCategory()) {
      case PRIMITIVE:
        final PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector)child;
        list = Lists.transform(objectList, new Function() {
          @Nullable
          @Override
          public Object apply(@Nullable Object input) {
            return primitiveObjectInspector.getPrimitiveJavaObject(input);
          }
        });
        break;
      default:
        break;
    }

    return list;
  }

  @Override
  @JsonProperty
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @JsonProperty
  public String getTypeString()
  {
    return typeString;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new OrcHadoopInputRowParser(parseSpec, typeString);
  }

  public InputRowParser withTypeString(String typeString)
  {
    return new OrcHadoopInputRowParser(parseSpec, typeString);
  }

  public static String typeStringFromParseSpec(ParseSpec parseSpec)
  {
    StringBuilder builder = new StringBuilder("struct<");
    builder.append(parseSpec.getTimestampSpec().getTimestampColumn()).append(":string");
    if (parseSpec.getDimensionsSpec().getDimensionNames().size() > 0) {
      builder.append(",");
      builder.append(StringUtils.join(parseSpec.getDimensionsSpec().getDimensionNames(), ":string,")).append(":string");
    }
    builder.append(">");

    return builder.toString();
  }

  public static Properties getTablePropertiesFromStructTypeInfo(StructTypeInfo structTypeInfo)
  {
    Properties table = new Properties();
    table.setProperty("columns", StringUtils.join(structTypeInfo.getAllStructFieldNames(), ","));
    table.setProperty("columns.types", StringUtils.join(
        Lists.transform(structTypeInfo.getAllStructFieldTypeInfos(),
            new Function<TypeInfo, String>() {
              @Nullable
              @Override
              public String apply(@Nullable TypeInfo typeInfo) {
                return typeInfo.getTypeName();
              }
            }),
        ","
    ));

    return table;
  }

  @Override
  public boolean equals(Object o)
  {
    if (!(o instanceof OrcHadoopInputRowParser)) {
      return false;
    }

    OrcHadoopInputRowParser other = (OrcHadoopInputRowParser)o;

    if (!parseSpec.equals(other.parseSpec)) {
      return false;
    }

    if (!typeString.equals(other.typeString)) {
      return false;
    }

    return true;
  }
}
