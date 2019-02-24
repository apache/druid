/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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

package org.apache.druid.data.input.orc;

import io.netty.util.SuppressForbidden;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcList;
import org.apache.orc.mapred.OrcMap;
import org.apache.orc.mapred.OrcStruct;
import org.apache.orc.mapred.OrcTimestamp;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class OrcStructConverter
{
  /**
   * Convert a orc struct field as though it were a map. Complex types will be transformed
   * into java lists and maps when possible ({@link OrcStructConverter#convertList} and
   * {@link OrcStructConverter#convertMap}), and
   * primitive types will be extracted into an ingestion friendly state (e.g. 'int' and 'long'). Finally,
   * if a field is not present, this method will return null.
   *
   * Note: "Union" types are not currently supported and will be returned as null
   */
  @Nullable
  Object convertField(OrcStruct struct, String fieldName)
  {
    TypeDescription schema = struct.getSchema();
    int fieldIndex = schema.getFieldNames().indexOf(fieldName);

    if (fieldIndex < 0) {
      return null;
    }

    TypeDescription fieldDescription = schema.getChildren().get(fieldIndex);
    WritableComparable fieldValue = struct.getFieldValue(fieldIndex);

    if (fieldDescription.getCategory().isPrimitive()) {
      return convertPrimitive(fieldDescription, fieldValue);
    } else {
      // handle complex column types
      /*
          ORC TYPE    WRITABLE TYPE
          array       org.apache.orc.mapred.OrcList
          map         org.apache.orc.mapred.OrcMap
          struct      org.apache.orc.mapred.OrcStruct
          uniontype   org.apache.orc.mapred.OrcUnion
       */
      switch (fieldDescription.getCategory()) {
        case LIST:
          OrcList orcList = (OrcList) fieldValue;
          return convertList(fieldDescription, orcList);
        case MAP:
          OrcMap map = (OrcMap) fieldValue;
          return convertMap(fieldDescription, map);
        case STRUCT:
          OrcStruct structMap = (OrcStruct) fieldValue;
          return convertMap(structMap);
        case UNION:
          // sorry union types :(
        default:
          return null;
      }
    }
  }

  @Nonnull
  private List<Object> convertList(TypeDescription fieldDescription, OrcList orcList)
  {
    // if primitive list, convert primitives
    TypeDescription listType = fieldDescription.getChildren().get(0);
    if (listType.getCategory().isPrimitive()) {
      return (List<Object>) orcList.stream()
                                   .map(li -> convertPrimitive(listType, (WritableComparable) li))
                                   .collect(Collectors.toList());
    }
    return new ArrayList<Object>(orcList);
  }

  static Map<Object, Object> convertMap(
      TypeDescription fieldDescription,
      OrcMap<? extends WritableComparable, ? extends WritableComparable> map
  )
  {
    Map<Object, Object> converted = new HashMap<>();
    TypeDescription keyDescription = fieldDescription.getChildren().get(0);
    TypeDescription valueDescription = fieldDescription.getChildren().get(1);
    for (WritableComparable key : map.navigableKeySet()) {
      Object newKey = convertPrimitive(keyDescription, key);
      if (valueDescription.getCategory().isPrimitive()) {
        converted.put(newKey, convertPrimitive(valueDescription, map.get(key)));
      } else {
        converted.put(newKey, map.get(key));
      }
    }
    return converted;
  }

  private Map<String, Object> convertMap(OrcStruct map)
  {
    Map<String, Object> converted = new HashMap<>();
    for (String key : map.getSchema().getFieldNames()) {
      converted.put(key, convertField(map, key));
    }
    return converted;
  }

  @SuppressForbidden(reason = "new DateTime(java.util.Date)")
  private static Object convertPrimitive(TypeDescription fieldDescription, WritableComparable field)
  {
    /*
        ORC TYPE    WRITABLE TYPE
        binary      org.apache.hadoop.io.BytesWritable
        bigint      org.apache.hadoop.io.LongWritable
        boolean     org.apache.hadoop.io.BooleanWritable
        char        org.apache.hadoop.io.Text
        date        org.apache.hadoop.hive.serde2.io.DateWritable
        decimal     org.apache.hadoop.hive.serde2.io.HiveDecimalWritable
        double      org.apache.hadoop.io.DoubleWritable
        float       org.apache.hadoop.io.FloatWritable
        int         org.apache.hadoop.io.IntWritable
        smallint    org.apache.hadoop.io.ShortWritable
        string      org.apache.hadoop.io.Text
        timestamp   org.apache.orc.mapred.OrcTimestamp
        tinyint     org.apache.hadoop.io.ByteWritable
        varchar     org.apache.hadoop.io.Text
     */
    switch (fieldDescription.getCategory()) {
      case STRING:
      case CHAR:
      case VARCHAR:
        return ((Text) field).toString();
      case BOOLEAN:
        return ((BooleanWritable) field).get();
      case BYTE:
        return ((ByteWritable) field).get();
      case SHORT:
        return ((ShortWritable) field).get();
      case INT:
        return ((IntWritable) field).get();
      case LONG:
        return ((LongWritable) field).get();
      case FLOAT:
        return ((FloatWritable) field).get();
      case DOUBLE:
        return ((DoubleWritable) field).get();
      case DECIMAL:
        return ((HiveDecimalWritable) field).getHiveDecimal().doubleValue();
      case TIMESTAMP:
        return ((OrcTimestamp) field).getTime();
      case DATE:
        // todo: is the the best way to go from java.util.Date to DateTime?
        return new DateTime(((DateWritable) field).get());
      case BINARY:
        // todo: hmm, i think we need a standard way of handling binary blobs this is a placeholder.
        return StringUtils.encodeBase64String(((BytesWritable) field).getBytes());
      default:
        return null;
    }
  }
}
