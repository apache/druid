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
package io.druid.data.input.parquet;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.parquet.model.Field;
import io.druid.data.input.parquet.model.FieldType;
import io.druid.data.input.parquet.model.ParquetParser;
import io.druid.java.util.common.StringUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.builder.ToStringBuilder;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class GenericRecordAsMap implements Map<String, Object>
{
  private final GenericRecord record;
  private final boolean binaryAsString;
  private final Map<String, Object> attributes;
  private final ParquetParser parquetParser;
  private final int maxDepthOfFieldTraversal;

  GenericRecordAsMap(
      GenericRecord record,
      boolean binaryAsString,
      ParquetParser parquetParser
  )
  {
    this.record = Preconditions.checkNotNull(record, "record");
    this.binaryAsString = binaryAsString;
    //As record is already connected to a schema, using a seperate attributes map
    //As attibutes is referred only within this object not making it a immutable collection
    this.attributes = Maps.newHashMap();
    this.parquetParser = parquetParser;
    this.maxDepthOfFieldTraversal = parquetParser != null ? parquetParser.getMaxDepth()
                                                          : ParquetParser.DEFAULT_MAX_DEPTH;
    // Update k,v from parquet parser
    if (this.parquetParser != null && this.parquetParser.getFields() != null) {
      for (Field field : this.parquetParser.getFields()) {
        if (field.getDimensionName() != null) {
          this.attributes.put(field.getDimensionName(), getFieldValue(field.getFieldType(), field, null, 0));
        } else if (field.getKey() != null) {
          this.attributes.put(field.getKey().toString(), getFieldValue(field.getFieldType(), field, null, 0));
        }
      }
    }
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private Object getFieldValue(FieldType fieldType, Field field, Object obj, int depth)
  {
    //TODO rewrite the comment This goes N'level depth per field definition and then the value's are extracted as per the customization
    //Traversing only to the given specified N' level depth, else terminating to avoid circular cases and deep parsing
    if (depth++ > maxDepthOfFieldTraversal) {
      return null;
    }
    switch (fieldType) {
      case MAP: {
        if (field == null) {
          return null;
        }

        final GenericRecord referencingRecord = (GenericRecord) getReferencingObject(obj);
        return parseMap(field, referencingRecord, depth);
      }
      case ARRAY: {
        if (field == null) {
          return null;
        }
        // Ignoring record list check as not matching the parser spec should result in runtime exception
        final List<Object> list = (List<Object>) ((GenericRecord) getReferencingObject(obj))
            .get(field.getKey().toString());
        if (list != null && field.getIndex() >= 0 && field.getIndex() < list.size()) {
          if (field.getField() != null) {
            //Extracting element per field definition
            return parseObject(list.get(field.getIndex()), field.getField(), depth);
          } else {
            return parseObject(list.get(field.getIndex()), depth);
          }
        }
        return null;
      }
      case GENERIC_DATA_RECORD: {
        if (obj == null) {
          return null;
        }
        return ((GenericRecord) getReferencingObject(obj)).get(0);
      }
      case STRUCT: {
        if (field == null || obj == null) {
          return null;
        }
        //TODO have to extract from object
        return ((GenericData.Record) ((GenericRecord) getReferencingObject(obj)).get(field.getIndex())).get(field.getIndex());
      }
      case STRUCT_LIST: {
        if (field == null) {
          return null;
        }
        final List objectList = (List) ((GenericRecord) getReferencingObject(obj)).get(field.getKey().toString());
        if (objectList != null && !objectList.isEmpty()) {
          List parsedList = Lists.newArrayList();
          for (Object object : objectList) {
            //TODO Check field -> field to be available
            parsedList.add(parseObject(object, field.getField(), depth));
          }
          return parsedList;
        }
        return null;
      }
      case STRUCT_MAP: {
        if (field == null || obj == null) {
          return null;
        }
        final GenericData.Record referenceObj = (GenericData.Record) ((GenericRecord) getReferencingObject(obj)).get(0);
        return parseMap(field, referenceObj, depth);
      }
      case UTF8: {
        return obj != null ? obj.toString() : null;
      }
      case LIST: {
        // For de duping PARQUET arrays like [{"element": 10}]
        final Object extractedObject = ((GenericRecord) getReferencingObject(obj)).get(field.getKey().toString());
        if (extractedObject instanceof List) {
          return parseList((List) extractedObject, depth);
        } else if (extractedObject instanceof GenericData.Record) {
          return parseList((GenericData.Record) extractedObject, depth);
        }
        return null;
      }
      case GENERIC_RECORD: {
        return parseGenericRecord((GenericRecord) obj);
      }
      case UNION: {
        // Support for HIVE union types, as this support is not continued
        // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-UnionTypesunionUnionTypes
        // having it for one of our use case, this will be deprecated and replaced with struct and field usage
        if (field == null) {
          return null;
        }
        // Field will be a struct having the index for GenericRecord, and then it will contain a map within it
        return parseObject(
            ((GenericRecord) getReferencingObject(obj)).get(field.getKey().toString()),
            field.getField(),
            depth
        );
      }
      case BYTE_BUFFER: {
        return getBufferString((ByteBuffer) obj);
      }
      default: {
        Preconditions.checkNotNull(
            field.getKey(),
            "Expecting key in field to be available for parsing from default MAP record!"
        );
        return record.get(field.getKey().toString());
      }
    }
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private Object parseMap(Field field, GenericRecord referencingRecord, int depth)
  {
    // Field's key variable check is excluded as it is done as part of JSON parsing
    Preconditions.checkNotNull(
        field.getRootFieldName(),
        "Expecting root_field_name in field to be available for parsing a MAP!"
    );
    if (referencingRecord.get(field.getRootFieldName()) != null) {
      // Ignoring record.get(field.getRootFieldName()) instanceof Map check as not matching
      // the parser spec should result in runtime exception
      if (field.getField() != null) {
        return parseObject(
            ((Map<Utf8, Object>) referencingRecord.get(field.getRootFieldName())).get(field.getKey()),
            field,
            depth
        );
      } else {
        return ((Map<Utf8, Object>) referencingRecord.get(field.getRootFieldName())).get(field.getKey());
      }
    }
    return null;
  }

  private Object getReferencingObject(Object obj)
  {
    return obj == null ? record : obj;
  }

  @Nullable
  private Object parseObject(Object obj, int depth)
  {
    if (obj != null) {
      return getFieldValue(getFieldType(obj.getClass()), null, obj, depth);
    }
    return null;
  }

  @Nullable
  private Object parseObject(Object obj, Field field, int depth)
  {
    Preconditions.checkNotNull(field, "Expecting field to be available for parsing generic object!");
    return getFieldValue(field.getFieldType(), field, obj, depth);
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private Object parseList(GenericData.Record list, int depth)
  {
    if (list != null) {
      List parsedList = Lists.newArrayList();
      for (int i = 0; i < list.getSchema().getFields().size(); i++) {
        if (list.get(i) != null) {
          final Object extractedValue = getFieldValue(getFieldType(list.get(i).getClass()), null, list.get(i), depth);
          if (extractedValue != null) {
            parsedList.add(extractedValue);
          }
        }
      }
      return parsedList;
    }
    return null;
  }

  @Nullable
  @SuppressWarnings("unchecked")
  private Object parseList(List list, int depth)
  {
    if (list != null) {
      List parsedList = Lists.newArrayList();
      for (Object aList : list) {
        if (aList != null) {
          final Object extractedValue = getFieldValue(getFieldType(aList.getClass()), null, aList, depth);
          if (extractedValue != null) {
            parsedList.add(extractedValue);
          }
        }
      }
      return parsedList;
    }
    return null;
  }

  /*
   * As of now returning the first value from the generic record, can parse appropriately by defining a valid Field in ParquetParser
   */
  @Nullable
  private Object parseGenericRecord(GenericRecord obj)
  {
    if (obj != null) {
      final Schema schema = obj.getSchema();
      if (schema != null && schema.getFields() != null && !schema.getFields().isEmpty()) {
        return obj.get(schema.getFields().get(0).name());
      }
    }
    return null;
  }

  @Override
  public int size()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isEmpty()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsKey(Object key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean containsValue(Object value)
  {
    throw new UnsupportedOperationException();
  }

  /**
   * When used in MapBasedRow, field in GenericRecord will be interpret as follows:
   * <ul>
   * <li> avro schema type -> druid dimension:</li>
   * <ul>
   * <li>null, boolean, int, long, float, double, string, Records, Enums, Maps, Fixed -> String, using String.valueOf</li>
   * <li>bytes -> Arrays.toString() or new String if binaryAsString is true</li>
   * <li>Arrays -> List&lt;String&gt;, using Lists.transform(&lt;List&gt;dimValue, TO_STRING_INCLUDING_NULL)</li>
   * </ul>
   * <li> avro schema type -> druid metric:</li>
   * <ul>
   * <li>null -> 0F/0L</li>
   * <li>int, long, float, double -> Float/Long, using Number.floatValue()/Number.longValue()</li>
   * <li>string -> Float/Long, using Float.valueOf()/Long.valueOf()</li>
   * <li>boolean, bytes, Arrays, Records, Enums, Maps, Fixed -> ParseException</li>
   * </ul>
   * </ul>
   */
  @Override
  public Object get(Object key)
  {
    if (attributes.containsKey(key.toString())) {
      // TODO send it as a string, check while construction whether the value is saved as string
      return attributes.get(key);
    } else {
      Object field = record.get(key.toString());
      if (field instanceof ByteBuffer) {
        return getBufferString((ByteBuffer) field);
      }
      if (field instanceof Utf8) {
        return field.toString();
      }
      return field;
    }
  }

  private Object getBufferString(ByteBuffer field)
  {
    if (binaryAsString) {
      return StringUtils.fromUtf8(((ByteBuffer) field).array());
    } else {
      return Arrays.toString(field.array());
    }
  }

  private FieldType getFieldType(Class<?> classz)
  {
    return parquetParser != null ? parquetParser.getFieldType(classz) : null;
  }

  @Override
  public Object put(String key, Object value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object remove(Object key)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void putAll(Map<? extends String, ?> m)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> keySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<Object> values()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<Entry<String, Object>> entrySet()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String toString()
  {
    return new ToStringBuilder(this)
        .append("record", record)
        .append("binaryAsString", binaryAsString)
        .append("attributes", attributes)
        .toString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    GenericRecordAsMap that = (GenericRecordAsMap) o;

    if (binaryAsString != that.binaryAsString) {
      return false;
    }
    if (record != null ? !record.equals(that.record) : that.record != null) {
      return false;
    }
    return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
  }

  @Override
  public int hashCode()
  {
    int result = record != null ? record.hashCode() : 0;
    result = 31 * result + (binaryAsString ? 1 : 0);
    result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
    return result;
  }
}
