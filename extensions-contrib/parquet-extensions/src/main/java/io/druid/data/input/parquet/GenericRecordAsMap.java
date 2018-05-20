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


class GenericRecordAsMap implements Map<String, Object>
{
  private final GenericRecord record;
  private final boolean binaryAsString;
  private final Map<String, Object> attributes;
  private final ParquetParser parquetParser;
  private final int maxDepthOfFieldTraversal;

  /**
   * Global constructor within this package, stores dataset required for retrieval of dimension and metrics at runtime
   *
   * @param record         incoming generic record
   * @param binaryAsString parsed from parserSpec
   * @param parquetParser  incoming parquet parser spec definition object if defined, else it is null
   */
  GenericRecordAsMap(
      GenericRecord record,
      boolean binaryAsString,
      ParquetParser parquetParser
  )
  {
    this.record = Preconditions.checkNotNull(record, "record");
    this.binaryAsString = binaryAsString;
    this.parquetParser = parquetParser;
    //As record is already connected to a schema, using a seperate attributes map
    this.attributes = Maps.newHashMap();

    // Update key & value from parquet parser.
    // Takes the key as dimensionName. If null then takes `key` from the parser config as key.
    // Updates the attributes Map based on the parser config
    if (this.parquetParser != null && this.parquetParser.getFields() != null) {
      // Maximum depth till which the nested fields will be traversed, default depth is 3
      this.maxDepthOfFieldTraversal = parquetParser.getMaxDepth();
      for (Field field : this.parquetParser.getFields()) {
        // Looking up for dimension_name specified in field, to override the default field name
        // Even basic datatypes can use this option to override the dimension name without changing parquet columnname
        String key = field.getDimensionName();
        if (key == null) {
          // Else use the key as the dimension_name
          key = field.getKey().toString();
        }
        // Single entry point for attributes updation,
        // as attributes is within the scope of this object not converting it as an unmodifiable MAP
        this.attributes.put(key, fetchFieldValue(field.getFieldType(), field, null, 0));
      }
    } else {
      this.maxDepthOfFieldTraversal = ParquetParser.DEFAULT_MAX_DEPTH;
    }
  }

  /**
   * Method to Read a field and the GenericRecord.
   * It parses the records as specified in the parser config and produces a flattened data.
   *
   * @param fieldType Type of the field to be parsed
   * @param field     Field config as mentioned in the parser spec
   * @param object    Object to parsed
   * @param depth     Depth of the parser fields
   *
   * @return Parsed object
   */
  @Nullable
  @SuppressWarnings("unchecked")
  private Object fetchFieldValue(FieldType fieldType, Field field, Object object, int depth)
  {
    if (depth++ > maxDepthOfFieldTraversal) {
      // If the spec definition's max_depth limit is crossed breaking the parser process for the given field
      return null;
    }

    if (object != null || depth == 1) {
      switch (fieldType) {
        // Section of code to parse a map and extract values as mentioned in the spec
        // If the object is of type Map, then directly consume it
        // Or else get Map from the GenericRecord
        case MAP: {
          if (field == null) {
            return null;
          }

          Object referenceObject = getReferencingObject(object, depth);

          Map<Utf8, Object> referenceMap = null;

          // If object is of type Map, then just assign it to the referenceMap
          if (referenceObject instanceof Map) {
            referenceMap = (Map<Utf8, Object>) referenceObject;
          }

          // If object is of type GenericRecord, then extract Map based on the rootFieldName
          // And then assign it to referenceMap
          if (referenceObject instanceof GenericRecord) {
            Preconditions.checkNotNull(
                field.getRootFieldName(),
                "Expecting field name in field to be available for parsing a MAP!"
            );

            GenericRecord genericRecord = (GenericRecord) referenceObject;
            referenceMap = (Map<Utf8, Object>) genericRecord.get(field.getRootFieldName());
          }

          // If reference Map is null, then just return it.
          if (referenceMap == null) {
            return null;
          }

          // Fetch value from the Map based on the key
          Object fetchedObject = referenceMap.get(field.getKey());

          // If the Map key is in string instead Utf8, then fetch as String
          if (fetchedObject == null) {
            fetchedObject = ((Map) referenceMap).get(field.getKey().toString());
          }

          // If child field is not null, then fetchFieldValue from the nested field
          if (field.getField() != null) {
            return fetchFieldValue(field.getFieldType(), field.getField(), fetchedObject, depth);
          }

          return fetchedObject;
        }

        // Section of code to parse a Union Type and extract values as mentioned in the spec
        case UNION: {
          if (field == null) {
            // Having this check to ensure on nested fields, no empty field is passed
            return null;
          }

          GenericRecord genericRecord = (GenericRecord) getReferencingObject(object, depth);

          Object extractedObject = genericRecord.get(field.getKey().toString());
          Field nextField = field.getField();

          if (nextField != null) {
            return fetchFieldValue(nextField.getFieldType(), nextField, extractedObject, depth);
          }
          return extractedObject;

        }

        // Section of code to parse a Struct and extract values as mentioned in the spec
        case STRUCT: {
          if (field == null) {
            return null;
          }
          GenericRecord genericRecord = (GenericRecord) getReferencingObject(object, depth);
          //Extracting from object
          if (field.getField() != null) {
            Field nextField = field.getField();
            return fetchFieldValue(
                nextField.getFieldType(),
                nextField,
                genericRecord.get(field.getKey().toString()),
                depth
            );
          }

          return genericRecord.get(field.getKey().toString());
        }

        // Section of code to parse a List and extract values as mentioned in the spec
        // If index is specified, then extract a single value or return the list
        case LIST: {
          if (field == null) {
            return null;
          }

          GenericRecord genericRecord = (GenericRecord) getReferencingObject(object, depth);

          // For de duping PARQUET arrays like [{"element": 10}]
          final Object extractedObject = genericRecord.get(field.getKey().toString());

          // Evaluate parseListrecord method, that checks for index and returns value based on that.
          if (extractedObject != null && extractedObject instanceof List) {
            return parseListRecord((List<GenericRecord>) extractedObject, field, depth);
          }

          return null;
        }

        // Section of code to parse a Utf8 and extract values as mentioned in the spec
        case UTF8: {
          if (field == null) {
            return null;
          }
          GenericRecord genericRecord = (GenericRecord) getReferencingObject(object, depth);
          return genericRecord.get(field.getKey().toString());
        }

        // Section of code to parse a GenericDataRecord and extract values as mentioned in the spec
        case GENERIC_DATA_RECORD: {
          GenericRecord genericRecord = (GenericRecord) getReferencingObject(object, depth);
          return parseGenericRecord(genericRecord, field, depth);
        }

        // Section of code to parse a GenericRecord and extract values as mentioned in the spec
        case GENERIC_RECORD: {
          GenericRecord genericRecord = (GenericRecord) getReferencingObject(object, depth);
          return parseGenericRecord(genericRecord, field, depth);
        }

        // Section of code to parse a ByteBuffer and extract values as mentioned in the spec
        case BYTE_BUFFER: {
          return getBufferString((ByteBuffer) object);
        }

        // Section of code to parse a default value and extract values as mentioned in the spec
        default: {
          if (field == null) {
            return null;
          }
          GenericRecord genericRecord = (GenericRecord) getReferencingObject(object, depth);
          //Null check for key is performed at field creation, as its a required parameter
          return genericRecord.get(field.getKey().toString());
        }
      }
    }

    return null;
  }

  /**
   * Helper method for determining extraction object to be either the passed object or the base generic record
   *
   * @param obj   object in scope
   * @param depth current depth of fields traversed
   *
   * @return the object to be used for value extraction
   */
  private Object getReferencingObject(Object obj, int depth)
  {
    return obj == null && depth == 1 ? record : obj;
  }

  /**
   * Extract field from a GenericRecord. If its Primitive types then directly extract and return the value.
   * If its non-primitive type, then extract values from it as per the spec
   *
   * @param genericRecord of type GenericRecord
   * @param field         of type Field
   * @param depth         of type int
   *
   * @return fetched object from GenericRecord. As of now its always first object.
   */
  @Nullable
  private Object parseGenericRecord(GenericRecord genericRecord, Field field, int depth)
  {
    if (field != null && field.getKey() != null) {
      Object recordValue = genericRecord.get(field.getKey().toString());
      Field nextField = field.getField();

      if (nextField != null) {
        return fetchFieldValue(nextField.getFieldType(), field.getField(), recordValue, depth);
      }

      return recordValue;
    }

    return genericRecord.get(0);
  }

  /**
   * Method to parse list.
   * If index is specified in the parse spec, then return value at the index
   * Else return a list of extracted values based on the parse spec.
   *
   * @param objectList List of object
   * @param field      Field parse spec
   * @param depth      depth for parsing
   *
   * @return Object/List<Object> based on the index value
   */
  @Nullable
  private Object parseListRecord(List<GenericRecord> objectList, Field field, int depth)
  {
    if (field == null) {
      return null;
    }

    if (field.getIndex() == -1) {
      return parseListRecordToList(objectList, field, depth);
    }

    return parseListRecordToObject(objectList, field, depth);
  }

  /**
   * Method to parse list.
   * Returns a list of extracted values based on the parse spec.
   *
   * @param objectList List of object
   * @param field      Field parse spec
   * @param depth      depth for parsing
   *
   * @return List<Object>
   */
  private List<Object> parseListRecordToList(List<GenericRecord> objectList, Field field, int depth)
  {
    List<Object> parsedObjectList = Lists.newArrayList();

    for (GenericRecord currentObject : objectList) {
      Field nextField = field.getField();
      FieldType nextFieldType = getFieldType(currentObject.getClass());

      if (nextField != null) {
        nextFieldType = nextField.getFieldType();
        currentObject = (GenericRecord) currentObject.get(0);
      }

      Object parsedObject = fetchFieldValue(nextFieldType, nextField, currentObject, depth);

      if (parsedObject != null) {
        parsedObjectList.add(parsedObject);
      }
    }

    return parsedObjectList;
  }

  /**
   * Method to parse list.
   * Returns an extracted value based on the parse spec.
   *
   * @param objectList List of object
   * @param field      Field parse spec
   * @param depth      depth for parsing
   *
   * @return Object
   */
  @Nullable
  private Object parseListRecordToObject(List<GenericRecord> objectList, Field field, int depth)
  {
    if (objectList != null && (field.getIndex() >= 0 && field.getIndex() < objectList.size())) {
      //Extracting element per field definition
      Object currentObject = objectList.get(field.getIndex());

      if (currentObject == null) {
        return null;
      }

      Field nextField = field.getField();
      FieldType nextFieldType = getFieldType(currentObject.getClass());

      if (nextField != null) {
        nextFieldType = nextField.getFieldType();
        currentObject = ((GenericRecord) currentObject).get(0);
      }

      return fetchFieldValue(nextFieldType, field.getField(), currentObject, depth);
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
      // If the attributes map built during parquet parser definition unpacking contains the key, returning its value
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
