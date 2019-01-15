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

package org.apache.druid.data.input.parquet.simple;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class ParquetGroupConverter
{
  private static final int JULIAN_EPOCH_OFFSET_DAYS = 2_440_588;
  private static final long MILLIS_IN_DAY = TimeUnit.DAYS.toMillis(1);
  private static final long NANOS_PER_MILLISECOND = TimeUnit.MILLISECONDS.toNanos(1);

  /**
   * See {@link ParquetGroupConverter#convertField(Group, String)}
   */
  @Nullable
  private static Object convertField(Group g, String fieldName, boolean binaryAsString)
  {
    if (!g.getType().containsField(fieldName)) {
      return null;
    }

    final int fieldIndex = g.getType().getFieldIndex(fieldName);

    if (g.getFieldRepetitionCount(fieldIndex) <= 0) {
      return null;
    }

    Type fieldType = g.getType().getFields().get(fieldIndex);

    // primitive field
    if (fieldType.isPrimitive()) {
      // primitive list
      if (fieldType.getRepetition().equals(Type.Repetition.REPEATED)) {
        int repeated = g.getFieldRepetitionCount(fieldIndex);
        List<Object> vals = new ArrayList<>();
        for (int i = 0; i < repeated; i++) {
          vals.add(convertPrimitiveField(g, fieldIndex, i, binaryAsString));
        }
        return vals;
      }
      return convertPrimitiveField(g, fieldIndex, binaryAsString);
    } else {
      if (fieldType.isRepetition(Type.Repetition.REPEATED)) {
        return convertRepeatedFieldToList(g, fieldIndex, binaryAsString);
      }

      if (isLogicalMapType(fieldType)) {
        return convertLogicalMap(g.getGroup(fieldIndex, 0), binaryAsString);
      }

      if (isLogicalListType(fieldType)) {
        return convertLogicalList(g.getGroup(fieldIndex, 0), binaryAsString);
      }

      // not a list, but not a primtive, return the nested group type
      return g.getGroup(fieldIndex, 0);
    }
  }

  /**
   * convert a repeated field into a list of primitives or groups
   */
  private static List<Object> convertRepeatedFieldToList(Group g, int fieldIndex, boolean binaryAsString)
  {

    Type t = g.getType().getFields().get(fieldIndex);
    assert t.getRepetition().equals(Type.Repetition.REPEATED);
    int repeated = g.getFieldRepetitionCount(fieldIndex);
    List<Object> vals = new ArrayList<>();
    for (int i = 0; i < repeated; i++) {
      if (t.isPrimitive()) {
        vals.add(convertPrimitiveField(g, fieldIndex, i, binaryAsString));
      } else {
        vals.add(g.getGroup(fieldIndex, i));
      }
    }
    return vals;
  }

  /**
   * check if a parquet type is a valid 'list' type
   */
  private static boolean isLogicalListType(Type listType)
  {
    return !listType.isPrimitive() &&
           listType.getOriginalType() != null &&
           listType.getOriginalType().equals(OriginalType.LIST) &&
           listType.asGroupType().getFieldCount() == 1 &&
           listType.asGroupType().getFields().get(0).isRepetition(Type.Repetition.REPEATED);
  }

  /**
   * convert a parquet 'list' logical type {@link Group} to a java list of primitives or groups
   */
  private static List<Object> convertLogicalList(Group g, boolean binaryAsString)
  {
    /*
      // List<Integer> (nullable list, non-null elements)
      optional group my_list (LIST) {
        repeated int32 element;
      }

      // List<Tuple<String, Integer>> (nullable list, non-null elements)
      optional group my_list (LIST) {
        repeated group element {
          required binary str (UTF8);
          required int32 num;
        };
      }

      // List<OneTuple<String>> (nullable list, non-null elements)
      optional group my_list (LIST) {
        repeated group array {
          required binary str (UTF8);
        };
      }

      // List<OneTuple<String>> (nullable list, non-null elements)
      optional group my_list (LIST) {
        repeated group my_list_tuple {
          required binary str (UTF8);
        };
      }
     */
    assert isLogicalListType(g.getType());
    int repeated = g.getFieldRepetitionCount(0);
    boolean isListItemPrimitive = g.getType().getFields().get(0).isPrimitive();
    List<Object> vals = new ArrayList<>();

    for (int i = 0; i < repeated; i++) {
      if (isListItemPrimitive) {
        vals.add(convertPrimitiveField(g, 0, i, binaryAsString));
      } else {
        Group listItem = g.getGroup(0, i);
        vals.add(listItem);
      }
    }
    return vals;
  }

  /**
   * check if a parquet type is a valid 'map' type
   */
  private static boolean isLogicalMapType(Type groupType)
  {
    OriginalType ot = groupType.getOriginalType();
    if (groupType.isPrimitive() || ot == null || groupType.isRepetition(Type.Repetition.REPEATED)) {
      return false;
    }
    if (groupType.getOriginalType().equals(OriginalType.MAP) ||
        groupType.getOriginalType().equals(OriginalType.MAP_KEY_VALUE)) {
      GroupType myMapType = groupType.asGroupType();
      if (myMapType.getFieldCount() != 1 || myMapType.getFields().get(0).isPrimitive()) {
        return false;
      }
      GroupType mapItemType = myMapType.getFields().get(0).asGroupType();
      return mapItemType.isRepetition(Type.Repetition.REPEATED) &&
             mapItemType.getFieldCount() == 2 &&
             mapItemType.getFields().get(0).getName().equalsIgnoreCase("key") &&
             mapItemType.getFields().get(0).isPrimitive() &&
             mapItemType.getFields().get(1).getName().equalsIgnoreCase("value");
    }
    return false;
  }

  /**
   * Convert a parquet 'map' logical type {@link Group} to a java map of string keys to groups/lists/primitive values
   */
  private static Map<String, Object> convertLogicalMap(Group g, boolean binaryAsString)
  {
    /*
      // Map<String, Integer> (nullable map, non-null values)
      optional group my_map (MAP) {
        repeated group map {
          required binary str (UTF8);
          required int32 num;
        }
      }

      // Map<String, Integer> (nullable map, nullable values)
      optional group my_map (MAP_KEY_VALUE) {(
        repeated group map {
          required binary key (UTF8);
          optional int32 value;
        }
      }
     */
    assert isLogicalMapType(g.getType());
    int mapEntries = g.getFieldRepetitionCount(0);
    Map<String, Object> converted = new HashMap<>();
    for (int i = 0; i < mapEntries; i++) {
      Group mapEntry = g.getGroup(0, i);
      String key = convertPrimitiveField(mapEntry, 0, binaryAsString).toString();
      Object value = convertField(mapEntry, "value", binaryAsString);
      converted.put(key, value);
    }
    return converted;
  }

  /**
   * Convert a primitive group field to a "ingestion friendly" java object
   *
   * @return "ingestion ready" java object, or null
   */
  @Nullable
  private static Object convertPrimitiveField(Group g, int fieldIndex, boolean binaryAsString)
  {
    PrimitiveType pt = (PrimitiveType) g.getType().getFields().get(fieldIndex);
    if (pt.isRepetition(Type.Repetition.REPEATED) && g.getFieldRepetitionCount(fieldIndex) > 1) {
      List<Object> vals = new ArrayList<>();
      for (int i = 0; i < g.getFieldRepetitionCount(fieldIndex); i++) {
        vals.add(convertPrimitiveField(g, fieldIndex, i, binaryAsString));
      }
      return vals;
    }
    return convertPrimitiveField(g, fieldIndex, 0, binaryAsString);
  }

  /**
   * Convert a primitive group field to a "ingestion friendly" java object
   *
   * @return "ingestion ready" java object, or null
   */
  @Nullable
  private static Object convertPrimitiveField(Group g, int fieldIndex, int index, boolean binaryAsString)
  {
    PrimitiveType pt = (PrimitiveType) g.getType().getFields().get(fieldIndex);
    OriginalType ot = pt.getOriginalType();

    try {
      if (ot != null) {
        // convert logical types
        switch (ot) {
          case DATE:
            long ts = g.getInteger(fieldIndex, index) * MILLIS_IN_DAY;
            return ts;
          case TIME_MICROS:
            return g.getLong(fieldIndex, index);
          case TIME_MILLIS:
            return g.getInteger(fieldIndex, index);
          case TIMESTAMP_MICROS:
            return TimeUnit.MILLISECONDS.convert(g.getLong(fieldIndex, index), TimeUnit.MICROSECONDS);
          case TIMESTAMP_MILLIS:
            return g.getLong(fieldIndex, index);
          case INTERVAL:
          /*
          INTERVAL is used for an interval of time. It must annotate a fixed_len_byte_array of length 12.
          This array stores three little-endian unsigned integers that represent durations at different
          granularities of time. The first stores a number in months, the second stores a number in days,
          and the third stores a number in milliseconds. This representation is independent of any particular
          timezone or date.

          Each component in this representation is independent of the others. For example, there is no
          requirement that a large number of days should be expressed as a mix of months and days because there is
          not a constant conversion from days to months.

          The sort order used for INTERVAL is undefined. When writing data, no min/max statistics should be
           saved for this type and if such non-compliant statistics are found during reading, they must be ignored.
           */
            Binary intervalVal = g.getBinary(fieldIndex, index);
            IntBuffer intBuf = intervalVal.toByteBuffer().order(ByteOrder.LITTLE_ENDIAN).asIntBuffer();
            int months = intBuf.get(0);
            int days = intBuf.get(1);
            int millis = intBuf.get(2);
            StringBuilder periodBuilder = new StringBuilder("P");
            if (months > 0) {
              periodBuilder.append(months).append("M");
            }
            if (days > 0) {
              periodBuilder.append(days).append("D");
            }
            if (periodBuilder.length() > 1) {
              Period p = Period.parse(periodBuilder.toString());
              Duration d = p.toStandardDuration().plus(millis);
              return d;
            } else {
              return new Duration(millis);
            }
          case INT_8:
          case INT_16:
          case INT_32:
            return g.getInteger(fieldIndex, index);
          case INT_64:
            return g.getLong(fieldIndex, index);
          // todo: idk wtd about unsigned
          case UINT_8:
          case UINT_16:
          case UINT_32:
            return g.getInteger(fieldIndex, index);
          case UINT_64:
            return g.getLong(fieldIndex, index);
          case DECIMAL:
          /*
            DECIMAL can be used to annotate the following types:
              int32: for 1 <= precision <= 9
              int64: for 1 <= precision <= 18; precision < 10 will produce a warning
              fixed_len_byte_array: precision is limited by the array size. Length n can
                store <= floor(log_10(2^(8*n - 1) - 1)) base-10 digits
              binary: precision is not limited, but is required. The minimum number of bytes to store
                the unscaled value should be used.
           */
            int precision = pt.asPrimitiveType().getDecimalMetadata().getPrecision();
            int scale = pt.asPrimitiveType().getDecimalMetadata().getScale();
            switch (pt.getPrimitiveTypeName()) {
              case INT32:
                return new BigDecimal(g.getInteger(fieldIndex, index));
              case INT64:
                return new BigDecimal(g.getLong(fieldIndex, index));
              case FIXED_LEN_BYTE_ARRAY:
              case BINARY:
                Binary value = g.getBinary(fieldIndex, index);
                return convertBinaryToDecimal(value, precision, scale);
              default:
                throw new RE(
                    "Unknown 'DECIMAL' type supplied to primitive conversion: %s (this should never happen)",
                    pt.getPrimitiveTypeName()
                );
            }
          case UTF8:
          case ENUM:
          case JSON:
            return g.getString(fieldIndex, index);
          case LIST:
          case MAP:
          case MAP_KEY_VALUE:
          case BSON:
          default:
            throw new RE(
                "Non-primitive supplied to primitive conversion: %s (this should never happen)",
                ot.name()
            );
        }
      } else {
        // fallback to handling the raw primitive type if no logical type mapping
        switch (pt.getPrimitiveTypeName()) {
          case BOOLEAN:
            return g.getBoolean(fieldIndex, index);
          case INT32:
            return g.getInteger(fieldIndex, index);
          case INT64:
            return g.getLong(fieldIndex, index);
          case FLOAT:
            return g.getFloat(fieldIndex, index);
          case DOUBLE:
            return g.getDouble(fieldIndex, index);
          case INT96:
            Binary tsBin = g.getInt96(fieldIndex, index);
            return convertInt96BinaryToTimestamp(tsBin);
          case FIXED_LEN_BYTE_ARRAY:
          case BINARY:
            Binary bin = g.getBinary(fieldIndex, index);
            byte[] bytes = bin.getBytes();
            if (binaryAsString) {
              return StringUtils.fromUtf8(bytes);
            } else {
              return bytes;
            }
          default:
            throw new RE("Unknown primitive conversion: %s", ot.name());
        }
      }
    }
    catch (Exception ex) {
      return null;
    }
  }

  /**
   * convert deprecated parquet int96 nanosecond timestamp to a long, based on
   * https://github.com/prestodb/presto/blob/master/presto-hive/src/main/java/com/facebook/presto/hive/parquet/ParquetTimestampUtils.java#L56
   */
  private static long convertInt96BinaryToTimestamp(Binary value)
  {
    // based on prestodb parquet int96 timestamp conversion
    byte[] bytes = value.getBytes();

    // little endian encoding - need to invert byte order
    long timeOfDayNanos =
        Longs.fromBytes(bytes[7], bytes[6], bytes[5], bytes[4], bytes[3], bytes[2], bytes[1], bytes[0]);
    int julianDay = Ints.fromBytes(bytes[11], bytes[10], bytes[9], bytes[8]);

    long ts = ((julianDay - JULIAN_EPOCH_OFFSET_DAYS) * MILLIS_IN_DAY) + (timeOfDayNanos / NANOS_PER_MILLISECOND);
    return ts;
  }

  /**
   * convert parquet binary decimal to BigDecimal, lifted from
   * https://github.com/apache/parquet-mr/blob/master/parquet-pig/src/main/java/org/apache/parquet/pig/convert/DecimalUtils.java#L38
   */
  private static BigDecimal convertBinaryToDecimal(Binary value, int precision, int scale)
  {
    // based on parquet-mr pig conversion which is based on spark conversion... yo dawg?
    if (precision <= 18) {
      ByteBuffer buffer = value.toByteBuffer();
      byte[] bytes = buffer.array();
      int start = buffer.arrayOffset() + buffer.position();
      int end = buffer.arrayOffset() + buffer.limit();
      long unscaled = 0L;
      int i = start;
      while (i < end) {
        unscaled = (unscaled << 8 | bytes[i] & 0xff);
        i++;
      }
      int bits = 8 * (end - start);
      long unscaledNew = (unscaled << (64 - bits)) >> (64 - bits);
      if (unscaledNew <= -Math.pow(10, 18) || unscaledNew >= Math.pow(10, 18)) {
        return new BigDecimal(unscaledNew);
      } else {
        return BigDecimal.valueOf(unscaledNew / Math.pow(10, scale));
      }
    } else {
      return new BigDecimal(new BigInteger(value.getBytes()), scale);
    }
  }


  static boolean isWrappedListPrimitive(Object o)
  {
    if (o instanceof Group) {
      Group g = (Group) o;
      return g.getType().isRepetition(Type.Repetition.REPEATED) &&
             !g.getType().isPrimitive() &&
             g.getType().asGroupType().getFieldCount() == 1 &&
             g.getType().getFields().get(0).isPrimitive();
    }
    return false;
  }

  private boolean binaryAsString;

  ParquetGroupConverter(boolean binaryAsString)
  {
    this.binaryAsString = binaryAsString;
  }

  /**
   * Convert a parquet group field as though it were a map. Logical types of 'list' and 'map' will be transformed
   * into java lists and maps respectively ({@link ParquetGroupConverter#convertLogicalList} and
   * {@link ParquetGroupConverter#convertLogicalMap}), repeated fields will also be translated to lists, and
   * primitive types will be extracted into an ingestion friendly state (e.g. 'int' and 'long'). Finally,
   * if a field is not present, this method will return null.
   */
  @Nullable
  Object convertField(Group g, String fieldName)
  {
    return convertField(g, fieldName, binaryAsString);
  }

  /**
   * Properly formed parquet lists when passed through {@link ParquetGroupConverter#convertField(Group, String)} can
   * return lists which contain 'wrapped' primitives, that are a {@link Group} with a single, primitive field (see
   * {@link ParquetGroupConverter#isWrappedListPrimitive(Object)})
   */
  Object unwrapListPrimitive(Object o)
  {
    assert isWrappedListPrimitive(o);
    Group g = (Group) o;
    return convertPrimitiveField(g, 0, binaryAsString);
  }
}
