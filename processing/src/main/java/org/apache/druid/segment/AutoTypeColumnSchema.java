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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.NestedCommonFormatColumn;
import org.apache.druid.segment.nested.NestedCommonFormatColumnSerializer;
import org.apache.druid.segment.nested.NestedDataColumnSerializer;
import org.apache.druid.segment.nested.ScalarDoubleColumnSerializer;
import org.apache.druid.segment.nested.ScalarLongColumnSerializer;
import org.apache.druid.segment.nested.ScalarStringColumnSerializer;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.nested.VariantArrayColumnSerializer;
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;

/**
 * Common {@link DimensionSchema} for ingestion of 'standard' Druid built-in {@link ColumnType} datatypes.
 *
 * Automatically determines the most appropriate type for the given input data, able to produce columns of type:
 *    {@link ColumnType#STRING}
 *    {@link ColumnType#STRING_ARRAY}
 *    {@link ColumnType#LONG}
 *    {@link ColumnType#LONG_ARRAY}
 *    {@link ColumnType#DOUBLE}
 *    {@link ColumnType#DOUBLE_ARRAY}
 *    {@link ColumnType#NESTED_DATA}
 *
 * and includes bitmap value set indexes. Input of mixed type will be stored as {@link ColumnType#NESTED_DATA}.
 *
 * @see AutoTypeColumnIndexer
 * @see AutoTypeColumnMerger
 * @see NestedCommonFormatColumnSerializer
 * @see VariantArrayColumnSerializer
 * @see ScalarDoubleColumnSerializer
 * @see ScalarLongColumnSerializer
 * @see NestedDataColumnSerializer
 * @see ScalarStringColumnSerializer
 * @see NestedCommonFormatColumnPartSerde
 * @see NestedCommonFormatColumn
 */
public class AutoTypeColumnSchema extends DimensionSchema
{
  public static final String TYPE = "auto";

  @JsonCreator
  public AutoTypeColumnSchema(
      @JsonProperty("name") String name
  )
  {
    super(name, null, true);
  }

  @Override
  public String getTypeName()
  {
    return TYPE;
  }

  @Override
  public ColumnType getColumnType()
  {
    return ColumnType.NESTED_DATA;
  }

  @Override
  public DimensionHandler<StructuredData, StructuredData, StructuredData> getDimensionHandler()
  {
    return new NestedCommonFormatColumnHandler(getName());
  }
}
