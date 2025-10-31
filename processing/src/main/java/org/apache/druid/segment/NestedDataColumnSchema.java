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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Nested column {@link DimensionSchema}. This uses {@link NestedCommonFormatColumnHandler} and is equivalent to using
 * {@link AutoTypeColumnSchema}, but remains for backwards compatibility.
 */
public class NestedDataColumnSchema extends DimensionSchema
{
  final int formatVersion;
  @Nullable
  final NestedCommonFormatColumnFormatSpec columnFormatSpec;

  @JsonCreator
  public NestedDataColumnSchema(
      @JsonProperty("name") String name,
      @JsonProperty("formatVersion") @Nullable Integer version,
      @JsonProperty("columnFormatSpec") @Nullable NestedCommonFormatColumnFormatSpec columnFormatSpec,
      @JacksonInject DefaultColumnFormatConfig defaultFormatConfig
  )
  {
    super(name, null, true);
    if (version != null) {
      formatVersion = version;
    } else if (defaultFormatConfig.getNestedColumnFormatVersion() != null) {
      formatVersion = defaultFormatConfig.getNestedColumnFormatVersion();
    } else {
      // this is sort of a lie... it's not really v5 in the segment, rather its v0 of the 'nested common format'
      // but as far as this is concerned it is v5
      formatVersion = 5;
    }
    DefaultColumnFormatConfig.validateNestedFormatVersion(formatVersion);
    if (columnFormatSpec == null) {
      if (defaultFormatConfig.getIndexSpec() != null) {
        this.columnFormatSpec = defaultFormatConfig.getIndexSpec().getAutoColumnFormatSpec();
      } else {
        this.columnFormatSpec = IndexSpec.getDefault().getAutoColumnFormatSpec();
      }
    } else {
      this.columnFormatSpec = columnFormatSpec;
    }
  }

  public NestedDataColumnSchema(
      String name,
      int version
  )
  {
    super(name, null, true);
    this.formatVersion = version;
    DefaultColumnFormatConfig.validateNestedFormatVersion(this.formatVersion);
    this.columnFormatSpec = IndexSpec.getDefault().getAutoColumnFormatSpec();
  }

  @JsonProperty("formatVersion")
  public int getFormatVersion()
  {
    return formatVersion;
  }

  @JsonProperty("columnFormatSpec")
  public NestedCommonFormatColumnFormatSpec getColumnFormatSpec()
  {
    return columnFormatSpec;
  }

  @Override
  public String getTypeName()
  {
    return NestedDataComplexTypeSerde.TYPE_NAME;
  }

  @Override
  public ColumnType getColumnType()
  {
    return ColumnType.NESTED_DATA;
  }

  @Override
  public DimensionHandler getDimensionHandler()
  {
    return new NestedCommonFormatColumnHandler(getName(), null, columnFormatSpec);
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
    if (!super.equals(o)) {
      return false;
    }
    NestedDataColumnSchema that = (NestedDataColumnSchema) o;
    return Objects.equals(formatVersion, that.formatVersion) &&
           Objects.equals(columnFormatSpec, that.columnFormatSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), formatVersion, columnFormatSpec);
  }

  @Override
  public String toString()
  {
    return "NestedDataColumnSchema{" +
           "formatVersion=" + formatVersion +
           "columnFormatSpec=" + columnFormatSpec +
           '}';
  }
}
