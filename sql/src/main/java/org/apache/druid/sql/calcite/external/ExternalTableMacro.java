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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.ExternalTable;

import java.util.List;
import java.util.Optional;

/**
 * Used by {@link ExternalOperatorConversion} to generate {@link DruidTable} that reference {@link ExternalDataSource}.
 *
 * This class is exercised in CalciteInsertDmlTest but is not currently exposed to end users.
 */
public class ExternalTableMacro implements TableMacro
{
  private final ObjectMapper jsonMapper;

  @Inject
  public ExternalTableMacro(@Json final ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  @Override
  public TranslatableTable apply(final List<Object> arguments)
  {
    try {
      final InputSource inputSource = jsonMapper.readValue((String) arguments.get(0), InputSource.class);
      final InputFormat inputFormat = jsonMapper.readValue((String) arguments.get(1), InputFormat.class);
      final RowSignature signature = jsonMapper.readValue((String) arguments.get(2), RowSignature.class);

      // Prevent a RowSignature that has a ColumnSignature with name "__time" and type that is not LONG because it
      // will be automatically casted to LONG while processing in RowBasedColumnSelectorFactory.
      // This can cause an issue when the incorrectly typecasted data is ingested or processed upon. One such example
      // of inconsistency is that functions such as TIME_PARSE evaluate incorrectly
      Optional<ColumnType> timestampColumnTypeOptional = signature.getColumnType(ColumnHolder.TIME_COLUMN_NAME);
      if (timestampColumnTypeOptional.isPresent() && !timestampColumnTypeOptional.get().equals(ColumnType.LONG)) {
        throw new ISE("EXTERN function with __time column can be used when __time column is of type long. "
                      + "Please change the column name to something other than __time");
      }

      return new ExternalTable(
            new ExternalDataSource(inputSource, inputFormat, signature),
            signature,
            jsonMapper
      );
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<FunctionParameter> getParameters()
  {
    return ImmutableList.of(
        new FunctionParameter()
        {
          @Override
          public int getOrdinal()
          {
            return 0;
          }

          @Override
          public String getName()
          {
            return "inputSource";
          }

          @Override
          public RelDataType getType(RelDataTypeFactory typeFactory)
          {
            return typeFactory.createJavaType(String.class);
          }

          @Override
          public boolean isOptional()
          {
            return false;
          }
        },
        new FunctionParameter()
        {
          @Override
          public int getOrdinal()
          {
            return 1;
          }

          @Override
          public String getName()
          {
            return "inputFormat";
          }

          @Override
          public RelDataType getType(RelDataTypeFactory typeFactory)
          {
            return typeFactory.createJavaType(String.class);
          }

          @Override
          public boolean isOptional()
          {
            return false;
          }
        },
        new FunctionParameter()
        {
          @Override
          public int getOrdinal()
          {
            return 2;
          }

          @Override
          public String getName()
          {
            return "signature";
          }

          @Override
          public RelDataType getType(RelDataTypeFactory typeFactory)
          {
            return typeFactory.createJavaType(String.class);
          }

          @Override
          public boolean isOptional()
          {
            return false;
          }
        }
    );
  }
}
