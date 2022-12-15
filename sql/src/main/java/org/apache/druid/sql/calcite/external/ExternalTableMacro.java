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
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.table.DruidTable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Used by {@link ExternalOperatorConversion} to generate a {@link DruidTable}
 * that references an {@link ExternalDataSource}.
 *
 * This class is exercised in CalciteInsertDmlTest but is not currently exposed to end users.
 */
public class ExternalTableMacro implements TableMacro
{
  private final List<FunctionParameter> parameters = ImmutableList.of(
      new FunctionParameterImpl(0, "inputSource", DruidTypeSystem.TYPE_FACTORY.createJavaType(String.class)),
      new FunctionParameterImpl(1, "inputFormat", DruidTypeSystem.TYPE_FACTORY.createJavaType(String.class)),
      new FunctionParameterImpl(2, "signature", DruidTypeSystem.TYPE_FACTORY.createJavaType(String.class))
  );

  private final ObjectMapper jsonMapper;

  public ExternalTableMacro(final ObjectMapper jsonMapper)
  {
    this.jsonMapper = jsonMapper;
  }

  public String signature()
  {
    final List<String> names = parameters.stream().map(p -> p.getName()).collect(Collectors.toList());
    return "(" + String.join(", ", names) + ")";
  }

  @Override
  public TranslatableTable apply(final List<Object> arguments)
  {
    try {
      ExternalTableSpec spec = new ExternalTableSpec(
          jsonMapper.readValue((String) arguments.get(0), InputSource.class),
          jsonMapper.readValue((String) arguments.get(1), InputFormat.class),
          jsonMapper.readValue((String) arguments.get(2), RowSignature.class)
      );
      return Externals.buildExternalTable(spec, jsonMapper);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<FunctionParameter> getParameters()
  {
    return parameters;
  }
}
