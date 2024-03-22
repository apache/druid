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

package org.apache.druid.sql.calcite.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.utils.CollectionUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * Extends the {@link SqlIdentifier} to hold parameters for an external destination.
 */
public class ExternalDestinationSqlIdentifier extends SqlIdentifier
{
  private final Map<String, String> properties;

  public ExternalDestinationSqlIdentifier(
      String name,
      SqlParserPos pos,
      Map<String, String> properties
  )
  {
    super(name, pos);
    this.properties = properties;
  }

  public String getDestinationType()
  {
    return CollectionUtils.getOnlyElement(
        names,
        x -> DruidException.defensive("Expected single name in external destination identifier, but got [%s]", names)
    );
  }

  public Map<String, String> getProperties()
  {
    return properties;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    SqlWriter.Frame externFrame = writer.startFunCall("EXTERN");
    SqlWriter.Frame frame = writer.startFunCall(getDestinationType());
    for (Map.Entry<String, String> property : properties.entrySet()) {
      writer.sep(",");
      writer.print(StringUtils.format("%s => '%s'", property.getKey(), property.getValue()));
    }
    writer.endFunCall(frame);
    writer.endFunCall(externFrame);
  }

  @Override
  public SqlNode clone(SqlParserPos pos)
  {
    final String name = CollectionUtils.getOnlyElement(
        names,
        x -> DruidException.defensive("Expected single name in external destination identifier, but got [%s]", names)
    );
    return new ExternalDestinationSqlIdentifier(name, pos, properties);
  }

  @Override
  @Deprecated
  public Object clone()
  {
    throw DruidException.defensive("Function is deprecated, please use clone(SqlNode) instead.");
  }

  public ExportStorageProvider toExportStorageProvider(ObjectMapper objectMapper)
  {
    final HashMap<String, String> storageConnectorProperties = new HashMap<>(properties);
    storageConnectorProperties.put("type", getDestinationType());

    return objectMapper.convertValue(
          storageConnectorProperties,
          ExportStorageProvider.class
      );
  }
}
