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

import com.google.common.collect.Iterables;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.catalog.model.table.export.ExportDestination;

import java.util.Map;

/**
 * Extends the {@link SqlIdentifier} to hold parameters for an external table destination. This contains information
 * required for a task to write to a destination.
 */
public class ExternalDestinationSqlIdentifier extends SqlIdentifier
{
  private final ExportDestination exportDestination;
  private final Map<String, String> propertiesForUnparse;

  public ExternalDestinationSqlIdentifier(
      String name,
      SqlParserPos pos,
      ExportDestination exportDestination,
      Map<String, String> propertiesForUnparse
  )
  {
    super(name, pos);
    this.exportDestination = exportDestination;
    this.propertiesForUnparse = propertiesForUnparse;
  }

  public ExportDestination getExportDestination()
  {
    return exportDestination;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    SqlWriter.Frame externFrame = writer.startFunCall("EXTERN");
    SqlWriter.Frame frame = writer.startFunCall(Iterables.getOnlyElement(names));
    for (Map.Entry<String, String> property : propertiesForUnparse.entrySet()) {
      writer.sep(",");
      writer.keyword(property.getKey());
      writer.print("=");
      writer.identifier(property.getValue(), false);
    }
    writer.endFunCall(frame);
    writer.endFunCall(externFrame);
  }

  @Override
  public SqlNode clone(SqlParserPos pos)
  {
    return new ExternalDestinationSqlIdentifier(Iterables.getOnlyElement(names), pos, exportDestination, propertiesForUnparse);
  }

  @Override
  @Deprecated
  public Object clone()
  {
    throw new UnsupportedOperationException("Function is deprecated, please use clone(SqlNode) instead.");
  }
}
