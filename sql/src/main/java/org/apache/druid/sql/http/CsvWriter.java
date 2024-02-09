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

package org.apache.druid.sql.http;

import com.opencsv.CSVWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.sql.calcite.table.RowSignatures;

import javax.annotation.Nullable;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class CsvWriter implements ResultFormat.Writer
{
  private final OutputStream outputStream;
  private final CSVWriter writer;
  private final List<String> currentLine = new ArrayList<>();

  public CsvWriter(final OutputStream outputStream)
  {
    this.outputStream = outputStream;
    this.writer = new CSVWriter(new BufferedWriter(new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)));
  }

  @Override
  public void writeResponseStart()
  {
    // Do nothing.
  }

  @Override
  public void writeResponseEnd() throws IOException
  {
    writer.flush();

    // Write an extra blank line, so users can tell the response was not cut off.
    outputStream.write('\n');
    outputStream.flush();
  }

  @Override
  public void writeHeader(
      final RelDataType rowType,
      final boolean includeTypes,
      final boolean includeSqlTypes
  )
  {
    final RowSignature signature = RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType);

    writer.writeNext(signature.getColumnNames().toArray(new String[0]), false);

    if (includeTypes) {
      final String[] types = new String[rowType.getFieldCount()];

      for (int i = 0; i < signature.size(); i++) {
        types[i] = signature.getColumnType(i).map(TypeSignature::asTypeString).orElse(null);
      }

      writer.writeNext(types, false);
    }

    if (includeSqlTypes) {
      final String[] sqlTypes = new String[rowType.getFieldCount()];

      for (int i = 0; i < signature.size(); i++) {
        sqlTypes[i] = rowType.getFieldList().get(i).getType().getSqlTypeName().getName();
      }

      writer.writeNext(sqlTypes, false);
    }
  }

  @Override
  public void writeHeaderFromRowSignature(final RowSignature signature, final boolean includeTypes)
  {
    writer.writeNext(signature.getColumnNames().toArray(new String[0]), false);

    if (includeTypes) {
      final String[] types = new String[signature.size()];

      for (int i = 0; i < signature.size(); i++) {
        types[i] = signature.getColumnType(i).map(TypeSignature::asTypeString).orElse(null);
      }

      writer.writeNext(types, false);
    }
  }

  @Override
  public void writeRowStart()
  {
    // Do nothing.
  }

  @Override
  public void writeRowField(final String name, @Nullable final Object value)
  {
    if (value == null) {
      // CSV cannot differentiate null and empty string.
      currentLine.add("");
    } else if (value instanceof String) {
      currentLine.add((String) value);
    } else {
      currentLine.add(value.toString());
    }
  }

  @Override
  public void writeRowEnd()
  {
    // Avoid writing blank lines, users may confuse them with the trailer.
    final boolean quoteEverything = currentLine.size() == 1 && currentLine.get(0).isEmpty();

    writer.writeNext(currentLine.toArray(new String[0]), quoteEverything);
    currentLine.clear();
  }

  @Override
  public void close() throws IOException
  {
    writer.close();
  }
}
