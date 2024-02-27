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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public enum ResultFormat
{
  ARRAY("array") {
    @Override
    public String contentType()
    {
      return MediaType.APPLICATION_JSON;
    }

    @Override
    public Writer createFormatter(final OutputStream outputStream, final ObjectMapper jsonMapper) throws IOException
    {
      return new ArrayWriter(outputStream, jsonMapper);
    }
  },

  ARRAYLINES("arrayLines") {
    @Override
    public String contentType()
    {
      return MediaType.TEXT_PLAIN;
    }

    @Override
    public Writer createFormatter(final OutputStream outputStream, final ObjectMapper jsonMapper) throws IOException
    {
      return new ArrayLinesWriter(outputStream, jsonMapper);
    }
  },

  CSV("csv") {
    @Override
    public String contentType()
    {
      return "text/csv";
    }

    @Override
    public Writer createFormatter(final OutputStream outputStream, final ObjectMapper jsonMapper)
    {
      return new CsvWriter(outputStream);
    }
  },

  OBJECT("object") {
    @Override
    public String contentType()
    {
      return MediaType.APPLICATION_JSON;
    }

    @Override
    public Writer createFormatter(final OutputStream outputStream, final ObjectMapper jsonMapper) throws IOException
    {
      return new ObjectWriter(outputStream, jsonMapper);
    }
  },

  OBJECTLINES("objectLines") {
    @Override
    public String contentType()
    {
      return MediaType.TEXT_PLAIN;
    }

    @Override
    public Writer createFormatter(final OutputStream outputStream, final ObjectMapper jsonMapper) throws IOException
    {
      return new ObjectLinesWriter(outputStream, jsonMapper);
    }
  };

  private final String name;

  ResultFormat(final String name)
  {
    this.name = name;
  }

  public abstract String contentType();

  public abstract Writer createFormatter(OutputStream outputStream, ObjectMapper jsonMapper) throws IOException;

  @Override
  @JsonValue
  public String toString()
  {
    return name;
  }

  public static final ResultFormat DEFAULT_RESULT_FORMAT = OBJECT;

  public interface Writer extends Closeable
  {
    /**
     * Start of the response, called once per writer.
     */
    void writeResponseStart() throws IOException;

    void writeHeader(RelDataType rowType, boolean includeTypes, boolean includeSqlTypes) throws IOException;

    void writeHeaderFromRowSignature(RowSignature rowSignature, boolean includeTypes) throws IOException;

    /**
     * Start of each result row.
     */
    void writeRowStart() throws IOException;

    /**
     * Field within a row.
     */
    void writeRowField(String name, @Nullable Object value) throws IOException;

    /**
     * End of each result row.
     */
    void writeRowEnd() throws IOException;

    /**
     * End of the response. Must allow the user to know that they have read all data successfully.
     */
    void writeResponseEnd() throws IOException;
  }

  @JsonCreator
  public static ResultFormat fromString(final String name)
  {
    return valueOf(StringUtils.toUpperCase(name));
  }
}
