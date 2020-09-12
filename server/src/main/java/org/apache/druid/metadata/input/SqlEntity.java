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

package org.apache.druid.metadata.input;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.SQLFirehoseDatabaseConnector;
import org.apache.druid.metadata.SQLMetadataStorageActionHandler;
import org.skife.jdbi.v2.ResultIterator;
import org.skife.jdbi.v2.exceptions.ResultSetException;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a rdbms based input resource and knows how to read query results from the resource using SQL queries.
 */
public class SqlEntity implements InputEntity
{
  private static final Logger LOG = new Logger(SqlEntity.class);

  private final String sql;
  private final ObjectMapper objectMapper;
  private final SQLFirehoseDatabaseConnector sqlFirehoseDatabaseConnector;
  private final boolean foldCase;

  public SqlEntity(
      String sql,
      SQLFirehoseDatabaseConnector sqlFirehoseDatabaseConnector,
      boolean foldCase,
      ObjectMapper objectMapper
  )
  {
    this.sql = sql;
    this.sqlFirehoseDatabaseConnector = Preconditions.checkNotNull(
        sqlFirehoseDatabaseConnector,
        "SQL Metadata Connector not configured!"
    );
    this.foldCase = foldCase;
    this.objectMapper = objectMapper;
  }

  public String getSql()
  {
    return sql;
  }

  @Nullable
  @Override
  public URI getUri()
  {
    return null;
  }

  @Override
  public InputStream open()
  {
    throw new UnsupportedOperationException("Please use fetch() instead");
  }

  @Override
  public CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer) throws IOException
  {
    final File tempFile = File.createTempFile("druid-sql-entity", ".tmp", temporaryDirectory);
    return openCleanableFile(sql, sqlFirehoseDatabaseConnector, objectMapper, foldCase, tempFile);

  }

  /**
   * Executes a SQL query on the specified database and fetches the result into the given file.
   * The result file is deleted if the query execution or the file write fails.
   *
   * @param sql                          The SQL query to be executed
   * @param sqlFirehoseDatabaseConnector The database connector
   * @param objectMapper                 An object mapper, used for deserialization
   * @param foldCase                     A boolean flag used to enable or disabling case sensitivity while handling database column names
   *
   * @return A {@link InputEntity.CleanableFile} object that wraps the file containing the SQL results
   */

  public static CleanableFile openCleanableFile(
      String sql,
      SQLFirehoseDatabaseConnector sqlFirehoseDatabaseConnector,
      ObjectMapper objectMapper,
      boolean foldCase,
      File tempFile
  )
      throws IOException
  {
    try (FileOutputStream fos = new FileOutputStream(tempFile);
         final JsonGenerator jg = objectMapper.getFactory().createGenerator(fos);) {

      // Execute the sql query and lazily retrieve the results into the file in json format.
      // foldCase is useful to handle differences in case sensitivity behavior across databases.
      sqlFirehoseDatabaseConnector.retryWithHandle(
          (handle) -> {
            ResultIterator<Map<String, Object>> resultIterator = handle.createQuery(
                sql
            ).map(
                (index, r, ctx) -> {
                  Map<String, Object> resultRow = foldCase ? new CaseFoldedMap() : new HashMap<>();
                  ResultSetMetaData resultMetadata;
                  try {
                    resultMetadata = r.getMetaData();
                  }
                  catch (SQLException e) {
                    throw new ResultSetException("Unable to obtain metadata from result set", e, ctx);
                  }
                  try {
                    for (int i = 1; i <= resultMetadata.getColumnCount(); i++) {
                      String key = resultMetadata.getColumnName(i);
                      String alias = resultMetadata.getColumnLabel(i);
                      Object value = r.getObject(i);
                      resultRow.put(alias != null ? alias : key, value);
                    }
                  }
                  catch (SQLException e) {
                    throw new ResultSetException("Unable to access specific metadata from " +
                                                 "result set metadata", e, ctx);
                  }
                  return resultRow;
                }
            ).iterator();
            jg.writeStartArray();
            while (resultIterator.hasNext()) {
              jg.writeObject(resultIterator.next());
            }
            jg.writeEndArray();
            jg.close();
            return null;
          },
          (exception) -> sqlFirehoseDatabaseConnector.isTransientException(exception)
                         && !(SQLMetadataStorageActionHandler.isStatementException(exception))
      );
      return new CleanableFile()
      {
        @Override
        public File file()
        {
          return tempFile;
        }

        @Override
        public void close()
        {
          if (!tempFile.delete()) {
            LOG.warn("Failed to remove file[%s]", tempFile.getAbsolutePath());
          }
        }
      };
    }
    catch (Exception e) {
      if (!tempFile.delete()) {
        LOG.warn("Failed to remove file[%s]", tempFile.getAbsolutePath());
      }
      throw new IOException(e);
    }
  }

  private static class CaseFoldedMap extends HashMap<String, Object>
  {
    public static final long serialVersionUID = 1L;

    @Override
    public Object get(Object obj)
    {
      return super.get(obj == null ? null : StringUtils.toLowerCase((String) obj));
    }

    @Override
    public Object put(String key, Object value)
    {
      return super.put(key == null ? null : StringUtils.toLowerCase(key), value);
    }

    @Override
    public boolean containsKey(Object obj)
    {
      return super.containsKey(obj == null ? null : StringUtils.toLowerCase((String) obj));
    }
  }
}
