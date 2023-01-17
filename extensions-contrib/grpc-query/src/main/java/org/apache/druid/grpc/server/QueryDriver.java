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

package org.apache.druid.grpc.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.NativeQuery;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.sql.DirectStatement;
import org.apache.druid.sql.DirectStatement.ResultSet;
import org.apache.druid.sql.SqlPlanningException;
import org.apache.druid.sql.SqlQueryPlus;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.SqlStatementFactory;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.sql.http.SqlParameter;
import org.druid.grpc.proto.QueryOuterClass.ColumnSchema;
import org.druid.grpc.proto.QueryOuterClass.DruidType;
import org.druid.grpc.proto.QueryOuterClass.QueryParameter;
import org.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.druid.grpc.proto.QueryOuterClass.QueryStatus;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * "Driver" for the gRPC query endpoint. Handles translating the gRPC {@link QueryRequest}
 * into Druid's internal formats, running the query, and translating the results into a
 * gRPC {@link QueryResponse}. Allows for easier unit testing as we separate the machinery
 * of running a query, given the request, from the gRPC server machinery.
 */
public class QueryDriver
{
  /**
   * Internal runtime exception to report request errors.
   */
  private static class RequestError extends RuntimeException
  {
    public RequestError(String msg)
    {
      super(msg);
    }
  }

  private final ObjectMapper jsonMapper;
  private final SqlStatementFactory sqlStatementFactory;

  @Inject
  public QueryDriver(
      final @Json ObjectMapper jsonMapper,
      final @NativeQuery SqlStatementFactory sqlStatementFactory
  )
  {
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
    this.sqlStatementFactory = Preconditions.checkNotNull(sqlStatementFactory, "sqlStatementFactory");
  }

  /**
   * First-cut synchronous query handler. Druid prefers to stream results, in
   * part to avoid overly-short network timeouts. However, for now, we simply run
   * the query within this call and prepare the Protobuf response. Async handling
   * can come later.
   */
  public QueryResponse submitQuery(QueryRequest request, AuthenticationResult authResult)
  {
    final SqlQueryPlus queryPlus = translateQuery(request, authResult);
    final DirectStatement stmt = sqlStatementFactory.directStatement(queryPlus);
    final String currThreadName = Thread.currentThread().getName();
    try {
      Thread.currentThread().setName(StringUtils.format("sql[%s]", stmt.sqlQueryId()));
      final ResultSet thePlan = stmt.plan();
      final SqlRowTransformer rowTransformer = thePlan.createRowTransformer();
      final ByteString results = encodeResults(request.getResultFormat(), thePlan, rowTransformer);
      stmt.reporter().succeeded(0); // TODO: real byte count (of payload)
      stmt.close();
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.OK)
          .setData(results)
          .clearErrorMessage()
          .addAllColumns(encodeColumns(rowTransformer))
          .build();
    }
    catch (ForbiddenException e) {
      stmt.reporter().failed(e);
      stmt.close();
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.UNAUTHORIZED)
          .setErrorMessage(Access.DEFAULT_ERROR_MESSAGE)
          .build();
    }
    catch (RequestError e) {
      stmt.reporter().failed(e);
      stmt.close();
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.REQUEST_ERROR)
          .setErrorMessage(Access.DEFAULT_ERROR_MESSAGE)
          .build();
    }
    catch (SqlPlanningException e) {
      stmt.reporter().failed(e);
      stmt.close();
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.INVALID_SQL)
          .setErrorMessage(e.getMessage())
          .build();
    }
    catch (IOException | RuntimeException e) {
      stmt.reporter().failed(e);
      stmt.close();
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.RUNTIME_ERROR)
          .setErrorMessage(e.getMessage())
          .build();
    }
    // There is a claim that Calcite sometimes throws a java.lang.AssertionError, but we do not have a test that can
    // reproduce it checked into the code (the best we have is something that uses mocks to throw an Error, which is
    // dubious at best).  We keep this just in case, but it might be best to remove it and see where the
    // AssertionErrors are coming from and do something to ensure that they don't actually make it out of Calcite
    catch (AssertionError e) {
      stmt.reporter().failed(e);
      stmt.close();
      return QueryResponse.newBuilder()
          .setQueryId(stmt.sqlQueryId())
          .setStatus(QueryStatus.RUNTIME_ERROR)
          .setErrorMessage(e.getMessage())
          .build();
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  /**
   * Convert the rRPC query format to the internal {@link SqlQueryPlus} format.
   */
  private SqlQueryPlus translateQuery(QueryRequest request, AuthenticationResult authResult)
  {
    return SqlQueryPlus.builder()
        .sql(request.getQuery())
        .context(translateContext(request))
        .sqlParameters(translateParameters(request))
        .auth(authResult)
        .build();
  }

  /**
   * Translate the query context from the gRPC format to the internal format. When
   * read from REST/JSON, the JSON translator will convert the type of each value
   * into a number, Boolean, etc. gRPC has no similar feature. Rather than clutter up
   * the gRPC request with typed context values, we rely on the existing code that can
   * translate string values to the desired type on the fly. Thus, we build up a
   * {@code Map<String, String>}.
   */
  private Map<String, Object> translateContext(QueryRequest request)
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    if (request.getContextCount() > 0) {
      for (Map.Entry<String, String> entry : request.getContextMap().entrySet()) {
        builder.put(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  /**
   * Convert the gRPC parameter format to the internal Druid {@link SqlParameter}
   * format. That format is then again translated by the {@link SqlQueryPlus} class.
   */
  private List<SqlParameter> translateParameters(QueryRequest request)
  {
    if (request.getParametersCount() == 0) {
      return null;
    }
    List<SqlParameter> params = new ArrayList<>();
    for (QueryParameter value : request.getParametersList()) {
      params.add(translateParameter(value));
    }
    return params;
  }

  private SqlParameter translateParameter(QueryParameter value)
  {
    switch (value.getValueCase()) {
      case ARRAYVALUE:
        // Not yet supported: waiting for an open PR
        return null;
      case DOUBLEVALUE:
        return new SqlParameter(SqlType.DOUBLE, value.getDoubleValue());
      case LONGVALUE:
        return new SqlParameter(SqlType.BIGINT, value.getLongValue());
      case STRINGVALUE:
        return new SqlParameter(SqlType.VARCHAR, value.getStringValue());
      case NULLVALUE:
      case VALUE_NOT_SET:
        return null;
      default:
        throw new RequestError("Invalid parameter type: " + value.getValueCase().name());
    }
  }

  /**
   * Translate the column schema from the Druid internal form to the gRPC
   * {@link ColumnSchema} form. Note that since the gRPC response returns the
   * schema, none of the data formats include a header. This makes the data format
   * simpler and cleaner.
   */
  private Iterable<? extends ColumnSchema> encodeColumns(SqlRowTransformer rowTransformer)
  {
    RelDataType rowType = rowTransformer.getRowType();
    final RowSignature signature = RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType);
    List<ColumnSchema> cols = new ArrayList<>();
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      ColumnSchema col = ColumnSchema.newBuilder()
          .setName(signature.getColumnName(i))
          .setSqlType(rowType.getFieldList().get(i).getType().getSqlTypeName().getName())
          .setDruidType(convertDruidType(signature.getColumnType(i)))
          .build();
      cols.add(col);
    }
    return cols;
  }

  /**
   * Convert from Druid's internal format of the Druid data type to the gRPC form.
   */
  private DruidType convertDruidType(Optional<ColumnType> colType)
  {
    if (!colType.isPresent()) {
      return DruidType.UNRECOGNIZED;
    }
    ColumnType druidType = colType.get();
    if (druidType == ColumnType.STRING) {
      return DruidType.STRING;
    }
    if (druidType == ColumnType.STRING_ARRAY) {
      return DruidType.STRING_ARRAY;
    }
    if (druidType == ColumnType.LONG) {
      return DruidType.LONG;
    }
    if (druidType == ColumnType.LONG_ARRAY) {
      return DruidType.LONG_ARRAY;
    }
    if (druidType == ColumnType.FLOAT) {
      return DruidType.FLOAT;
    }
    if (druidType == ColumnType.FLOAT_ARRAY) {
      return DruidType.FLOAT_ARRAY;
    }
    if (druidType == ColumnType.DOUBLE) {
      return DruidType.DOUBLE;
    }
    if (druidType == ColumnType.DOUBLE_ARRAY) {
      return DruidType.DOUBLE_ARRAY;
    }
    if (druidType == ColumnType.UNKNOWN_COMPLEX) {
      return DruidType.COMPLEX;
    }
    return DruidType.UNRECOGNIZED;
  }

  /**
   * Generic mechanism to write query results to one of the supported gRPC formats.
   */
  public interface GrpcResultWriter
  {
    void start() throws IOException;
    void writeRow(Object[] row) throws IOException;
    void close() throws IOException;
  }

  /**
   * Writer for the SQL result formats. Reuses the SQL format writer implementations.
   * Note: gRPC does not use the headers: schema information is available in the
   * rRPC response.
   */
  public static class GrpcResultFormatWriter implements GrpcResultWriter
  {
    protected final ResultFormat.Writer formatWriter;
    protected final SqlRowTransformer rowTransformer;

    public GrpcResultFormatWriter(
        final ResultFormat.Writer formatWriter,
        final SqlRowTransformer rowTransformer
    )
    {
      this.formatWriter = formatWriter;
      this.rowTransformer = rowTransformer;
    }

    @Override
    public void start() throws IOException
    {
    }

    @Override
    public void writeRow(Object[] row) throws IOException
    {
      formatWriter.writeRowStart();
      for (int i = 0; i < rowTransformer.getFieldList().size(); i++) {
        final Object value = rowTransformer.transform(row, i);
        formatWriter.writeRowField(rowTransformer.getFieldList().get(i), value);
      }
      formatWriter.writeRowEnd();
    }

    @Override
    public void close() throws IOException
    {
      formatWriter.close();
    }
  }

  /**
   * Internal runtime exception to pass {@link IOException}s though the
   * {@link Sequence} {@link Accumulator} protocol.
   */
  private static class ResponseError extends RuntimeException
  {
    public ResponseError(IOException e)
    {
      super(e);
    }
  }

  /**
   * Druid query results use a complex {@link Sequence} mechanism. This class uses an
   * {@link Accumulator} to walk the results and present each to the associated
   * results writer. This is a very rough analogy of the {@code SqlResourceQueryResultPusher}
   * in the REST {@code SqlResource} class.
   */
  public static class GrpcResultsAccumulator implements Accumulator<Void, Object[]>
  {
    private final GrpcResultWriter writer;

    public GrpcResultsAccumulator(final GrpcResultWriter writer)
    {
      this.writer = writer;
    }

    public void push(org.apache.druid.server.QueryResponse<Object[]> queryResponse) throws IOException
    {
      final Sequence<Object[]> results = queryResponse.getResults();
      writer.start();
      try {
        results.accumulate(null, this);
      }
      catch (ResponseError e) {
        throw (IOException) e.getCause();
      }
      writer.close();
    }

    @Override
    public Void accumulate(Void accumulated, Object[] in)
    {
      try {
        writer.writeRow(in);
      }
      catch (IOException e) {
        throw new ResponseError(e);
      }
      return null;
    }
  }

  /**
   * Convert the query results to a set of bytes to be attached to the query response.
   * <p>
   * This version is pretty basic: the results are materialized as a byte array. That's
   * fine for small result sets, but should be rethought for larger result sets.
   */
  private ByteString encodeResults(
      final QueryResultFormat queryResultFormat,
      final ResultSet thePlan,
      final SqlRowTransformer rowTransformer
  ) throws IOException
  {
    // Accumulate the results as a byte array.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GrpcResultWriter writer;

    // For the SQL-supported formats, use the SQL-provided writers.
    switch (queryResultFormat) {
      case CSV:
        writer = new GrpcResultFormatWriter(
            ResultFormat.CSV.createFormatter(out, jsonMapper),
            rowTransformer
        );
        break;
      case JSON_ARRAY:
        writer = new GrpcResultFormatWriter(
            ResultFormat.ARRAY.createFormatter(out, jsonMapper),
            rowTransformer
        );
        break;
      case JSON_ARRAY_LINES:
        writer = new GrpcResultFormatWriter(
            ResultFormat.ARRAYLINES.createFormatter(out, jsonMapper),
            rowTransformer
        );
        break;

      // TODO: Provide additional writers for the other formats which we
      // want in gRPC.
      case JSON_OBJECT:
        throw new UnsupportedOperationException(); // TODO
      case JSON_OBJECT_LINES:
        throw new UnsupportedOperationException(); // TODO
      case PROTOBUF_INLINE:
        throw new UnsupportedOperationException(); // TODO

      // This is the hard one: encode the results as a Protobuf array.
      case PROTOBUF_RESPONSE:
        throw new UnsupportedOperationException(); // TODO
      default:
        throw new RequestError("Unsupported query result format");
    }
    GrpcResultsAccumulator accumulator = new GrpcResultsAccumulator(writer);
    accumulator.push(thePlan.run());
    return ByteString.copyFrom(out.toByteArray());
  }
}
