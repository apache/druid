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

package org.apache.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.server.security.ForbiddenException;

import javax.annotation.Nullable;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;

public abstract class ResultPusher
{
  private static final Logger log = new Logger(ResultPusher.class);

  private final HttpServletResponse response;
  private final String queryId;
  private final ObjectMapper jsonMapper;
  private final ResponseContextConfig responseContextConfig;
  private final DruidNode selfNode;
  private final QueryResource.QueryMetricCounter counter;
  private final MediaType contentType;

  private StreamingHttpResponseAccumulator accumulator = null;

  public ResultPusher(
      HttpServletResponse response,
      ObjectMapper jsonMapper,
      ResponseContextConfig responseContextConfig,
      DruidNode selfNode,
      QueryResource.QueryMetricCounter counter,
      String queryId,
      MediaType contentType
  )
  {
    this.response = response;
    this.queryId = queryId;
    this.jsonMapper = jsonMapper;
    this.responseContextConfig = responseContextConfig;
    this.selfNode = selfNode;
    this.counter = counter;
    this.contentType = contentType;
  }

  /**
   * Starts a ResultsWriter, which encapsulates logic to run the query, serialize it and also report success/failure
   * <p>
   * This response must not be null.  The job of this ResultsWriter is largely to provide lifecycle management to
   * the query running and reporting, so this object must never be null.  Also, this start() method should do as little
   * work as possible.
   *
   * @return
   */
  public abstract ResultsWriter start();

  public abstract void writeException(Exception e, OutputStream out) throws IOException;

  public void push()
  {
    response.setHeader(QueryResource.QUERY_ID_RESPONSE_HEADER, queryId);

    ResultsWriter resultsWriter = null;
    try {
      resultsWriter = start();


      final QueryResponse<Object> queryResponse = resultsWriter.start(response);
      if (queryResponse == null) {
        // It's already been handled...
        return;
      }

      final Sequence<Object> results = queryResponse.getResults();

      accumulator = new StreamingHttpResponseAccumulator(
          ResultPusher.this.response,
          queryId,
          queryResponse.getResponseContext(),
          jsonMapper,
          responseContextConfig,
          resultsWriter,
          selfNode,
          contentType
      );

      results.accumulate(null, accumulator);
      accumulator.flush();

      counter.incrementSuccess();
      accumulator.close();
      resultsWriter.recordSuccess(accumulator.getNumBytesSent());
    }
    catch (QueryException e) {
      handleQueryException(resultsWriter, e);
      return;
    }
    catch (RuntimeException re) {
      if (re instanceof ForbiddenException) {
        // Forbidden exceptions are special, they get thrown instead of serialized.  They happen before the response
        // has been committed because the response is committed after results are returned.  And, if we started
        // returning results before a ForbiddenException gets thrown, that means that we've already leaked stuff
        // that should not have been leaked.  I.e. it means, we haven't validated the authorization early enough.
        if (response.isCommitted()) {
          log.error(re, "Got a forbidden exception for query[%s] after the response was already committed.", queryId);
        }
        throw re;
      }
      handleQueryException(resultsWriter, new QueryInterruptedException(re));
      return;
    }
    catch (IOException ioEx) {
      handleQueryException(resultsWriter, new QueryInterruptedException(ioEx));
      return;
    }
    finally {
      if (accumulator != null) {
        try {
          accumulator.close();
        }
        catch (IOException e) {
          log.warn(e, "Suppressing exception closing accumulator for query[%s]", queryId);
        }
      }
      if (resultsWriter == null) {
        log.warn("resultsWriter was null for query[%s], work was maybe done in start() that shouldn't be.", queryId);
      } else {
        try {
          resultsWriter.close();
        }
        catch (IOException e) {
          log.warn(e, "Suppressing exception closing accumulator for query[%s]", queryId);
        }
      }
    }
  }

  private void handleQueryException(ResultsWriter resultsWriter, QueryException e)
  {
    if (accumulator != null && accumulator.isInitialized()) {
      // We already started sending a response when we got the error message.  In this case we just give up
      // and hope that the partial stream generates a meaningful failure message for our client.  We could consider
      // also throwing the exception body into the response to make it easier for the client to choke if it manages
      // to parse a meaningful object out, but that's potentially an API change so we leave that as an exercise for
      // the future.

      resultsWriter.recordFailure(e);

      // This case is always a failure because the error happened mid-stream of sending results back.  Therefore,
      // we do not believe that the response stream was actually useable
      counter.incrementFailed();
      return;
    }

    if (response.isCommitted()) {
      QueryResource.NO_STACK_LOGGER.warn(e, "Response was committed without the accumulator writing anything!?");
    }

    final QueryException.FailType failType = e.getFailType();
    switch (failType) {
      case USER_ERROR:
      case QUERY_RUNTIME_FAILURE:
      case SERVER_ERROR:
      case CANCELED:
        counter.incrementInterrupted();
        break;
      case CAPACITY_EXCEEDED:
      case UNSUPPORTED:
        counter.incrementFailed();
        break;
      case TIMEOUT:
        counter.incrementTimedOut();
        break;
      case UNKNOWN:
        log.warn(
            e,
            "Unknown errorCode[%s], support needs to be added for error handling.",
            e.getErrorCode()
        );
        counter.incrementFailed();
    }
    final int responseStatus = failType.getExpectedStatus();

    response.setStatus(responseStatus);
    response.setHeader("Content-Type", contentType.toString());
    try (ServletOutputStream out = response.getOutputStream()) {
      writeException(e, out);
    }
    catch (IOException ioException) {
      log.warn(
          ioException,
          "Suppressing IOException thrown sending error response for query[%s]",
          queryId
      );
    }

    resultsWriter.recordFailure(e);
  }

  public interface ResultsWriter extends Closeable
  {
    /**
     * Runs the query and returns a ResultsWriter from running the query.
     * <p>
     * This also serves as a hook for any logic that runs on the metadata from a QueryResponse.  If this method
     * returns {@code null} then the Pusher believes that the response was already handled and skips the rest
     * of its logic.  As such, any implementation that returns null must make sure that the response has been set
     * with a meaningful status, etc.
     * <p>
     * Even if this method returns null, close() should still be called on this object.
     *
     * @return QueryResponse or null if no more work to do.
     */
    @Nullable
    QueryResponse<Object> start(HttpServletResponse response);

    Writer makeWriter(OutputStream out) throws IOException;

    void recordSuccess(long numBytes);

    void recordFailure(Exception e);
  }

  public interface Writer extends Closeable
  {
    /**
     * Start of the response, called once per writer.
     */
    void writeResponseStart() throws IOException;

    /**
     * Write a row
     *
     * @param obj object representing the row
     */
    void writeRow(Object obj) throws IOException;

    /**
     * End of the response. Must allow the user to know that they have read all data successfully.
     */
    void writeResponseEnd() throws IOException;
  }
}
