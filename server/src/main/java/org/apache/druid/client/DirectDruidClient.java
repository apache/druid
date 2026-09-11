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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileConstants;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ConcurrentResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.QueryResource;
import org.apache.druid.utils.CloseableUtils;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DirectDruidClient<T> implements QueryRunner<T>
{
  public static final String QUERY_FAIL_TIME = "queryFailTime";

  private static final Logger log = new Logger(DirectDruidClient.class);
  private static final int VAL_TO_REDUCE_REMAINING_RESPONSES = -1;

  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QueryWatcher queryWatcher;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final String scheme;
  private final String host;
  private final ServiceEmitter emitter;

  private final AtomicInteger openConnections;
  private final boolean isSmile;
  private final ScheduledExecutorService queryCancellationExecutor;

  /**
   * Removes the magical fields added by {@link #makeResponseContextForQuery()}.
   */
  public static void removeMagicResponseContextFields(ResponseContext responseContext)
  {
    responseContext.remove(ResponseContext.Keys.QUERY_TOTAL_BYTES_GATHERED);
    responseContext.remove(ResponseContext.Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS);
  }

  public static ConcurrentResponseContext makeResponseContextForQuery()
  {
    final ConcurrentResponseContext responseContext = ConcurrentResponseContext.createEmpty();
    responseContext.initialize();
    return responseContext;
  }

  public DirectDruidClient(
      QueryRunnerFactoryConglomerate conglomerate,
      QueryWatcher queryWatcher,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      String scheme,
      String host,
      ServiceEmitter emitter,
      ScheduledExecutorService queryCancellationExecutor
  )
  {
    this.conglomerate = conglomerate;
    this.queryWatcher = queryWatcher;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
    this.scheme = scheme;
    this.host = host;
    this.emitter = emitter;

    this.isSmile = this.objectMapper.getFactory() instanceof SmileFactory;
    this.openConnections = new AtomicInteger();
    this.queryCancellationExecutor = queryCancellationExecutor;
  }

  public int getNumOpenConnections()
  {
    return openConnections.get();
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, final ResponseContext context)
  {
    final Query<T> query = queryPlus.getQuery();
    QueryToolChest<T, Query<T>> toolChest = conglomerate.getToolChest(query);
    boolean isBySegment = query.context().isBySegment();
    final JavaType queryResultType = isBySegment ? toolChest.getBySegmentResultType() : toolChest.getBaseResultType();

    final ListenableFuture<InputStream> future;
    final String url = scheme + "://" + host + "/druid/v2/";
    final String cancelUrl = url + query.getId();

    try {
      log.debug("Querying queryId [%s] url [%s]", query.getId(), url);

      final long requestStartTimeNs = System.nanoTime();
      final QueryContext queryContext = query.context();
      // Will NPE if the value is not set.
      final long timeoutAt = queryContext.getLong(QUERY_FAIL_TIME);
      final long maxScatterGatherBytes = queryContext.getMaxScatterGatherBytes();
      final AtomicLong totalBytesGathered = context.getTotalBytes();
      final long maxQueuedBytes = queryContext.getMaxQueuedBytes(0);
      final boolean usingBackpressure = maxQueuedBytes > 0;

      final HttpResponseHandler<InputStream, InputStream> responseHandler = new HttpResponseHandler<>()
      {
        private final AtomicLong totalByteCount = new AtomicLong(0);
        private final AtomicLong queuedByteCount = new AtomicLong(0);
        private final AtomicLong channelSuspendedTime = new AtomicLong(0);
        private final BlockingQueue<InputStreamHolder> queue = new LinkedBlockingQueue<>();
        private final AtomicBoolean done = new AtomicBoolean(false);
        // Set when the consumer closes the result stream early (see the SequenceInputStream.close() override in
        // handleResponse). Once set, incoming chunks are dropped rather than buffered.
        private final AtomicBoolean discard = new AtomicBoolean(false);
        private final AtomicBoolean nodeMetricsEmitted = new AtomicBoolean(false);
        // Tracks whether the response body's leading (non-whitespace) byte has been classified as JSON or
        // HTML. For chunked responses the initial HttpResponse can arrive with an empty or all-whitespace body,
        // in which case the check is retried against each subsequent HttpChunk until it resolves.
        private final AtomicBoolean bodyPrefixResolved = new AtomicBoolean(false);
        // HTTP status of the initial response, remembered so that an HTML body first seen in a later chunk of a
        // 429/503 reply is still reported as capacity-exceeded rather than as a generic HTML-instead-of-JSON error.
        private volatile int responseStatusCode = -1;
        // Netty 4 delivers the body after the headers, so the Content-Type recorded in handleResponse has
        // to survive until the first chunk arrives to be usable in the non-JSON check.
        private volatile String responseContentType = null;
        // Message and (when available) original exception for a read failure, published together through a single
        // reference so a reader that observes a non-null Failure also sees its cause. Publishing them as two
        // separate AtomicReferences (a "fail" flag written before a "failCause" detail) would let a concurrent
        // SequenceInputStream callback observe the flag before the cause and fall back to a generic RE, losing a
        // typed exception like QueryCapacityExceededException thrown by failIfNonJsonBody from a later chunk.
        private final AtomicReference<Failure> failure = new AtomicReference<>();

        /**
         * Immutable pairing of a failure message with its originating exception, when one exists. Published as a
         * single unit through {@link #failure} so the message and cause are always visible together.
         */
        private static final class Failure
        {
          private final String message;
          private final Throwable cause;

          private Failure(String message, Throwable cause)
          {
            this.message = message;
            this.cause = cause;
          }
        }

        private final AtomicReference<TrafficCop> trafficCopRef = new AtomicReference<>();

        private QueryMetrics<? super Query<T>> queryMetrics;
        private long responseStartTimeNs;

        private QueryMetrics<? super Query<T>> acquireResponseMetrics()
        {
          if (queryMetrics == null) {
            queryMetrics = toolChest.makeMetrics(query);
            queryMetrics.server(host);
          }
          return queryMetrics;
        }

        /**
         * Queue a buffer. Returns true if we should keep reading, false otherwise.
         */
        private boolean enqueue(ByteBuf buffer, long chunkNum) throws InterruptedException
        {
          // If the consumer has abandoned the response (see the SequenceInputStream.close() override below), drop the
          // chunk instead of buffering it, and keep reads flowing (continueReading = true) so we never suspend the
          // channel while it is being wound down.
          if (discard.get()) {
            return true;
          }
          // Increment queuedByteCount before queueing the object, so queuedByteCount is at least as high as
          // the actual number of queued bytes at any particular time.
          final InputStreamHolder holder = InputStreamHolder.fromChannelBuffer(buffer, chunkNum);
          final long currentQueuedByteCount = queuedByteCount.addAndGet(holder.getLength());
          queue.put(holder);

          // True if we should keep reading.
          return !usingBackpressure || currentQueuedByteCount < maxQueuedBytes;
        }

        private InputStream dequeue() throws InterruptedException
        {
          final InputStreamHolder holder = queue.poll(checkQueryTimeout(), TimeUnit.MILLISECONDS);
          if (holder == null) {
            throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query[%s] url[%s] timed out.", query.getId(), url));
          }

          final long currentQueuedByteCount = queuedByteCount.addAndGet(-holder.getLength());
          if (usingBackpressure && currentQueuedByteCount < maxQueuedBytes) {
            long backPressureTime = Preconditions.checkNotNull(trafficCopRef.get(), "No TrafficCop, how can this be?")
                                                 .resume(holder.getChunkNum());
            channelSuspendedTime.addAndGet(backPressureTime);
          }

          return holder.getStream();
        }

        /**
         * Scans past leading whitespace in {@code buffer} looking for the first content byte, without consuming
         * (advancing the reader index of) the buffer, and returns it. Once a non-whitespace byte is found, the prefix
         * is considered resolved (see {@link #bodyPrefixResolved}) and later calls return null. If {@code buffer} is
         * empty or entirely whitespace, the prefix remains unresolved (and null is returned) so a later call, from a
         * subsequent chunk, can retry the check; this matters for chunked responses, where the initial
         * {@link HttpResponse} can carry an empty body and the real content, HTML or otherwise, only arrives via
         * {@link #handleChunk}.
         */
        @Nullable
        private Byte bodyPrefixByte(ByteBuf buffer)
        {
          if (bodyPrefixResolved.get()) {
            return null;
          }
          final int readerIndex = buffer.readerIndex();
          final int readable = buffer.readableBytes();
          for (int i = 0; i < readable; i++) {
            byte b = buffer.getByte(readerIndex + i);
            if (b == ' ' || b == '\n' || b == '\r' || b == '\t') {
              continue;
            }
            bodyPrefixResolved.set(true);
            return b;
          }
          return null;
        }

        /**
         * Classifies the body prefix in {@code buffer} (see {@link #bodyPrefixByte}) and fails the query if it is not
         * JSON (or Smile, when the request was sent as Smile per {@link #isSmile}). HTML always fails; any other
         * non-JSON/non-Smile body fails only when the status is 429/503, since Druid itself never sends such a body
         * with those statuses but proxies routinely do (an HTML error page from nginx or a load balancer, a
         * plain-text "upstream connect error" from Envoy). A structured body in the request's own format, whatever
         * the status, is left alone so that the normal parse path can surface the server's own structured error.
         *
         * @param contentType Content-Type header of the initial response, possibly null; a text/html value fails the
         *                    query regardless of the body prefix
         * @param chunkNum    0 for the initial response body, else the chunk number
         */
        private void failIfNonJsonBody(String contentType, ByteBuf buffer, long chunkNum)
        {
          final boolean isHtmlContentType =
              contentType != null && StringUtils.toLowerCase(contentType).contains("text/html");
          final Byte prefix = bodyPrefixByte(buffer);
          final boolean isHtml = isHtmlContentType || (prefix != null && prefix == '<');
          // A data server negotiates its response format from the request (ResourceIOReaderWriterFactory#factorize),
          // so a Smile request gets a Smile response, error bodies included; those begin with the Smile format
          // header's 0x3a byte rather than JSON's '{'/'['. Checking only '{'/'[' here would misclassify every
          // structured Smile 429/503 body as non-JSON and discard the server's real error.
          final boolean isNonJson = isSmile
                                     ? prefix != null && prefix != SmileConstants.HEADER_BYTE_1
                                     : prefix != null && prefix != '{' && prefix != '[';
          final int statusCode = responseStatusCode;
          if (isHtml || (isNonJson && (statusCode == 429 || statusCode == 503))) {
            throwForNonJsonBody(statusCode, contentType, buffer, chunkNum, isHtml);
          }
        }

        /**
         * Fails the query because the response body is not JSON, typically an error page produced by a load balancer
         * or reverse proxy sitting in front of the data server. A 429/503 status is reported as
         * {@link QueryCapacityExceededException} since that is what such intermediaries return when the server is
         * over capacity; any other status is reported as a {@link QueryInterruptedException}. Either way, the caller
         * gets a message that says what actually came back instead of a {@code JsonParseException} on {@code '<'}.
         * <p>
         * The exception message never includes the body itself. The broker-to-data-server response is not a trusted
         * boundary (it can be an error page from any proxy sitting in between), and this message is logged by
         * {@link JsonParserIterator} and can reach the query error/trailer, so it is limited to bounded, sanitized
         * metadata: HTTP status, Content-Type when present, and the body length.
         *
         * @param statusCode  HTTP status of the initial response
         * @param contentType Content-Type header of the initial response, possibly null
         * @param buffer      the buffer in which the non-JSON body was detected (the initial response body or a chunk)
         * @param chunkNum    0 if detected in the initial response body, else the chunk number
         * @param isHtml      whether the body was identified as HTML specifically (vs. some other non-JSON content)
         */
        private void throwForNonJsonBody(
            int statusCode,
            String contentType,
            ByteBuf buffer,
            long chunkNum,
            boolean isHtml
        )
        {
          final String where = chunkNum > 0 ? StringUtils.format(" (detected in chunk[%d])", chunkNum) : "";
          final String bodyInfo = StringUtils.format(
              "contentType[%s] bodyLength[%d]",
              contentType,
              buffer.readableBytes()
          );
          if (statusCode == 429 || statusCode == 503) {
            throw QueryCapacityExceededException.withErrorMessageAndResolvedHost(
                StringUtils.format(
                    "Query[%s] url[%s] failed with status[%s]%s %s",
                    query.getId(),
                    url,
                    statusCode,
                    where,
                    bodyInfo
                )
            );
          }
          throw new QueryInterruptedException(
              QueryException.UNKNOWN_EXCEPTION_ERROR_CODE,
              StringUtils.format(
                  "Query[%s] url[%s] returned %s response instead of JSON with status[%s]%s %s",
                  query.getId(),
                  url,
                  isHtml ? "HTML" : "non-JSON",
                  statusCode,
                  where,
                  bodyInfo
              ),
              QueryInterruptedException.class.getName(),
              host
          );
        }

        @Override
        public ClientResponse<InputStream> handleResponse(HttpResponse response, TrafficCop trafficCop)
        {
          trafficCopRef.set(trafficCop);
          checkQueryTimeout();
          // Netty 4: the initial HttpResponse carries no body, so the status and Content-Type are recorded
          // here and the body itself is inspected on the first HttpContent chunk. The goal is to detect a
          // non-JSON body (a 429/503 error page from a proxy, say) before it reaches the JSON parser, where it
          // would surface only as a JsonParseException on 0x3c ('<'). The shortcut is taken only when the body
          // is confirmed non-JSON: a 429/503 carrying Druid's own JSON error body (a genuine
          // QueryCapacityExceededException or SERVICE_UNAVAILABLE from a data server) falls through to the
          // normal JSON error path below, which preserves the server's structured error details.
          responseStatusCode = response.status().code();
          responseContentType = response.headers().get(HttpHeaders.Names.CONTENT_TYPE);

          log.debug("Initial response from url[%s] for queryId[%s]", url, query.getId());
          responseStartTimeNs = System.nanoTime();
          acquireResponseMetrics().reportNodeTimeToFirstByte(responseStartTimeNs - requestStartTimeNs).emit(emitter);

          final boolean continueReading;
          try {
            log.trace(
                "Got a response from [%s] for query ID[%s], subquery ID[%s]",
                url,
                query.getId(),
                query.getSubQueryId()
            );
            final String responseContext = response.headers().get(QueryResource.HEADER_RESPONSE_CONTEXT);
            context.addRemainingResponse(query.getMostSpecificId(), VAL_TO_REDUCE_REMAINING_RESPONSES);
            // context may be null in case of error or query timeout
            if (responseContext != null) {
              context.merge(ResponseContext.deserialize(responseContext, objectMapper));
            }
            // Netty 4: initial HttpResponse has no body content; body arrives via HttpContent chunks.
            // Seed the queue with an empty placeholder so SequenceInputStream's constructor (which
            // eagerly calls peekNextStream()) doesn't block before chunks arrive.
            queue.put(InputStreamHolder.fromChannelBuffer(Unpooled.EMPTY_BUFFER, 0L));
            continueReading = true;
          }
          catch (final IOException e) {
            log.error(e, "Error parsing response context from url [%s]", url);
            return ClientResponse.finished(
                new InputStream()
                {
                  @Override
                  public int read() throws IOException
                  {
                    throw e;
                  }
                }
            );
          }
          catch (InterruptedException e) {
            log.error(e, "Queue appending interrupted");
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
          return ClientResponse.finished(
              new SequenceInputStream(
                  new Enumeration<>()
                  {
                    @Override
                    public boolean hasMoreElements()
                    {
                      // If the consumer abandoned the stream (close() ran), report end-of-stream. After discard is set
                      // enqueue() drops every chunk, so a further read would otherwise block in dequeue() until the
                      // query timeout and then throw a misleading QueryTimeoutException.
                      if (discard.get()) {
                        return false;
                      }
                      if (failure.get() != null) {
                        throw failureException();
                      }
                      checkQueryTimeout();

                      // Done is always true until the last stream has be put in the queue.
                      // Then the stream should be spouting good InputStreams.
                      synchronized (done) {
                        return !done.get() || !queue.isEmpty();
                      }
                    }

                    @Override
                    public InputStream nextElement()
                    {
                      if (failure.get() != null) {
                        throw failureException();
                      }

                      try {
                        return dequeue();
                      }
                      catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                      }
                    }
                  }
              )
              {
                /**
                 * Closing this stream means the caller no longer needs the response. The default
                 * {@link SequenceInputStream#close()} would drain the entire remaining response off the wire first
                 * we want to avoid. Instead, abandon the response and force-close the connection
                 */
                @Override
                public void close()
                {
                  final TrafficCop trafficCop;
                  synchronized (done) {
                    if (done.get()) {
                      return;
                    }
                    // Stop buffering further chunks (see enqueue()) and drop anything already buffered so the
                    // underlying Netty ChannelBuffers can be released.
                    discard.set(true);
                    queue.clear();
                    trafficCop = trafficCopRef.get();
                  }
                  if (trafficCop == null) {
                    return;
                  }
                  trafficCop.abort();
                }
              },
              continueReading
          );
        }

        @Override
        public ClientResponse<InputStream> handleChunk(
            ClientResponse<InputStream> clientResponse,
            HttpContent chunk,
            long chunkNum
        )
        {
          checkQueryTimeout();

          final ByteBuf channelBuffer = chunk.content();
          final int bytes = channelBuffer.readableBytes();

          checkTotalBytesLimit(bytes);

          // Under Netty 4 the body never appears on the initial HttpResponse, so this is where the JSON-vs-not
          // prefix check happens, against the first chunk that carries any non-whitespace byte, and before those
          // bytes are enqueued for JSON parsing. Otherwise an HTML error page (from a load balancer or reverse
          // proxy, say) is enqueued blind and surfaces later as a confusing JsonParseException. The Content-Type
          // comes from the headers recorded in handleResponse. This is a no-op once the prefix has resolved.
          failIfNonJsonBody(responseContentType, channelBuffer, chunkNum);

          boolean continueReading = true;
          if (bytes > 0) {
            try {
              continueReading = enqueue(channelBuffer, chunkNum);
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            totalByteCount.addAndGet(bytes);
          }

          return ClientResponse.finished(clientResponse.getObj(), continueReading);
        }

        @Override
        public ClientResponse<InputStream> done(ClientResponse<InputStream> clientResponse)
        {
          long stopTimeNs = System.nanoTime();
          long nodeTimeNs = stopTimeNs - requestStartTimeNs;
          final long nodeTimeMs = TimeUnit.NANOSECONDS.toMillis(nodeTimeNs);
          log.debug(
              "Completed queryId[%s] request to url[%s] with %,d bytes returned in %,d millis [%,f b/s].",
              query.getId(),
              url,
              totalByteCount.get(),
              nodeTimeMs,
              // Floating math; division by zero will yield Inf, not exception
              totalByteCount.get() / (0.001 * nodeTimeMs)
          );
          emitNodeMetrics(nodeTimeNs);
          synchronized (done) {
            try {
              // An empty byte array is put at the end to give the SequenceInputStream.close() as something to close out
              // after done is set to true, regardless of the rest of the stream's state.
              queue.put(InputStreamHolder.fromChannelBuffer(Unpooled.EMPTY_BUFFER, Long.MAX_VALUE));
            }
            catch (InterruptedException e) {
              log.error(e, "Unable to put finalizing input stream into Sequence queue for url [%s]", url);
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            finally {
              done.set(true);
            }
          }
          return ClientResponse.finished(clientResponse.getObj());
        }

        @Override
        public void exceptionCaught(final ClientResponse<InputStream> clientResponse, final Throwable e)
        {
          // Fall back to Throwable.toString() when the exception carries no message, so a timeout
          // (Netty's ReadTimeoutException is a stackless, messageless singleton) does not render as
          // "exception msg [null]" but as "exception msg [io.netty.handler.timeout.ReadTimeoutException]"
          // instead.
          final String exceptionDetail = e.getMessage() != null ? e.getMessage() : e.toString();
          String msg = StringUtils.format(
              "Query[%s] url[%s] failed with exception msg [%s]",
              query.getId(),
              url,
              exceptionDetail
          );
          setupResponseReadFailure(msg, e);
        }

        private void setupResponseReadFailure(String msg, Throwable th)
        {
          emitNodeMetrics(System.nanoTime() - requestStartTimeNs);
          // Publish message and cause together as one Failure so a reader that observes a non-null failure()
          // always sees the cause that goes with it; see the field comment on failure.
          failure.set(new Failure(msg, th));
          queue.clear();
          queue.offer(
              InputStreamHolder.fromStream(
                  new InputStream()
                  {
                    @Override
                    public int read() throws IOException
                    {
                      if (th instanceof QueryException) {
                        // Rethrow a typed query failure (e.g. QueryCapacityExceededException) as itself rather than
                        // burying it as the cause of a generic IOException, where it would otherwise only be
                        // recoverable by callers that specifically unwrap getCause(). Deliberately limited to
                        // QueryException: a transport-level RuntimeException such as Netty's ChannelException from a
                        // mid-stream disconnect must keep going out as an IOException, because that is the form
                        // JsonParserIterator normalizes into a QueryInterruptedException carrying this client's host.
                        throw (QueryException) th;
                      } else if (th != null) {
                        throw new IOException(msg, th);
                      } else {
                        throw new IOException(msg);
                      }
                    }
                  },
                  -1,
                  0
              )
          );
        }

        /**
         * Returns the exception to surface for a failure recorded by {@link #setupResponseReadFailure}. Rethrows the
         * original cause directly when it is already a {@link QueryException} (e.g. the
         * {@link QueryCapacityExceededException} thrown from {@link #handleChunk} on a later chunk) so its concrete
         * type survives to the caller instead of being flattened into a plain {@link RE}. Every other cause keeps
         * the pre-existing {@link RE} form, so a transport failure such as a mid-stream disconnect still reaches
         * the caller with this method's message rather than as a raw Netty exception. Only called after
         * confirming {@link #failure} is non-null, so the message and cause it reads are always the ones from the
         * same {@link Failure} publication.
         */
        private RuntimeException failureException()
        {
          final Failure f = failure.get();
          if (f.cause instanceof QueryException) {
            return (QueryException) f.cause;
          }
          return new RE(f.message);
        }

        // Emit exactly once, regardless of whether we reach this via done() or setupResponseReadFailure().
        private void emitNodeMetrics(long nodeTimeNs)
        {
          if (!nodeMetricsEmitted.compareAndSet(false, true)) {
            return;
          }
          QueryMetrics<? super Query<T>> responseMetrics = acquireResponseMetrics();
          responseMetrics.reportNodeTime(nodeTimeNs);
          responseMetrics.reportNodeBytes(totalByteCount.get());
          if (usingBackpressure) {
            responseMetrics.reportBackPressureTime(channelSuspendedTime.get());
          }
          responseMetrics.emit(emitter);
        }

        // Returns remaining timeout or throws exception if timeout already elapsed.
        private long checkQueryTimeout()
        {
          long timeLeft = timeoutAt - System.currentTimeMillis();
          if (timeLeft <= 0) {
            String msg = StringUtils.format("Query[%s] url[%s] timed out.", query.getId(), url);
            setupResponseReadFailure(msg, null);
            throw new QueryTimeoutException(msg);
          } else {
            return timeLeft;
          }
        }

        private void checkTotalBytesLimit(long bytes)
        {
          final long currentTotalBytesGathered = totalBytesGathered.addAndGet(bytes);
          if (currentTotalBytesGathered > maxScatterGatherBytes) {
            String msg = StringUtils.format(
                "Query[%s] url[%s] total bytes gathered[%,d] exceeds maxScatterGatherBytes[%,d]",
                query.getId(),
                url,
                currentTotalBytesGathered,
                maxScatterGatherBytes
            );
            setupResponseReadFailure(msg, null);
            throw new ResourceLimitExceededException(msg);
          }
        }
      };

      long timeLeft = timeoutAt - System.currentTimeMillis();

      if (timeLeft <= 0) {
        throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query[%s] url[%s] timed out.", query.getId(), url));
      }

      // increment is moved up so that if future initialization is queued by some other process,
      // we can increment the count earlier so that we can route the request to a different server
      openConnections.getAndIncrement();
      try {
        future = httpClient.go(
            new Request(
                HttpMethod.POST,
                new URL(url)
            ).setContent(objectMapper.writeValueAsBytes(Queries.withTimeout(query, timeLeft)))
             .setHeader(
                 HttpHeaders.Names.CONTENT_TYPE,
                 isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON
             ),
            responseHandler,
            Duration.millis(timeLeft)
        );
      }
      catch (Exception e) {
        openConnections.getAndDecrement();
        throw e;
      }

      queryWatcher.registerQueryFuture(query, future);
      Futures.addCallback(
          future,
          new FutureCallback<>()
          {
            @Override
            public void onSuccess(InputStream result)
            {
              openConnections.getAndDecrement();
            }

            @Override
            public void onFailure(Throwable t)
            {
              openConnections.getAndDecrement();
              if (future.isCancelled()) {
                cancelQuery(query, cancelUrl);
              }
            }
          },
          // The callback is non-blocking and quick, so it's OK to schedule it using directExecutor()
          Execs.directExecutor()
      );
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    Sequence<T> retVal = new BaseSequence<>(
        new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
        {
          @Override
          public JsonParserIterator<T> make()
          {
            return new JsonParserIterator<>(
                queryResultType,
                future,
                url,
                query,
                host,
                toolChest.decorateObjectMapper(objectMapper, query)
            );
          }

          @Override
          public void cleanup(JsonParserIterator<T> iterFromMake)
          {
            CloseableUtils.closeAndWrapExceptions(iterFromMake);
          }
        }
    );

    // bySegment queries are de-serialized after caching results in order to
    // avoid the cost of de-serializing and then re-serializing again when adding to cache
    if (!isBySegment) {
      retVal = Sequences.map(
          retVal,
          toolChest.makePreComputeManipulatorFn(
              query,
              MetricManipulatorFns.deserializing()
          )
      );
    }

    return retVal;
  }

  private void cancelQuery(Query<T> query, String cancelUrl)
  {
    Runnable cancelRunnable = () -> {
      try {
        Future<StatusResponseHolder> responseFuture = httpClient.go(
            new Request(HttpMethod.DELETE, new URL(cancelUrl))
            .setContent(objectMapper.writeValueAsBytes(query))
            .setHeader(HttpHeaders.Names.CONTENT_TYPE, isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON),
            StatusResponseHandler.getInstance(),
            Duration.standardSeconds(1));

        Runnable checkRunnable = () -> {
          try {
            if (!responseFuture.isDone()) {
              log.error("Error cancelling query[%s]", query);
            }
            StatusResponseHolder response = responseFuture.get(30, TimeUnit.SECONDS);
            if (response.getStatus().code() >= 500) {
              log.error("Error cancelling query[%s]: queriable node returned status[%d] [%s].",
                  query,
                  response.getStatus().code(),
                  response.getStatus().reasonPhrase());
            }
          }
          catch (ExecutionException | InterruptedException e) {
            log.error(e, "Error cancelling query[%s]", query);
          }
          catch (TimeoutException e) {
            log.error(e, "Timed out cancelling query[%s]", query);
          }
        };
        queryCancellationExecutor.schedule(checkRunnable, 5, TimeUnit.SECONDS);
      }
      catch (IOException e) {
        log.error(e, "Error cancelling query[%s]", query);
      }
    };
    queryCancellationExecutor.submit(cancelRunnable);
  }

  @Override
  public String toString()
  {
    return "DirectDruidClient{" +
           "host='" + host + '\'' +
           ", isSmile=" + isSmile +
           '}';
  }
}
