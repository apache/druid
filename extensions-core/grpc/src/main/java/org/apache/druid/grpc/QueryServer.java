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

package org.apache.druid.grpc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import io.grpc.Attributes;
import io.grpc.BindableService;
import io.grpc.Grpc;
import io.grpc.KnownLength;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.Status;
import io.grpc.inprocess.InProcessSocketAddress;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.Result;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryManager;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthorizerMapper;
import org.slf4j.MDC;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class QueryServer
{
  private static final Logger LOG = new Logger(QueryServer.class);
  private static final String FULL_SERVICE_NAME = "org.apache.druid";
  private static final String FULL_METHOD_NAME = MethodDescriptor.generateFullMethodName(
      FULL_SERVICE_NAME,
      "query-json"
  );
  @VisibleForTesting
  final QueryImpl queryImpl = new QueryImpl();
  private final QueryLifecycleFactory queryLifecycleFactory;
  private final ObjectMapper mapper;
  private final QueryManager queryManager;
  private final QuerySegmentWalker texasRanger;
  private final GrpcConfig grpcConfig;
  private final AuthConfig authConfig;
  private final AuthorizerMapper authorizerMapper;
  private final GenericQueryMetricsFactory queryMetricsFactory;
  @GuardedBy("this")
  private Server server = null;

  @Inject
  public QueryServer(
      QueryLifecycleFactory queryLifecycleFactory,
      @Json ObjectMapper mapper,
      QueryManager queryManager,
      QuerySegmentWalker texasRanger,
      GrpcConfig grpcConfig,
      AuthConfig authConfig,
      AuthorizerMapper authorizerMapper,
      GenericQueryMetricsFactory queryMetricsFactory
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.mapper = mapper;
    this.queryManager = queryManager;
    this.texasRanger = texasRanger;
    this.grpcConfig = grpcConfig;
    this.authConfig = authConfig;
    this.authorizerMapper = authorizerMapper;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  public static String getRemoteAddress(Attributes attributes)
  {
    if (attributes == null) {
      return "unknown";
    }
    final SocketAddress remoteAddrSocket = attributes.get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
    if (remoteAddrSocket instanceof InetSocketAddress) {
      final InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddrSocket;
      final InetAddress inetAddress = inetSocketAddress.getAddress();
      final int port = inetSocketAddress.getPort();
      return HostAndPort.fromParts(inetAddress.getHostAddress(), port).toString();
    } else if (remoteAddrSocket instanceof InProcessSocketAddress) {
      final InProcessSocketAddress inProcessSocketAddress = (InProcessSocketAddress) remoteAddrSocket;
      return StringUtils.format("local://%s", inProcessSocketAddress.getName());
    } else {
      return "unknown";
    }
  }

  @LifecycleStart
  public synchronized void start()
  {
    final ClassLoader oldLoader = Thread.currentThread().getContextClassLoader();
    try {
      // Set the thread class loader for initializing ServerProvider class (via ServerBuilder).
      Thread.currentThread().setContextClassLoader(QueryServer.class.getClassLoader());
      final Server oldServer = this.server;
      if (oldServer != null) {
        throw new ISE("Expected no server set, instead found %s", oldServer);
      }
      final Server server = ServerBuilder
          .forPort(grpcConfig.getPort())
          .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
          .addService(queryImpl)
          .build();
      try {
        server.start();
        this.server = server;
        LOG.info("Started gRPC server on port %d", grpcConfig.getPort());
      }
      catch (IOException e) {
        server.shutdownNow();
        this.server = null;
        throw new RE(e, "Failed to start gRPC server on port %d", grpcConfig.getPort());
      }
    }
    finally {
      Thread.currentThread().setContextClassLoader(oldLoader);
    }
  }

  @LifecycleStop
  public synchronized void stop()
  {
    final Server server = this.server;
    this.server = null;
    if (server != null) {
      server.shutdown();
      try {
        if (!server.awaitTermination(grpcConfig.getShutdownTimeoutMs(), TimeUnit.MILLISECONDS)) {
          throw new TimeoutException(
              StringUtils.format("Timed out waiting for termination. Waited %d ms", grpcConfig.getShutdownTimeoutMs())
          );
        }
        LOG.info("Shutdown gRPC server.");
      }
      catch (InterruptedException | TimeoutException e) {
        LOG.warn(e, "Problem during shutdown of gRPC server, potentially unclean shutdown.");
      }
    }
  }

  static final class ByteArrayInputStream extends java.io.ByteArrayInputStream implements KnownLength
  {
    ByteArrayInputStream(byte[] buf)
    {
      super(buf);
    }
  }

  @VisibleForTesting
  class QueryImpl implements BindableService
  {
    @VisibleForTesting
    final MethodDescriptor.Marshaller<Supplier<Query>> QUERY_MARSHALL = new MethodDescriptor.Marshaller<Supplier<Query>>()
    {
      @Override
      public InputStream stream(Supplier<Query> value)
      {
        try {
          return new ByteArrayInputStream(mapper.writeValueAsBytes(value.get()));
        }
        catch (IOException ioe) {
          throw new RE(ioe, "Error writing query value %s", value);
        }
      }

      @Override
      public Supplier<Query> parse(InputStream stream)
      {
        final Query query;
        try {
          query = mapper.readValue(stream, Query.class);
        }
        catch (IOException e) {
          final RE re = new RE(e, "Error parsing gRPC query request");
          return () -> {
            throw re;
          };
        }
        return () -> query;
      }
    };
    @VisibleForTesting
    final MethodDescriptor.Marshaller<Result> RESULT_MARSHALL = new MethodDescriptor.Marshaller<Result>()
    {
      @Override
      public InputStream stream(Result value)
      {
        try {
          return new ByteArrayInputStream(mapper.writeValueAsBytes(value));
        }
        catch (IOException ioe) {
          throw new RE(ioe, "Error writing return value %s", value);
        }
      }

      @Override
      public Result parse(InputStream stream)
      {
        try {
          return mapper.readValue(stream, Result.class);
        }
        catch (IOException ioe) {
          throw new RE(ioe, "Error parsing query result from stream");
        }
      }
    };
    @VisibleForTesting
    final MethodDescriptor<Supplier<Query>, Result> METHOD_DESCRIPTOR = MethodDescriptor
        .<Supplier<Query>, Result>newBuilder()
        .setFullMethodName(FULL_METHOD_NAME)
        .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
        .setRequestMarshaller(QUERY_MARSHALL)
        .setResponseMarshaller(RESULT_MARSHALL)
        // These two are experimental, see https://github.com/grpc/grpc-java/issues/1775
        // If segment version changes, might not be the same result
        .setIdempotent(false)
        // setSafe() means setSideEffectFree() in this API.
        // Query execution is not side effect free: at the very least, it affects caches
        .setSafe(false)
        .build();
    @VisibleForTesting
    final ServiceDescriptor SERVICE_DESCRIPTOR = ServiceDescriptor
        .newBuilder(FULL_SERVICE_NAME)
        .addMethod(METHOD_DESCRIPTOR)
        .build();
    @VisibleForTesting
    final Metadata.Key<String> QUERY_ID_KEY = Metadata.Key.of(
        "X-Druid-Query-Id",
        new Metadata.AsciiMarshaller<String>()
        {
          @Override
          public String toAsciiString(String value)
          {
            return value;
          }

          @Override
          public String parseAsciiString(String serialized)
          {
            return serialized;
          }
        }
    );
    @VisibleForTesting
    final Metadata.Key<Map<String, Object>> QUERY_CONTEXT = Metadata.Key.of(
        "X-Druid-Query-Context-bin",
        new Metadata.BinaryMarshaller<Map<String, Object>>()
        {
          @Override
          public byte[] toBytes(Map<String, Object> value)
          {
            try {
              return mapper.writeValueAsBytes(value);
            }
            catch (JsonProcessingException e) {
              throw new RE(e, "Error parsing [%s]", value);
            }
          }

          @Override
          public Map<String, Object> parseBytes(byte[] serialized)
          {
            try {
              return mapper.readValue(serialized, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT);
            }
            catch (IOException e) {
              throw new RE(e, "Error parsing [%s]", StringUtils.fromUtf8(serialized));
            }
          }
        }
    );
    @VisibleForTesting
    final ServerCallHandler<Supplier<Query>, Result> SERVICE_CALL_HANDLER = new ServerCallHandler<Supplier<Query>, Result>()
    {
      @Override
      public ServerCall.Listener<Supplier<Query>> startCall(
          final ServerCall<Supplier<Query>, Result> call,
          final Metadata headers
      )
      {
        final AtomicBoolean queryReceived = new AtomicBoolean(false);
        final String id = queryIdOrNew(headers);
        call.request(1);
        final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();
        final String remoteAddr = getRemoteAddress(call.getAttributes());
        return new ServerCall.Listener<Supplier<Query>>()
        {
          public void onMessage(Supplier<Query> querySupplier)
          {
            final Map priorMap = MDC.getCopyOfContextMap();
            final String currThreadName = Thread.currentThread().getName();
            try {
              MDC.put("queryId", id);
              MDC.put("remoteAddr", remoteAddr);
              final Metadata metadata = new Metadata();
              metadata.put(QUERY_ID_KEY, id);
              Query<?> query;
              try {
                query = querySupplier.get();
              }
              catch (RE re) {
                final Status status = Status.fromCode(Status.Code.INVALID_ARGUMENT);
                safeClose(status.augmentDescription(re.getCause().getMessage()), metadata);
                return;
              }
              if (!queryReceived.compareAndSet(false, true)) {
                safeClose(
                    Status.INTERNAL.augmentDescription("Query already called once"),
                    metadata
                );
                return;
              }
              final String passedId = query.getId();
              if (passedId != null && !id.equals(passedId)) {
                safeClose(Status.INVALID_ARGUMENT.augmentDescription("query id does not match header"), metadata);
                return;
              }
              queryLifecycle.initialize(query.withId(id));
              query = queryLifecycle.getQuery();
              Thread.currentThread()
                    .setName(StringUtils.format(
                        "%s[%s_%s_%s]",
                        currThreadName,
                        query.getType(),
                        query.getDataSource().getNames(),
                        id
                    ));
              query.getContext().forEach((k, v) -> MDC.put(StringUtils.format("queryContext:%s", k), v.toString()));

              // TODO: some gRPC idiomatic stuff here
              final Access authResult = queryLifecycle.authorize(new AuthenticationResult(
                  "ignored_identity",
                  "ignored_name",
                  "ignored_authenticatedBy",
                  null
              ));

              if (!authResult.isAllowed()) {
                LOG.warn("Unauthorized");
                safeClose(Status.PERMISSION_DENIED.augmentDescription(authResult.getMessage()), metadata);
                return;
              }
              final Metadata trailers = new Metadata();
              // Sometimes the call can be cancelled by the time we get here
              try {
                call.sendHeaders(metadata);
              }
              catch (IllegalStateException ise) {
                LOG.info(ise, "Sending headers failed");
                safeClose(Status.INTERNAL.augmentDescription("sending headers failed"), trailers);
                return;
              }

              // OFF TO THE RACES!!!
              final QueryLifecycle.QueryResponse queryResponse = queryLifecycle.execute();
              @SuppressWarnings("unchecked")
              final Sequence<Result> results = queryResponse.getResults();
              trailers.put(QUERY_CONTEXT, queryResponse.getResponseContext());
              Yielder<Result> yielder = null;
              try {
                yielder = Yielders.each(results);
                while (!yielder.isDone() && !call.isCancelled()) {
                  try {
                    final Result result = yielder.get();
                    // On some strange close race condition this might throw IllegalStateException in which case it will
                    // be caught by the RuntimeException catch
                    call.sendMessage(result);
                    yielder = yielder.next(result);
                  }
                  catch (QueryInterruptedException ie) {
                    if (CancellationException.class.getName().equals(ie.getErrorClass())) {
                      LOG.debug(ie, "Cancelled");
                      safeClose(Status.CANCELLED, trailers);
                    } else {
                      LOG.info(ie, "Unknown QueryInterruptedException");
                      safeClose(Status.INTERNAL.augmentDescription(ie.toString()), trailers);
                      queryLifecycle.emitLogsAndMetrics(ie, remoteAddr, -1);
                    }
                    return;
                  }
                }
                // Golden path
                if (call.isCancelled()) {
                  LOG.debug("Cancelled");
                  safeClose(Status.CANCELLED, trailers);
                } else {
                  // Would it ever make sense to have one channel handle lots of queries?
                  safeClose(Status.OK, trailers);
                  queryLifecycle.emitLogsAndMetrics(null, remoteAddr, -1);
                }
              }
              catch (RuntimeException e) {
                // Error path, usually problems in the yielders, or if a `call.sendMessage` happens after a cancel
                safeClose(Status.INTERNAL.augmentDescription(e.toString()), trailers);
                LOG.info(e, "Unknown error");
                queryLifecycle.emitLogsAndMetrics(e, remoteAddr, -1);
              }
              finally {
                if (yielder != null) {
                  try {
                    yielder.close();
                  }
                  catch (Exception e) {
                    LOG.debug(e, "Failed to close yielder");
                  }
                }
              }
            }
            finally {
              Thread.currentThread().setName(currThreadName);
              MDC.setContextMap(priorMap);
            }
          }

          private void safeClose(Status status, Metadata trailers)
          {
            try {
              call.close(status, trailers);
            }
            catch (IllegalStateException ise) {
              LOG.debug(ise, "Failed to close query [%s]", id);
            }
          }

          public void onCancel()
          {
            // Small race condition here where the query has been received but not enqueued when the cancel comes in.
            // To counter this, onCancel() is also called if the query result loop never ran in the first place
            if (queryManager != null && !queryManager.cancelQuery(id)) {
              LOG.debug("Unable to cancel query [%s]", id);
            }
          }
        };
      }
    };

    String queryIdOrNew(Metadata headers)
    {
      if (headers.containsKey(QUERY_ID_KEY)) {
        return headers.get(QUERY_ID_KEY);
      } else {
        return UUID.randomUUID().toString();
      }
    }


    @Override
    public ServerServiceDefinition bindService()
    {
      return ServerServiceDefinition
          .builder(SERVICE_DESCRIPTOR)
          .addMethod(METHOD_DESCRIPTOR, SERVICE_CALL_HANDLER)
          .build();
    }
  }
}
