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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.calcite.avatica.remote.ProtobufTranslation;
import org.apache.calcite.avatica.remote.ProtobufTranslationImpl;
import org.apache.calcite.avatica.remote.Service;
import org.apache.commons.io.IOUtils;
import org.apache.druid.client.selector.Server;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.jetty.HttpException;
import org.apache.druid.server.initialization.jetty.StandardResponseHeaderFilterHolder;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.router.QueryHostFinder;
import org.apache.druid.server.router.Router;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authenticator;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.sql.http.SqlResource;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.client.api.Result;
import org.eclipse.jetty.client.util.BytesContentProvider;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.proxy.AsyncProxyServlet;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class does async query processing and should be merged with QueryResource at some point
 */
public class AsyncQueryForwardingServlet extends AsyncProxyServlet implements QueryCountStatsProvider {
    private static final EmittingLogger LOG = new EmittingLogger(AsyncQueryForwardingServlet.class);
    @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
    private static final String APPLICATION_SMILE = "application/smile";

    private static final String AVATICA_CONNECTION_ID = "connectionId";
    private static final String AVATICA_STATEMENT_HANDLE = "statementHandle";

    private static final String HOST_ATTRIBUTE = "org.apache.druid.proxy.to.host";
    private static final String SCHEME_ATTRIBUTE = "org.apache.druid.proxy.to.host.scheme";
    private static final String QUERY_ATTRIBUTE = "org.apache.druid.proxy.query";
    private static final String AVATICA_QUERY_ATTRIBUTE = "org.apache.druid.proxy.avaticaQuery";
    private static final String SQL_QUERY_ATTRIBUTE = "org.apache.druid.proxy.sqlQuery";
    private static final String OBJECTMAPPER_ATTRIBUTE = "org.apache.druid.proxy.objectMapper";

    private static final String PROPERTY_SQL_ENABLE = "druid.router.sql.enable";
    private static final String PROPERTY_SQL_ENABLE_DEFAULT = "false";

    private static final long CANCELLATION_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);
    private static final long CANCELLATION_TIMEOUT_TEMPORARY_MILLIS = TimeUnit.SECONDS.toMillis(30);

    private final AtomicLong successfulQueryCount = new AtomicLong();
    private final AtomicLong failedQueryCount = new AtomicLong();
    private final AtomicLong interruptedQueryCount = new AtomicLong();

    @VisibleForTesting
    void handleException(HttpServletResponse response, ObjectMapper objectMapper, Exception exception)
            throws IOException {
        QueryInterruptedException exceptionToReport = QueryInterruptedException.wrapIfNeeded(exception);
        LOG.warn(exceptionToReport, "Unexpected exception occurs");
        if (!response.isCommitted()) {
            response.resetBuffer();
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            objectMapper.writeValue(
                    response.getOutputStream(),
                    serverConfig.getErrorResponseTransformStrategy().transformIfNeeded(exceptionToReport)
            );
        }
        response.flushBuffer();
    }

    private final QueryToolChestWarehouse warehouse;
    private final ObjectMapper jsonMapper;
    private final ObjectMapper smileMapper;
    private final QueryHostFinder hostFinder;
    private final Provider<HttpClient> httpClientProvider;
    private final DruidHttpClientConfig httpClientConfig;
    private final ServiceEmitter emitter;
    private final RequestLogger requestLogger;
    private final GenericQueryMetricsFactory queryMetricsFactory;
    private final AuthenticatorMapper authenticatorMapper;
    private final ProtobufTranslation protobufTranslation;
    private final ServerConfig serverConfig;
    private QueuedThreadPool primaryThreadPool;
    private QueuedThreadPool cancellationThreadPool;

    private final boolean routeSqlByStrategy;

    private HttpClient broadcastClient;
    private ScheduledExecutorService monitoringExecutor;

    @Inject
    public AsyncQueryForwardingServlet(
            QueryToolChestWarehouse warehouse,
            @Json ObjectMapper jsonMapper,
            @Smile ObjectMapper smileMapper,
            QueryHostFinder hostFinder,
            @Router Provider<HttpClient> httpClientProvider,
            @Router DruidHttpClientConfig httpClientConfig,
            ServiceEmitter emitter,
            RequestLogger requestLogger,
            GenericQueryMetricsFactory queryMetricsFactory,
            AuthenticatorMapper authenticatorMapper,
            Properties properties,
            final ServerConfig serverConfig
    ) {
        this.warehouse = warehouse;
        this.jsonMapper = jsonMapper;
        this.smileMapper = smileMapper;
        this.hostFinder = hostFinder;
        this.httpClientProvider = httpClientProvider;
        this.httpClientConfig = httpClientConfig;
        this.emitter = emitter;
        this.requestLogger = requestLogger;
        this.queryMetricsFactory = queryMetricsFactory;
        this.authenticatorMapper = authenticatorMapper;
        this.protobufTranslation = new ProtobufTranslationImpl();
        this.routeSqlByStrategy = Boolean.parseBoolean(
                properties.getProperty(PROPERTY_SQL_ENABLE, PROPERTY_SQL_ENABLE_DEFAULT)
        );
        this.serverConfig = serverConfig;
        this.monitoringExecutor = ScheduledExecutors.fixed(1, "AsyncQueryForwardingServlet-Emittor");
    }

    public QueuedThreadPool getThreadPool() {
        return this.primaryThreadPool;
    }

    public QueuedThreadPool getCancellationThreadPool() {
        return this.cancellationThreadPool;
    }

    @Override
    public void init() throws ServletException {
        super.init();
        this.primaryThreadPool = (QueuedThreadPool) super.getHttpClient().getExecutor();

        // Note that httpClientProvider is setup to return same HttpClient instance on each get() so
        // it is same http client as that is used by parent ProxyServlet.
        broadcastClient = newHttpClient();
        this.cancellationThreadPool = (QueuedThreadPool) broadcastClient.getExecutor();
        try {
            broadcastClient.start();
            this.monitoringExecutor.scheduleAtFixedRate(() -> {
                LOG.info("router/jetty/threadPool/total: %d", primaryThreadPool.getThreads());
                LOG.info("router/jetty/threadPool/idle: %d", primaryThreadPool.getIdleThreads());
                LOG.info("router/jetty/threadPool/busy: %d", primaryThreadPool.getBusyThreads());
                LOG.info("router/jetty/threadPool/queueSize: %d", primaryThreadPool.getQueueSize());
                LOG.info("router/jetty/threadPool/maxThreads: %d", primaryThreadPool.getMaxThreads());
                LOG.info("cancellationClient/jetty/threadPool/total: %d", cancellationThreadPool.getThreads());
                LOG.info("cancellationClient/jetty/threadPool/idle: %d", cancellationThreadPool.getIdleThreads());
                LOG.info("cancellationClient/jetty/threadPool/busy: %d", cancellationThreadPool.getBusyThreads());
                LOG.info("cancellationClient/jetty/threadPool/queueSize: %d", cancellationThreadPool.getQueueSize());
                LOG.info("cancellationClient/jetty/threadPool/maxThreads: %d", cancellationThreadPool.getMaxThreads());
            }, 1, 1, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new ServletException(e);
        }

    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            broadcastClient.stop();
            if (monitoringExecutor != null) {
                monitoringExecutor.shutdownNow();
            }
        } catch (Exception e) {
            LOG.warn(e, "Error stopping servlet");
        }
    }

    @Override
    protected void service(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(request.getContentType())
                || APPLICATION_SMILE.equals(request.getContentType());
        final ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
        request.setAttribute(OBJECTMAPPER_ATTRIBUTE, objectMapper);

        final String requestURI = request.getRequestURI();
        final String method = request.getMethod();
        final Server targetServer;

        // The Router does not have the ability to look inside SQL queries and route them intelligently, so just treat
        // them as a generic request.
        final boolean isNativeQueryEndpoint = requestURI.startsWith("/druid/v2") && !requestURI.startsWith("/druid/v2/sql");
        final boolean isSqlQueryEndpoint = requestURI.startsWith("/druid/v2/sql");

        final boolean isAvaticaJson = requestURI.startsWith("/druid/v2/sql/avatica");
        final boolean isAvaticaPb = requestURI.startsWith("/druid/v2/sql/avatica-protobuf");

    if (isAvaticaPb) {
      byte[] requestBytes = IOUtils.toByteArray(request.getInputStream());
      Service.Request protobufRequest = this.protobufTranslation.parseRequest(requestBytes);
      String connectionId = getAvaticaProtobufConnectionId(protobufRequest);
      targetServer = hostFinder.findServerAvatica(connectionId);
      request.setAttribute(AVATICA_QUERY_ATTRIBUTE, requestBytes);
      LOG.debug("Forwarding protobuf JDBC connection [%s] to broker [%s]", connectionId, targetServer);
    } else if (isAvaticaJson) {
      Map<String, Object> requestMap = objectMapper.readValue(
          request.getInputStream(),
          JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
      );
      String connectionId = getAvaticaConnectionId(requestMap);
      targetServer = hostFinder.findServerAvatica(connectionId);
      byte[] requestBytes = objectMapper.writeValueAsBytes(requestMap);
      request.setAttribute(AVATICA_QUERY_ATTRIBUTE, requestBytes);
      LOG.debug("Forwarding JDBC connection [%s] to broker [%s]", connectionId, targetServer.getHost());
    } else if (HttpMethod.DELETE.is(method)) {
      // query cancellation request
      targetServer = hostFinder.pickDefaultServer();
      broadcastQueryCancelRequest(request, targetServer);
      LOG.debug("Broadcasting cancellation request to all brokers");
    } else if (isNativeQueryEndpoint && HttpMethod.POST.is(method)) {
      // query request
      try {
        Query inputQuery = objectMapper.readValue(request.getInputStream(), Query.class);
        if (inputQuery != null) {
          targetServer = hostFinder.pickServer(inputQuery);
          if (inputQuery.getId() == null) {
            inputQuery = inputQuery.withId(UUID.randomUUID().toString());
          }
          LOG.debug("Forwarding JSON query [%s] to broker [%s]", inputQuery.getId(), targetServer.getHost());
        } else {
          targetServer = hostFinder.pickDefaultServer();
          LOG.debug("Forwarding JSON request to broker [%s]", targetServer.getHost());
        }
        request.setAttribute(QUERY_ATTRIBUTE, inputQuery);
      }
      catch (IOException e) {
        handleQueryParseException(request, response, objectMapper, e, true);
        return;
      }
      catch (Exception e) {
        handleException(response, objectMapper, e);
        return;
      }
    } else if (isSqlQueryEndpoint && HttpMethod.POST.is(method)) {
      try {
        SqlQuery inputSqlQuery = SqlQuery.from(request, objectMapper);
        inputSqlQuery = buildSqlQueryWithId(inputSqlQuery);
        request.setAttribute(SQL_QUERY_ATTRIBUTE, inputSqlQuery);
        if (routeSqlByStrategy) {
          targetServer = hostFinder.findServerSql(inputSqlQuery);
        } else {
          targetServer = hostFinder.pickDefaultServer();
        }
        LOG.debug("Forwarding SQL query to broker [%s]", targetServer.getHost());
      }
      catch (HttpException e) {
        handleQueryParseException(request, response, e.getStatusCode().getStatusCode(), objectMapper, e, false);
        return;
      }
      catch (Exception e) {
        handleException(response, objectMapper, e);
        return;
      }
    } else {
      targetServer = hostFinder.pickDefaultServer();
      LOG.debug("Forwarding query to broker [%s]", targetServer.getHost());
    }

        request.setAttribute(HOST_ATTRIBUTE, targetServer.getHost());
        request.setAttribute(SCHEME_ATTRIBUTE, targetServer.getScheme());

        doService(request, response);
    }

    /**
     * Rebuilds the {@link SqlQuery} object with sqlQueryId and queryId context parameters if not present
     *
     * @param sqlQuery the original SqlQuery
     * @return an updated sqlQuery object with sqlQueryId and queryId context parameters
     */
    private SqlQuery buildSqlQueryWithId(SqlQuery sqlQuery) {
        Map<String, Object> context = new HashMap<>(sqlQuery.getContext());
        String sqlQueryId = (String) context.getOrDefault(BaseQuery.SQL_QUERY_ID, UUID.randomUUID().toString());
        // set queryId to sqlQueryId if not overridden
        String queryId = (String) context.getOrDefault(BaseQuery.QUERY_ID, sqlQueryId);
        context.put(BaseQuery.SQL_QUERY_ID, sqlQueryId);
        context.put(BaseQuery.QUERY_ID, queryId);
        return sqlQuery.withOverridenContext(context);
    }

    /**
     * Issues async query cancellation requests to all Brokers (except the given
     * targetServer). Query cancellation on the targetServer is handled by the
     * proxy servlet.
     */
    private void broadcastQueryCancelRequest(HttpServletRequest request, Server targetServer) {
        String sqlQueryId;
        String[] uriParts = request.getRequestURI().split("/");
        if (uriParts.length > 0) {
            sqlQueryId = uriParts[uriParts.length - 1];
        } else {
            sqlQueryId = null;
        }
        for (final Server server : hostFinder.getAllServers()) {
            LOG.debug("Broadcasting cancellation request to server [{%s} ({%s})]", server.getHost(), server.getScheme());
            if (server.getHost().equals(targetServer.getHost())) {
                // target broker.
                continue;
            }

            // issue async requests
            Response.CompleteListener completeListener = result -> {
                if (result.isFailed()) {
                    LOG.noStackTrace().info(
                            result.getFailure(),
                            "[Query Id: %s] Failed to forward cancellation request to [%s]",
                            sqlQueryId,
                            server.getHost()
                    );
                }
            };

            Request broadcastReq = broadcastClient
                    .newRequest(rewriteURI(request, server.getScheme(), server.getHost()))
                    .method(HttpMethod.DELETE)
                    .timeout(CANCELLATION_TIMEOUT_TEMPORARY_MILLIS, TimeUnit.MILLISECONDS);

            copyRequestHeaders(request, broadcastReq);
            broadcastReq.send(completeListener);
        }

        interruptedQueryCount.incrementAndGet();
    }

  @VisibleForTesting
  void handleQueryParseException(
      HttpServletRequest request,
      HttpServletResponse response,
      ObjectMapper objectMapper,
      Throwable parseException,
      boolean isNativeQuery
  ) throws IOException
  {
    handleQueryParseException(request, response, HttpServletResponse.SC_BAD_REQUEST, objectMapper, parseException, isNativeQuery);
  }

  private void handleQueryParseException(
      HttpServletRequest request,
      HttpServletResponse response,
      int httpStatusCode,
      ObjectMapper objectMapper,
      Throwable parseException,

      boolean isNativeQuery
  ) throws IOException
  {
    QueryInterruptedException exceptionToReport = QueryInterruptedException.wrapIfNeeded(parseException);
    LOG.warn(exceptionToReport, "Exception parsing query");

        // Log the error message
        final String errorMessage = exceptionToReport.getMessage() == null
                ? "no error message" : exceptionToReport.getMessage();

        AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(request);

        if (isNativeQuery) {
            requestLogger.logNativeQuery(
                    RequestLogLine.forNative(
                            null,
                            DateTimes.nowUtc(),
                            request.getRemoteAddr(),
                            new QueryStats(ImmutableMap.of("success", false, "exception", errorMessage, "identity", authenticationResult.getIdentity()))
                    )
            );
        } else {
            requestLogger.logSqlQuery(
                    RequestLogLine.forSql(
                            null,
                            null,
                            DateTimes.nowUtc(),
                            request.getRemoteAddr(),
                            new QueryStats(ImmutableMap.of("success", false, "exception", errorMessage, "identity", authenticationResult.getIdentity()))
                    )
            );
        }

    // Write to the response
    response.setStatus(httpStatusCode);
    response.setContentType(MediaType.APPLICATION_JSON);
    objectMapper.writeValue(
        response.getOutputStream(),
        serverConfig.getErrorResponseTransformStrategy().transformIfNeeded(exceptionToReport)
    );
  }

    protected void doService(
            HttpServletRequest request,
            HttpServletResponse response
    ) throws ServletException, IOException {
        // Just call the superclass service method. Overridden in tests.
        super.service(request, response);
    }

    @Override
    protected void sendProxyRequest(
            HttpServletRequest clientRequest,
            HttpServletResponse proxyResponse,
            Request proxyRequest
    ) {
        proxyRequest.timeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);
        proxyRequest.idleTimeout(httpClientConfig.getReadTimeout().getMillis(), TimeUnit.MILLISECONDS);

        byte[] avaticaQuery = (byte[]) clientRequest.getAttribute(AVATICA_QUERY_ATTRIBUTE);
        if (avaticaQuery != null) {
            proxyRequest.content(new BytesContentProvider(avaticaQuery));
        }

        final Query query = (Query) clientRequest.getAttribute(QUERY_ATTRIBUTE);
        final SqlQuery sqlQuery = (SqlQuery) clientRequest.getAttribute(SQL_QUERY_ATTRIBUTE);
        if (query != null) {
            setProxyRequestContent(proxyRequest, clientRequest, query);
        } else if (sqlQuery != null) {
            setProxyRequestContent(proxyRequest, clientRequest, sqlQuery);
        }

        // Since we can't see the request object on the remote side, we can't check whether the remote side actually
        // performed an authorization check here, so always set this to true for the proxy servlet.
        // If the remote node failed to perform an authorization check, PreResponseAuthorizationCheckFilter
        // will log that on the remote node.
        clientRequest.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);

        // Check if there is an authentication result and use it to decorate the proxy request if needed.
        AuthenticationResult authenticationResult = (AuthenticationResult) clientRequest.getAttribute(
                AuthConfig.DRUID_AUTHENTICATION_RESULT);
        if (authenticationResult != null && authenticationResult.getAuthenticatedBy() != null) {
            Authenticator authenticator = authenticatorMapper.getAuthenticatorMap()
                    .get(authenticationResult.getAuthenticatedBy());
            if (authenticator != null) {
                authenticator.decorateProxyRequest(
                        clientRequest,
                        proxyResponse,
                        proxyRequest
                );
            } else {
                LOG.error("Can not find Authenticator with Name [%s]", authenticationResult.getAuthenticatedBy());
            }
        }
        super.sendProxyRequest(
                clientRequest,
                proxyResponse,
                proxyRequest
        );
    }

  private void setProxyRequestContent(Request proxyRequest, HttpServletRequest clientRequest, Object content)
  {
    final ObjectMapper objectMapper = (ObjectMapper) clientRequest.getAttribute(OBJECTMAPPER_ATTRIBUTE);
    try {
      byte[] bytes = objectMapper.writeValueAsBytes(content);
      proxyRequest.content(new BytesContentProvider(bytes));
      proxyRequest.getHeaders().put(HttpHeader.CONTENT_LENGTH, String.valueOf(bytes.length));
      proxyRequest.getHeaders().put(HttpHeader.CONTENT_TYPE, objectMapper.getFactory() instanceof SmileFactory ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

    @Override
    protected Response.Listener newProxyResponseListener(HttpServletRequest request, HttpServletResponse response) {
        boolean isJDBC = request.getAttribute(AVATICA_QUERY_ATTRIBUTE) != null;
        return newMetricsEmittingProxyResponseListener(
                request,
                response,
                (Query) request.getAttribute(QUERY_ATTRIBUTE),
                (SqlQuery) request.getAttribute(SQL_QUERY_ATTRIBUTE),
                isJDBC,
                System.nanoTime()
        );
    }

    @Override
    protected String rewriteTarget(HttpServletRequest request) {
        return rewriteURI(
                request,
                (String) request.getAttribute(SCHEME_ATTRIBUTE),
                (String) request.getAttribute(HOST_ATTRIBUTE)
        );
    }

    protected String rewriteURI(HttpServletRequest request, String scheme, String host) {
        return makeURI(scheme, host, request.getRequestURI(), request.getQueryString());
    }

    @VisibleForTesting
    static String makeURI(String scheme, String host, String requestURI, String rawQueryString) {
        return JettyUtils.concatenateForRewrite(
                scheme + "://" + host,
                requestURI,
                rawQueryString
        );
    }

    // newHttpClient --> Uses httpClientProvider to get the HttpClient instance
    @Override
    protected HttpClient newHttpClient() {
        return httpClientProvider.get();
    }

    @Override
    protected HttpClient createHttpClient() throws ServletException {
        HttpClient client = super.createHttpClient();
        // override timeout set in ProxyServlet.createHttpClient
        setTimeout(httpClientConfig.getReadTimeout().getMillis());
        return client;
    }

    private Response.Listener newMetricsEmittingProxyResponseListener(
            HttpServletRequest request,
            HttpServletResponse response,
            @Nullable Query query,
            @Nullable SqlQuery sqlQuery,
            boolean isJDBC,
            long startNs
    ) {
        return new MetricsEmittingProxyResponseListener(request, response, query, sqlQuery, isJDBC, startNs);
    }

    @Override
    public long getSuccessfulQueryCount() {
        return successfulQueryCount.get();
    }

    @Override
    public long getFailedQueryCount() {
        return failedQueryCount.get();
    }

    @Override
    public long getInterruptedQueryCount() {
        return interruptedQueryCount.get();
    }

    @Override
    public long getTimedOutQueryCount() {
        // Query timeout metric is not relevant here and this metric is already being tracked in the Broker and the
        // data nodes using QueryResource
        return 0L;
    }

    @Override
    protected void onServerResponseHeaders(
            HttpServletRequest clientRequest,
            HttpServletResponse proxyResponse,
            Response serverResponse
    ) {
        StandardResponseHeaderFilterHolder.deduplicateHeadersInProxyServlet(proxyResponse, serverResponse);
        super.onServerResponseHeaders(clientRequest, proxyResponse, serverResponse);
    }

    @VisibleForTesting
    static String getAvaticaConnectionId(Map<String, Object> requestMap) {
        // avatica commands always have a 'connectionId'. If commands are not part of a prepared statement, this appears at
        // the top level of the request, but if it is part of a statement, then it will be nested in the 'statementHandle'.
        // see https://calcite.apache.org/avatica/docs/json_reference.html#requests for more details
        Object connectionIdObj = requestMap.get(AVATICA_CONNECTION_ID);
        if (connectionIdObj == null) {
            Object statementHandle = requestMap.get(AVATICA_STATEMENT_HANDLE);
            if (statementHandle != null && statementHandle instanceof Map) {
                connectionIdObj = ((Map) statementHandle).get(AVATICA_CONNECTION_ID);
            }
        }

        if (connectionIdObj == null) {
            throw new IAE("Received an Avatica request without a %s.", AVATICA_CONNECTION_ID);
        }
        if (!(connectionIdObj instanceof String)) {
            throw new IAE("Received an Avatica request with a non-String %s.", AVATICA_CONNECTION_ID);
        }

        return (String) connectionIdObj;
    }

    static String getAvaticaProtobufConnectionId(Service.Request request) {
        if (request instanceof Service.CatalogsRequest) {
            return ((Service.CatalogsRequest) request).connectionId;
        }

        if (request instanceof Service.SchemasRequest) {
            return ((Service.SchemasRequest) request).connectionId;
        }

        if (request instanceof Service.TablesRequest) {
            return ((Service.TablesRequest) request).connectionId;
        }

        if (request instanceof Service.TypeInfoRequest) {
            return ((Service.TypeInfoRequest) request).connectionId;
        }

        if (request instanceof Service.ColumnsRequest) {
            return ((Service.ColumnsRequest) request).connectionId;
        }

        if (request instanceof Service.ExecuteRequest) {
            return ((Service.ExecuteRequest) request).statementHandle.connectionId;
        }

        if (request instanceof Service.TableTypesRequest) {
            return ((Service.TableTypesRequest) request).connectionId;
        }

        if (request instanceof Service.PrepareRequest) {
            return ((Service.PrepareRequest) request).connectionId;
        }

        if (request instanceof Service.PrepareAndExecuteRequest) {
            return ((Service.PrepareAndExecuteRequest) request).connectionId;
        }

        if (request instanceof Service.FetchRequest) {
            return ((Service.FetchRequest) request).connectionId;
        }

        if (request instanceof Service.CreateStatementRequest) {
            return ((Service.CreateStatementRequest) request).connectionId;
        }

        if (request instanceof Service.CloseStatementRequest) {
            return ((Service.CloseStatementRequest) request).connectionId;
        }

        if (request instanceof Service.OpenConnectionRequest) {
            return ((Service.OpenConnectionRequest) request).connectionId;
        }

        if (request instanceof Service.CloseConnectionRequest) {
            return ((Service.CloseConnectionRequest) request).connectionId;
        }

        if (request instanceof Service.ConnectionSyncRequest) {
            return ((Service.ConnectionSyncRequest) request).connectionId;
        }

        if (request instanceof Service.DatabasePropertyRequest) {
            return ((Service.DatabasePropertyRequest) request).connectionId;
        }

        if (request instanceof Service.SyncResultsRequest) {
            return ((Service.SyncResultsRequest) request).connectionId;
        }

        if (request instanceof Service.CommitRequest) {
            return ((Service.CommitRequest) request).connectionId;
        }

        if (request instanceof Service.RollbackRequest) {
            return ((Service.RollbackRequest) request).connectionId;
        }

        if (request instanceof Service.PrepareAndExecuteBatchRequest) {
            return ((Service.PrepareAndExecuteBatchRequest) request).connectionId;
        }

        if (request instanceof Service.ExecuteBatchRequest) {
            return ((Service.ExecuteBatchRequest) request).connectionId;
        }

        throw new IAE("Received an unknown Avatica protobuf request");
    }

    private class MetricsEmittingProxyResponseListener<T> extends ProxyResponseListener {
        private final HttpServletRequest req;
        @Nullable
        private final Query<T> query;
        @Nullable
        private final SqlQuery sqlQuery;
        private final boolean isJDBC;
        private final long startNs;

        public MetricsEmittingProxyResponseListener(
                HttpServletRequest request,
                HttpServletResponse response,
                @Nullable Query<T> query,
                @Nullable SqlQuery sqlQuery,
                boolean isJDBC,
                long startNs
        ) {
            super(request, response);

            this.req = request;
            this.query = query;
            this.sqlQuery = sqlQuery;
            this.isJDBC = isJDBC;
            this.startNs = startNs;
        }

        @Override
        public void onComplete(Result result) {
            final long requestTimeNs = System.nanoTime() - startNs;
            String queryId = null;
            String sqlQueryId = null;
            if (isJDBC) {
                sqlQueryId = result.getResponse().getHeaders().get(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER);
            } else if (sqlQuery != null) {
                sqlQueryId = (String) sqlQuery.getContext().getOrDefault(BaseQuery.SQL_QUERY_ID, null);
                queryId = (String) sqlQuery.getContext().getOrDefault(BaseQuery.QUERY_ID, null);
            } else if (query != null) {
                queryId = query.getId();
            }

            // not a native or SQL query, no need to emit metrics and logs
            if (queryId == null && sqlQueryId == null) {
                super.onComplete(result);
                return;
            }

            boolean success = result.isSucceeded();
            if (success) {
                successfulQueryCount.incrementAndGet();
            } else {
                failedQueryCount.incrementAndGet();
            }
            emitQueryTime(requestTimeNs, success, sqlQueryId, queryId);

            AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

            //noinspection VariableNotUsedInsideIf
            if (sqlQueryId != null) {
                // SQL query doesn't have a native query translation in router. Hence, not logging the native query.
                if (sqlQuery != null) {
                    try {
                        requestLogger.logSqlQuery(
                                RequestLogLine.forSql(
                                        sqlQuery.getQuery(),
                                        sqlQuery.getContext(),
                                        DateTimes.nowUtc(),
                                        req.getRemoteAddr(),
                                        new QueryStats(
                                                ImmutableMap.of(
                                                        "query/time",
                                                        TimeUnit.NANOSECONDS.toMillis(requestTimeNs),
                                                        "success",
                                                        success
                                                                && result.getResponse().getStatus() == Status.OK.getStatusCode(),
                                                        "identity",
                                                        authenticationResult.getIdentity()
                                                )
                                        )
                                )
                        );
                    } catch (IOException e) {
                        LOG.error(e, "Unable to log SQL query [%s]!", sqlQuery);
                    }
                }
                super.onComplete(result);
                return;
            }

            try {
                requestLogger.logNativeQuery(
                        RequestLogLine.forNative(
                                query,
                                DateTimes.nowUtc(),
                                req.getRemoteAddr(),
                                new QueryStats(
                                        ImmutableMap.of(
                                                "query/time",
                                                TimeUnit.NANOSECONDS.toMillis(requestTimeNs),
                                                "success",
                                                success
                                                        && result.getResponse().getStatus() == Status.OK.getStatusCode(),
                                                "identity",
                                                authenticationResult.getIdentity()
                                        )
                                )
                        )
                );
            } catch (Exception e) {
                LOG.error(e, "Unable to log query [%s]!", query);
            }

            super.onComplete(result);
        }

        @Override
        public void onFailure(Response response, Throwable failure) {
            final long requestTimeNs = System.nanoTime() - startNs;
            final String errorMessage = failure.getMessage();
            String queryId = null;
            String sqlQueryId = null;
            if (isJDBC) {
                sqlQueryId = response.getHeaders().get(SqlResource.SQL_QUERY_ID_RESPONSE_HEADER);
            } else if (sqlQuery != null) {
                sqlQueryId = (String) sqlQuery.getContext().getOrDefault(BaseQuery.SQL_QUERY_ID, null);
                queryId = (String) sqlQuery.getContext().getOrDefault(BaseQuery.QUERY_ID, null);
            } else if (query != null) {
                queryId = query.getId();
            }

            // not a native or SQL query, no need to emit metrics and logs
            if (queryId == null && sqlQueryId == null) {
                super.onFailure(response, failure);
                return;
            }

            failedQueryCount.incrementAndGet();
            emitQueryTime(requestTimeNs, false, sqlQueryId, queryId);
            AuthenticationResult authenticationResult = AuthorizationUtils.authenticationResultFromRequest(req);

            //noinspection VariableNotUsedInsideIf
            if (sqlQueryId != null) {
                // SQL query doesn't have a native query translation in router. Hence, not logging the native query.
                if (sqlQuery != null) {
                    try {
                        requestLogger.logSqlQuery(
                                RequestLogLine.forSql(
                                        sqlQuery.getQuery(),
                                        sqlQuery.getContext(),
                                        DateTimes.nowUtc(),
                                        req.getRemoteAddr(),
                                        new QueryStats(
                                                ImmutableMap.of(
                                                        "success",
                                                        false,
                                                        "exception",
                                                        errorMessage == null ? "no message" : errorMessage,
                                                        "identity",
                                                        authenticationResult.getIdentity()
                                                )
                                        )
                                )
                        );
                    } catch (IOException e) {
                        LOG.error(e, "Unable to log SQL query [%s]!", sqlQuery);
                    }
                    LOG.makeAlert(failure, "Exception handling request")
                            .addData("exception", failure.toString())
                            .addData("sqlQuery", sqlQuery)
                            .addData("peer", req.getRemoteAddr())
                            .emit();
                }
                super.onFailure(response, failure);
                return;
            }

            try {
                requestLogger.logNativeQuery(
                        RequestLogLine.forNative(
                                query,
                                DateTimes.nowUtc(),
                                req.getRemoteAddr(),
                                new QueryStats(
                                        ImmutableMap.of(
                                                "success",
                                                false,
                                                "exception",
                                                errorMessage == null ? "no message" : errorMessage,
                                                "identity",
                                                authenticationResult.getIdentity()
                                        )
                                )
                        )
                );
            } catch (IOException logError) {
                LOG.error(logError, "Unable to log query [%s]!", query);
            }

            LOG.makeAlert(failure, "Exception handling request")
                    .addData("exception", failure.toString())
                    .addData("query", query)
                    .addData("peer", req.getRemoteAddr())
                    .emit();

            super.onFailure(response, failure);
        }

        private void emitQueryTime(
                long requestTimeNs,
                boolean success,
                @Nullable String sqlQueryId,
                @Nullable String queryId
        ) {
            QueryMetrics queryMetrics;
            if (sqlQueryId != null) {
                queryMetrics = queryMetricsFactory.makeMetrics();
                queryMetrics.remoteAddress(req.getRemoteAddr());
                // Setting sqlQueryId and queryId dimensions to the metric
                queryMetrics.sqlQueryId(sqlQueryId);
                if (queryId != null) { // query id is null for JDBC SQL
                    queryMetrics.queryId(queryId);
                }
            } else {
                queryMetrics = DruidMetrics.makeRequestMetrics(
                        queryMetricsFactory,
                        warehouse.getToolChest(query),
                        query,
                        req.getRemoteAddr()
                );
            }
            queryMetrics.success(success);
            queryMetrics.reportQueryTime(requestTimeNs).emit(emitter);
        }
    }
}
