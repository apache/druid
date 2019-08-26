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

package org.apache.druid.discovery;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.client.selector.Server;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.curator.discovery.ServerDiscoverySelector;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class facilitates interaction with Coordinator/Overlord leader nodes. Instance of this class is injected
 * via Guice with annotations @Coordinator or @IndexingService .
 * Usage:
 * Request request = druidLeaderClient.makeRequest(HttpMethod, requestPath)
 * request.setXXX(..)
 * FullResponseHolder responseHolder = druidLeaderClient.go(request)
 */
public class DruidLeaderClient
{
  private final Logger log = new Logger(DruidLeaderClient.class);

  private static final int MAX_RETRIES = 5;

  private final HttpClient httpClient;
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final NodeType nodeTypeToWatch;

  private final String leaderRequestPath;

  //Note: This is kept for back compatibility with pre 0.11.0 releases and should be removed in future.
  private final ServerDiscoverySelector serverDiscoverySelector;

  private LifecycleLock lifecycleLock = new LifecycleLock();
  private DruidNodeDiscovery druidNodeDiscovery;
  private AtomicReference<String> currentKnownLeader = new AtomicReference<>();

  public DruidLeaderClient(
      HttpClient httpClient,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      NodeType nodeTypeToWatch,
      String leaderRequestPath,
      ServerDiscoverySelector serverDiscoverySelector
  )
  {
    this.httpClient = httpClient;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.nodeTypeToWatch = nodeTypeToWatch;
    this.leaderRequestPath = leaderRequestPath;
    this.serverDiscoverySelector = serverDiscoverySelector;
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      druidNodeDiscovery = druidNodeDiscoveryProvider.getForNodeType(nodeTypeToWatch);
      lifecycleLock.started();
      log.info("Started.");
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    log.info("Stopped.");
  }

  /**
   * Make a Request object aimed at the leader. Throws IOException if the leader cannot be located.
   *
   * @param cached Uses cached leader if true, else uses the current leader
   */
  public Request makeRequest(HttpMethod httpMethod, String urlPath, boolean cached) throws IOException
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    return new Request(httpMethod, new URL(StringUtils.format("%s%s", getCurrentKnownLeader(cached), urlPath)));
  }

  /**
   * Make a Request object aimed at the leader. Throws IOException if the leader cannot be located.
   */
  public Request makeRequest(HttpMethod httpMethod, String urlPath) throws IOException
  {
    return makeRequest(httpMethod, urlPath, true);
  }

  public StringFullResponseHolder go(Request request) throws IOException, InterruptedException
  {
    return go(request, new StringFullResponseHandler(StandardCharsets.UTF_8));
  }

  /**
   * Executes the request object aimed at the leader and process the response with given handler
   * Note: this method doesn't do retrying on errors or handle leader changes occurred during communication
   */
  public <Intermediate, Final> ListenableFuture<Final> goAsync(
      final Request request,
      final HttpResponseHandler<Intermediate, Final> handler
  )
  {
    return httpClient.go(request, handler);
  }

  /**
   * Executes a Request object aimed at the leader. Throws IOException if the leader cannot be located.
   */
  public <T, H extends FullResponseHolder<T>> H go(Request request, HttpResponseHandler<H, H> responseHandler)
      throws IOException, InterruptedException
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    for (int counter = 0; counter < MAX_RETRIES; counter++) {

      final H fullResponseHolder;

      try {
        try {
          fullResponseHolder = httpClient.go(request, responseHandler).get();
        }
        catch (ExecutionException e) {
          // Unwrap IOExceptions and ChannelExceptions, re-throw others
          Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
          Throwables.propagateIfInstanceOf(e.getCause(), ChannelException.class);
          throw new RE(e, "HTTP request to[%s] failed", request.getUrl());
        }
      }
      catch (IOException | ChannelException ex) {
        // can happen if the node is stopped.
        log.warn(ex, "Request[%s] failed.", request.getUrl());

        try {
          if (request.getUrl().getQuery() == null) {
            request = withUrl(
                request,
                new URL(StringUtils.format("%s%s", getCurrentKnownLeader(false), request.getUrl().getPath()))
            );
          } else {
            request = withUrl(
                request,
                new URL(StringUtils.format(
                    "%s%s?%s",
                    getCurrentKnownLeader(false),
                    request.getUrl().getPath(),
                    request.getUrl().getQuery()
                ))
            );
          }
          continue;
        }
        catch (MalformedURLException e) {
          // Not an IOException; this is our own fault.
          throw new ISE(
              e,
              "failed to build url with path[%] and query string [%s].",
              request.getUrl().getPath(),
              request.getUrl().getQuery()
          );
        }
      }

      if (HttpResponseStatus.TEMPORARY_REDIRECT.equals(fullResponseHolder.getResponse().getStatus())) {
        String redirectUrlStr = fullResponseHolder.getResponse().headers().get("Location");
        if (redirectUrlStr == null) {
          throw new IOE("No redirect location is found in response from url[%s].", request.getUrl());
        }

        log.info("Request[%s] received redirect response to location [%s].", request.getUrl(), redirectUrlStr);

        final URL redirectUrl;
        try {
          redirectUrl = new URL(redirectUrlStr);
        }
        catch (MalformedURLException ex) {
          throw new IOE(
              ex,
              "Malformed redirect location is found in response from url[%s], new location[%s].",
              request.getUrl(),
              redirectUrlStr
          );
        }

        //update known leader location
        currentKnownLeader.set(StringUtils.format(
            "%s://%s:%s",
            redirectUrl.getProtocol(),
            redirectUrl.getHost(),
            redirectUrl.getPort()
        ));

        request = withUrl(request, redirectUrl);
      } else {
        return fullResponseHolder;
      }
    }

    throw new IOE("Retries exhausted, couldn't fulfill request to [%s].", request.getUrl());
  }

  public String findCurrentLeader()
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));
    final StringFullResponseHolder responseHolder;
    try {
      responseHolder = go(makeRequest(HttpMethod.GET, leaderRequestPath));
    }
    catch (Exception ex) {
      throw new ISE(ex, "Couldn't find leader.");
    }

    if (responseHolder.getStatus().getCode() == 200) {
      String leaderUrl = responseHolder.getContent();

      //verify this is valid url
      try {
        URL validatedUrl = new URL(leaderUrl);
        currentKnownLeader.set(leaderUrl);

        // validatedUrl.toString() is returned instead of leaderUrl or else teamcity build fails because of breaking
        // the rule of ignoring new URL(leaderUrl) object.
        return validatedUrl.toString();
      }
      catch (MalformedURLException ex) {
        log.error(ex, "Received malformed leader url[%s].", leaderUrl);
      }
    }

    throw new ISE(
        "Couldn't find leader, failed response status is [%s] and content [%s].",
        responseHolder.getStatus().getCode(),
        responseHolder.getContent()
    );
  }

  private String getCurrentKnownLeader(final boolean cached) throws IOException
  {
    final String leader = currentKnownLeader.accumulateAndGet(
        null,
        (current, given) -> current == null || !cached ? pickOneHost() : current
    );

    if (leader == null) {
      throw new IOE("No known server");
    } else {
      return leader;
    }
  }

  @Nullable
  private String pickOneHost()
  {
    Server server = serverDiscoverySelector.pick();
    if (server != null) {
      return StringUtils.format(
          "%s://%s:%s",
          server.getScheme(),
          server.getAddress(),
          server.getPort()
      );
    }

    Iterator<DiscoveryDruidNode> iter = druidNodeDiscovery.getAllNodes().iterator();
    if (iter.hasNext()) {
      DiscoveryDruidNode node = iter.next();
      return StringUtils.format(
          "%s://%s",
          node.getDruidNode().getServiceScheme(),
          node.getDruidNode().getHostAndPortToUse()
      );
    }

    return null;
  }

  private Request withUrl(Request old, URL url)
  {
    Request req = new Request(old.getMethod(), url);
    req.addHeaderValues(old.getHeaders());
    if (old.hasContent()) {
      req.setContent(old.getContent());
    }
    return req;
  }
}
