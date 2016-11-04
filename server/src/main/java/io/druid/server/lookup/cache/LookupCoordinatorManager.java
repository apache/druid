/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.server.lookup.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.SequenceInputStreamResponseHandler;
import io.druid.audit.AuditInfo;
import io.druid.common.config.JacksonConfigManager;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StreamUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.query.lookup.LookupModule;
import io.druid.server.listener.announcer.ListenerDiscoverer;
import io.druid.server.listener.resource.ListenerResource;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class LookupCoordinatorManager
{
  public static final String LOOKUP_CONFIG_KEY = "lookups";
  // Doesn't have to be the same, but it makes things easy to look at
  public static final String LOOKUP_LISTEN_ANNOUNCE_KEY = LOOKUP_CONFIG_KEY;
  private static final EmittingLogger LOG = new EmittingLogger(LookupCoordinatorManager.class);
  private static final TypeReference<Map<String, Object>> MAP_STRING_OBJ_TYPE = new TypeReference<Map<String, Object>>()
  {
  };
  private final static Function<HostAndPort, URL> HOST_TO_URL = new Function<HostAndPort, URL>()
  {
    @Nullable
    @Override
    public URL apply(HostAndPort input)
    {
      if (input == null) {
        LOG.warn("null entry in lookups");
        return null;
      }
      try {
        return getLookupsURL(input);
      }
      catch (MalformedURLException e) {
        LOG.warn(e, "Skipping node. Malformed URL from `%s`", input);
        return null;
      }
    }
  };

  private final ListeningScheduledExecutorService executorService;
  private final ListenerDiscoverer listenerDiscoverer;
  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;
  private final JacksonConfigManager configManager;
  private final LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig;
  private final Object startStopSync = new Object();
  // Updated by config watching service
  private AtomicReference<Map<String, Map<String, Map<String, Object>>>> lookupMapConfigRef;
  private volatile Map<String, Map<String, Map<String, Object>>> prior_update = ImmutableMap.of();
  private volatile boolean started = false;
  private volatile ListenableScheduledFuture<?> backgroundManagerFuture = null;
  private final CountDownLatch backgroundManagerExitedLatch = new CountDownLatch(1);


  @Inject
  public LookupCoordinatorManager(
      final @Global HttpClient httpClient,
      final ListenerDiscoverer listenerDiscoverer,
      final @Smile ObjectMapper smileMapper,
      final JacksonConfigManager configManager,
      final LookupCoordinatorManagerConfig lookupCoordinatorManagerConfig
  )
  {
    this.listenerDiscoverer = listenerDiscoverer;
    this.configManager = configManager;
    this.httpClient = httpClient;
    this.smileMapper = smileMapper;
    this.lookupCoordinatorManagerConfig = lookupCoordinatorManagerConfig;
    executorService = MoreExecutors.listeningDecorator(
        Executors.newScheduledThreadPool(
            lookupCoordinatorManagerConfig.getThreadPoolSize(),
            Execs.makeThreadFactory("LookupCoordinatorManager--%s")
        )
    );
  }

  void deleteOnHost(final URL url)
      throws ExecutionException, InterruptedException, IOException
  {
    final AtomicInteger returnCode = new AtomicInteger(0);
    final AtomicReference<String> reasonString = new AtomicReference<>(null);
    LOG.debug("Dropping %s", url);

    try (final InputStream result = httpClient.go(
        new Request(HttpMethod.DELETE, url)
            .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE),
        makeResponseHandler(returnCode, reasonString),
        lookupCoordinatorManagerConfig.getHostDeleteTimeout()
    ).get()) {
      // 404 is ok here, that means it was already deleted
      if (!httpStatusIsSuccess(returnCode.get()) && !httpStatusIsNotFound(returnCode.get())) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          StreamUtils.copyAndClose(result, baos);
        }
        catch (IOException e2) {
          LOG.warn(e2, "Error reading response from [%s]", url);
        }

        throw new IOException(
            String.format(
                "Bad lookup delete request to [%s] : [%d] : [%s]  Response: [%s]",
                url,
                returnCode.get(),
                reasonString.get(),
                StringUtils.fromUtf8(baos.toByteArray())
            )
        );
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Delete to [%s] : Status: %s reason: [%s]", url, returnCode.get(), reasonString.get());
        }
      }
    }
  }

  void updateAllOnHost(final URL url, Map<String, Map<String, Object>> knownLookups)
      throws IOException, InterruptedException, ExecutionException
  {
    final AtomicInteger returnCode = new AtomicInteger(0);
    final AtomicReference<String> reasonString = new AtomicReference<>(null);
    final byte[] bytes;
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Loading up %d lookups to %s", knownLookups.size(), url);
      }
      bytes = smileMapper.writeValueAsBytes(knownLookups);
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }

    try (final InputStream result = httpClient.go(
        new Request(HttpMethod.POST, url)
            .addHeader(HttpHeaders.Names.ACCEPT, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
            .addHeader(HttpHeaders.Names.CONTENT_TYPE, SmileMediaTypes.APPLICATION_JACKSON_SMILE)
            .setContent(bytes),
        makeResponseHandler(returnCode, reasonString),
        lookupCoordinatorManagerConfig.getHostUpdateTimeout()
    ).get()) {
      if (!httpStatusIsSuccess(returnCode.get())) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
          StreamUtils.copyAndClose(result, baos);
        }
        catch (IOException e2) {
          LOG.warn(e2, "Error reading response");
        }

        throw new IOException(
            String.format(
                "Bad update request to [%s] : [%d] : [%s]  Response: [%s]",
                url,
                returnCode.get(),
                reasonString.get(),
                StringUtils.fromUtf8(baos.toByteArray())
            )
        );
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Update on [%s], Status: %s reason: [%s]", url, returnCode.get(), reasonString.get());
        }
        final Map<String, Object> resultMap = smileMapper.readValue(result, MAP_STRING_OBJ_TYPE);
        final Object missingValuesObject = resultMap.get(LookupModule.FAILED_UPDATES_KEY);
        if (null == missingValuesObject) {
          throw new IAE("Update result did not have field for [%s]", LookupModule.FAILED_UPDATES_KEY);
        }

        final Map<String, Object> missingValues = smileMapper.convertValue(missingValuesObject, MAP_STRING_OBJ_TYPE);
        if (!missingValues.isEmpty()) {
          throw new IAE("Lookups failed to update: %s", smileMapper.writeValueAsString(missingValues.keySet()));
        } else {
          LOG.debug("Updated all lookups on [%s]", url);
        }
      }
    }
  }

  // Overridden in unit tests
  HttpResponseHandler<InputStream, InputStream> makeResponseHandler(
      final AtomicInteger returnCode,
      final AtomicReference<String> reasonString
  )
  {
    return new SequenceInputStreamResponseHandler()
    {
      @Override
      public ClientResponse<InputStream> handleResponse(HttpResponse response)
      {
        returnCode.set(response.getStatus().getCode());
        reasonString.set(response.getStatus().getReasonPhrase());
        return super.handleResponse(response);
      }
    };
  }

  void deleteAllOnTier(final String tier, final Collection<String> dropLookups)
      throws ExecutionException, InterruptedException, IOException
  {
    if (dropLookups.isEmpty()) {
      LOG.debug("Nothing to drop");
      return;
    }
    final Collection<URL> urls = getAllHostsAnnounceEndpoint(tier);
    final List<ListenableFuture<?>> futures = new ArrayList<>(urls.size());
    for (final URL url : urls) {
      futures.add(executorService.submit(new Runnable()
      {
        @Override
        public void run()
        {
          for (final String drop : dropLookups) {
            final URL lookupURL;
            try {
              lookupURL = new URL(
                  url.getProtocol(),
                  url.getHost(),
                  url.getPort(),
                  String.format("%s/%s", url.getFile(), drop)
              );
            }
            catch (MalformedURLException e) {
              throw new ISE(e, "Error creating url for [%s]/[%s]", url, drop);
            }
            try {
              deleteOnHost(lookupURL);
            }
            catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              LOG.warn("Delete [%s] interrupted", lookupURL);
              throw Throwables.propagate(e);
            }
            catch (IOException | ExecutionException e) {
              // Don't raise as ExecutionException. Just log and continue
              LOG.makeAlert(e, "Error deleting [%s]", lookupURL).emit();
            }
          }
        }
      }));
    }
    final ListenableFuture allFuture = Futures.allAsList(futures);
    try {
      allFuture.get(lookupCoordinatorManagerConfig.getUpdateAllTimeout().getMillis(), TimeUnit.MILLISECONDS);
    }
    catch (TimeoutException e) {
      // This should cause Interrupted exceptions on the offending ones
      allFuture.cancel(true);
      throw new ExecutionException("Timeout in updating hosts! Attempting to cancel", e);
    }
  }

  void updateAllNewOnTier(final String tier, final Map<String, Map<String, Object>> knownLookups)
      throws InterruptedException, ExecutionException, IOException
  {
    final Collection<URL> urls = Collections2.transform(
        listenerDiscoverer.getNewNodes(LookupModule.getTierListenerPath(tier)),
        HOST_TO_URL
    );
    if (urls.isEmpty() || knownLookups.isEmpty()) {
      LOG.debug("Nothing new to report");
      return;
    }
    updateNodes(urls, knownLookups);
  }

  void updateAllOnTier(final String tier, final Map<String, Map<String, Object>> knownLookups)
      throws InterruptedException, ExecutionException, IOException
  {
    updateNodes(getAllHostsAnnounceEndpoint(tier), knownLookups);
  }

  void updateNodes(Collection<URL> urls, final Map<String, Map<String, Object>> knownLookups)
      throws IOException, InterruptedException, ExecutionException
  {
    if (knownLookups == null) {
      LOG.debug("No config for lookups found");
      return;
    }
    if (knownLookups.isEmpty()) {
      LOG.debug("No known lookups. Skipping update");
      return;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating %d lookups on %d nodes", knownLookups.size(), urls.size());
    }
    final List<ListenableFuture<?>> futures = new ArrayList<>(urls.size());
    for (final URL url : urls) {
      futures.add(executorService.submit(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            updateAllOnHost(url, knownLookups);
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Update on [%s] interrupted", url);
            throw Throwables.propagate(e);
          }
          catch (IOException | ExecutionException e) {
            // Don't raise as ExecutionException. Just log and continue
            LOG.makeAlert(e, "Error submitting to [%s]", url).emit();
          }
        }
      }));
    }
    final ListenableFuture allFuture = Futures.allAsList(futures);
    try {
      allFuture.get(lookupCoordinatorManagerConfig.getUpdateAllTimeout().getMillis(), TimeUnit.MILLISECONDS);
    }
    catch (TimeoutException e) {
      LOG.warn("Timeout in updating hosts! Attempting to cancel");
      // This should cause Interrupted exceptions on the offending ones
      allFuture.cancel(true);
    }
  }

  Collection<URL> getAllHostsAnnounceEndpoint(final String tier) throws IOException
  {
    return ImmutableList.copyOf(
        Collections2.filter(
            Collections2.transform(
                listenerDiscoverer.getNodes(LookupModule.getTierListenerPath(tier)),
                HOST_TO_URL
            ),
            Predicates.notNull()
        )
    );
  }

  public boolean updateLookup(
      final String tier,
      final String lookupName,
      Map<String, Object> spec,
      final AuditInfo auditInfo
  )
  {
    return updateLookups(
        ImmutableMap.<String, Map<String, Map<String, Object>>>of(tier, ImmutableMap.of(lookupName, spec)),
        auditInfo
    );
  }

  public boolean updateLookups(final Map<String, Map<String, Map<String, Object>>> updateSpec, AuditInfo auditInfo)
  {
    synchronized (startStopSync) {
      final Map<String, Map<String, Map<String, Object>>> priorSpec = getKnownLookups();
      if (priorSpec == null && !updateSpec.isEmpty()) {
        // To prevent accidentally erasing configs if we haven't updated our cache of the values
        throw new ISE("Not initialized. If this is the first lookup, post an empty map to initialize");
      }
      final Map<String, Map<String, Map<String, Object>>> updatedSpec;

      // Only add or update here, don't delete.
      if (priorSpec == null) {
        // all new
        updatedSpec = updateSpec;
      } else {
        // Needs update
        updatedSpec = new HashMap<>(priorSpec);
        for (final String tier : updateSpec.keySet()) {
          final Map<String, Map<String, Object>> priorTierSpec = priorSpec.get(tier);
          final Map<String, Map<String, Object>> updateTierSpec = updateSpec.get(tier);
          if (priorTierSpec == null) {
            // New tier
            updatedSpec.put(tier, updateTierSpec);
          } else {
            // Update existing tier
            final Map<String, Map<String, Object>> updatedTierSpec = new HashMap<>(priorTierSpec);
            updatedTierSpec.putAll(updateTierSpec);
            updatedSpec.put(tier, updatedTierSpec);
          }
        }
      }
      return configManager.set(LOOKUP_CONFIG_KEY, updatedSpec, auditInfo);
    }
  }

  public Map<String, Map<String, Map<String, Object>>> getKnownLookups()
  {
    if (!started) {
      throw new ISE("Not started");
    }
    return lookupMapConfigRef.get();
  }

  public boolean deleteLookup(final String tier, final String lookup, AuditInfo auditInfo)
  {
    synchronized (startStopSync) {
      final Map<String, Map<String, Map<String, Object>>> priorSpec = getKnownLookups();
      if (priorSpec == null) {
        LOG.warn("Requested delete lookup [%s]/[%s]. But no lookups exist!", tier, lookup);
        return false;
      }
      final Map<String, Map<String, Map<String, Object>>> updateSpec = new HashMap<>(priorSpec);
      final Map<String, Map<String, Object>> priorTierSpec = updateSpec.get(tier);
      if (priorTierSpec == null) {
        LOG.warn("Requested delete of lookup [%s]/[%s] but tier does not exist!", tier, lookup);
        return false;
      }

      if (!priorTierSpec.containsKey(lookup)) {
        LOG.warn("Requested delete of lookup [%s]/[%s] but lookup does not exist!", tier, lookup);
        return false;
      }

      final Map<String, Map<String, Object>> updateTierSpec = new HashMap<>(priorTierSpec);
      updateTierSpec.remove(lookup);
      updateSpec.put(tier, updateTierSpec);
      return configManager.set(LOOKUP_CONFIG_KEY, updateSpec, auditInfo);
    }
  }

  public Collection<String> discoverTiers()
  {
    try {
      return listenerDiscoverer.discoverChildren(LookupCoordinatorManager.LOOKUP_LISTEN_ANNOUNCE_KEY);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Try to find a lookupName spec for the specified lookupName.
   *
   * @param lookupName The lookupName to look for
   *
   * @return The lookupName spec if found or null if not found or if no lookups at all are found
   */
  public
  @Nullable
  Map<String, Object> getLookup(final String tier, final String lookupName)
  {
    final Map<String, Map<String, Map<String, Object>>> prior = getKnownLookups();
    if (prior == null) {
      LOG.warn("Requested tier [%s] lookupName [%s]. But no lookups exist!", tier, lookupName);
      return null;
    }
    final Map<String, Map<String, Object>> tierLookups = prior.get(tier);
    if (tierLookups == null) {
      LOG.warn("Tier [%s] does not exist", tier);
      return null;
    }
    return tierLookups.get(lookupName);
  }


  @LifecycleStart
  public void start()
  {
    synchronized (startStopSync) {
      if (started) {
        return;
      }
      if (executorService.isShutdown()) {
        throw new ISE("Cannot restart after stop!");
      }
      lookupMapConfigRef = configManager.watch(
          LOOKUP_CONFIG_KEY,
          new TypeReference<Map<String, Map<String, Map<String, Object>>>>()
          {
          },
          null
      );
      final ListenableScheduledFuture backgroundManagerFuture = this.backgroundManagerFuture = executorService.scheduleWithFixedDelay(
          new Runnable()
          {
            @Override
            public void run()
            {
              final Map<String, Map<String, Map<String, Object>>> allLookupTiers = lookupMapConfigRef.get();
              // Sanity check for if we are shutting down
              if (Thread.currentThread().isInterrupted()) {
                LOG.info("Not updating lookups because process was interrupted");
                return;
              }
              if (!started) {
                LOG.info("Not started. Returning");
                return;
              }
              if (allLookupTiers == null) {
                LOG.info("Not updating lookups because no data exists");
                return;
              }
              for (final String tier : allLookupTiers.keySet()) {
                try {
                  final Map<String, Map<String, Object>> allLookups = allLookupTiers.get(tier);
                  final Map<String, Map<String, Object>> oldLookups = prior_update.get(tier);
                  final Collection<String> drops;
                  if (oldLookups == null) {
                    drops = ImmutableList.of();
                  } else {
                    drops = Sets.difference(oldLookups.keySet(), allLookups.keySet());
                  }
                  if (allLookupTiers == prior_update) {
                    LOG.debug("No updates");
                    updateAllNewOnTier(tier, allLookups);
                  } else {
                    updateAllOnTier(tier, allLookups);
                    deleteAllOnTier(tier, drops);
                  }
                }
                catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw Throwables.propagate(e);
                }
                catch (Exception e) {
                  LOG.error(e, "Error updating lookups for tier [%s]. Will try again soon", tier);
                }
              }
              prior_update = allLookupTiers;
            }
          },
          0,
          lookupCoordinatorManagerConfig.getPeriod(),
          TimeUnit.MILLISECONDS
      );
      Futures.addCallback(backgroundManagerFuture, new FutureCallback<Object>()
      {
        @Override
        public void onSuccess(@Nullable Object result)
        {
          backgroundManagerExitedLatch.countDown();
          LOG.debug("Exited background lookup manager");
        }

        @Override
        public void onFailure(Throwable t)
        {
          backgroundManagerExitedLatch.countDown();
          if (backgroundManagerFuture.isCancelled()) {
            LOG.info("Background lookup manager exited");
            LOG.trace(t, "Background lookup manager exited with throwable");
          } else {
            LOG.makeAlert(t, "Background lookup manager exited with error!").emit();
          }
        }
      });
      started = true;
      LOG.debug("Started");
    }
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (startStopSync) {
      if (!started) {
        LOG.warn("Not started, ignoring stop request");
        return;
      }
      started = false;
      executorService.shutdownNow();
      final ListenableScheduledFuture backgroundManagerFuture = this.backgroundManagerFuture;
      this.backgroundManagerFuture = null;
      if (backgroundManagerFuture != null && !backgroundManagerFuture.cancel(true)) {
        LOG.warn("Background lookup manager thread could not be cancelled");
      }
      // NOTE: we can't un-watch the configuration key
      LOG.debug("Stopped");
    }
  }

  static URL getLookupsURL(HostAndPort druidNode) throws MalformedURLException
  {
    return new URL(
        "http",
        druidNode.getHostText(),
        druidNode.getPortOrDefault(-1),
        ListenerResource.BASE_PATH + "/" + LOOKUP_LISTEN_ANNOUNCE_KEY
    );
  }

  private static boolean httpStatusIsSuccess(int statusCode)
  {
    return statusCode >= 200 && statusCode < 300;
  }

  private static boolean httpStatusIsNotFound(int statusCode)
  {
    return statusCode == 404;
  }

  @VisibleForTesting
  boolean backgroundManagerIsRunning()
  {
    ListenableScheduledFuture backgroundManagerFuture = this.backgroundManagerFuture;
    return backgroundManagerFuture != null && !backgroundManagerFuture.isDone();
  }

  @VisibleForTesting
  boolean waitForBackgroundTermination(long timeout) throws InterruptedException
  {
    return backgroundManagerExitedLatch.await(timeout, TimeUnit.MILLISECONDS);
  }
}
