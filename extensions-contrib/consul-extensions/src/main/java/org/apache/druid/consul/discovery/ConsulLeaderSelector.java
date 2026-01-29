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

package org.apache.druid.consul.discovery;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.QueryParams;
import com.ecwid.consul.v1.Response;
import com.ecwid.consul.v1.kv.model.GetValue;
import com.ecwid.consul.v1.kv.model.PutParams;
import com.ecwid.consul.v1.session.model.NewSession;
import com.ecwid.consul.v1.session.model.Session;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.DruidNode;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consul-based implementation of {@link DruidLeaderSelector} using Consul sessions and KV locks.
 *
 * <p>{@link #registerListener(Listener)} starts background executors. Leader election runs on one single-thread
 * executor and invokes {@link Listener} callbacks; session renewal runs on another single-thread executor.
 * {@link #unregisterListener()} may invoke {@link Listener#stopBeingLeader()} on the calling thread.
 *
 * <p>Consul RPCs and retry backoffs are performed on the background threads; avoid invoking lifecycle methods from
 * time-sensitive threads.
 */
public class ConsulLeaderSelector implements DruidLeaderSelector
{
  private static final Logger LOGGER = new Logger(ConsulLeaderSelector.class);

  private final LifecycleLock lifecycleLock = new LifecycleLock();
  private final DruidNode self;
  private final String lockKey;
  private final ConsulDiscoveryConfig config;
  private final ConsulClient consulClient;
  @Inject(optional = true)
  @Nullable
  private ServiceEmitter emitter = null;

  private volatile DruidLeaderSelector.Listener listener = null;
  private final AtomicBoolean leader = new AtomicBoolean(false);
  private final AtomicInteger term = new AtomicInteger(0);

  private ScheduledExecutorService executorService;
  private ScheduledExecutorService sessionKeeperService;
  private volatile String sessionId;
  private volatile boolean stopping = false;
  private long errorRetryCount = 0;

  public ConsulLeaderSelector(
      DruidNode self,
      String lockKey,
      ConsulDiscoveryConfig config,
      ConsulClient consulClient
  )
  {
    this.self = Preconditions.checkNotNull(self, "self");
    this.lockKey = Preconditions.checkNotNull(lockKey, "lockKey");
    this.config = Preconditions.checkNotNull(config, "config");
    this.consulClient = Preconditions.checkNotNull(consulClient, "consulClient");

    if (config.getLeader().getLeaderSessionTtl().getStandardSeconds() > 120) {
      LOGGER.warn("leaderSessionTtl is %s; leader failover may take up to %s",
                  config.getLeader().getLeaderSessionTtl(),
                  Duration.standardSeconds(config.getLeader().getLeaderSessionTtl().getStandardSeconds() * 2));
    }
  }

  @Nullable
  @Override
  public String getCurrentLeader()
  {
    try {
      Response<GetValue> response = consulClient.getKVValue(
          lockKey,
          config.getAuth().getAclToken(),
          buildQueryParams()
      );
      if (response != null && response.getValue() != null && response.getValue().getValue() != null) {
        return new String(Base64.getDecoder().decode(response.getValue().getValue()), StandardCharsets.UTF_8);
      }
      return null;
    }
    catch (Exception e) {
      LOGGER.error(e, "Failed to get current leader from Consul");
      return null;
    }
  }

  @Override
  public boolean isLeader()
  {
    return leader.get();
  }

  @Override
  public int localTerm()
  {
    return term.get();
  }

  @Override
  public void registerListener(Listener listener)
  {
    Preconditions.checkArgument(listener != null, "listener is null");

    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start");
    }

    try {
      this.listener = listener;
      this.executorService = Execs.scheduledSingleThreaded("ConsulLeaderSelector-%d");
      this.sessionKeeperService = Execs.scheduledSingleThreaded("ConsulSessionKeeper-%d");

      startLeaderElection();

      lifecycleLock.started();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @Override
  public void unregisterListener()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop");
    }

    LOGGER.info("Unregistering leader selector for [%s]", lockKey);
    stopping = true;

    try {
      if (leader.get()) {
        try {
          listener.stopBeingLeader();
        }
        catch (Exception e) {
          LOGGER.error(e, "Exception while stopping being leader");
        }
        leader.set(false);
      }

      // Destroying session releases the Consul lock, allowing another node to become leader
      if (sessionId != null) {
        try {
          consulClient.sessionDestroy(sessionId, buildQueryParams(), config.getAuth().getAclToken());
        }
        catch (Exception e) {
          LOGGER.error(e, "Failed to destroy Consul session");
        }
        sessionId = null;
      }

      if (executorService != null) {
        executorService.shutdownNow();
        try {
          if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.warn("Leader selector executor did not terminate in time");
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }

      if (sessionKeeperService != null) {
        sessionKeeperService.shutdownNow();
        try {
          if (!sessionKeeperService.awaitTermination(5, TimeUnit.SECONDS)) {
            LOGGER.warn("Session keeper service did not terminate in time");
          }
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  private void startLeaderElection()
  {
    executorService.submit(this::leaderElectionLoop);
    sessionKeeperService.submit(this::sessionKeeperLoop);
  }

  private void leaderElectionLoop()
  {
    LOGGER.info("Starting leader election loop for [%s]", lockKey);
    ConsulMetrics.emitCount(emitter, "consul/leader/loop", "lock", lockKey, "state", "start");

    while (!stopping && !Thread.currentThread().isInterrupted()) {
      try {
        if (sessionId == null) {
          sessionId = createSession();
        }

        if (sessionId != null && !leader.get()) {
          if (!isSessionValid(sessionId)) {
            LOGGER.info("Follower session [%s] expired or invalid, recreating", shortSessionId(sessionId));
            sessionId = null;
            continue;
          }
        }

        boolean acquired = tryAcquireLock(sessionId);

        if (acquired && !leader.get()) {
          boolean interrupted = Thread.currentThread().isInterrupted();
          if (stopping || interrupted) {
            LOGGER.info(
                "Skipping leadership for [%s] because selector is stopping (interrupted=%s)",
                lockKey,
                interrupted
            );
          } else if (sessionId == null) {
            LOGGER.warn("Skipping leadership for [%s] because session is null", lockKey);
          } else if (validateLockOwnership(sessionId)) {
            long electionStart = System.nanoTime();
            becomeLeader();
            long electionLatency = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - electionStart);
            ConsulMetrics.emitTimer(emitter, "consul/leader/election_latency", electionLatency,
                "lock", lockKey);
          } else {
            LOGGER.warn("Lock ownership validation failed for [%s]; will retry", lockKey);
            emitOwnershipMismatchMetric();
          }
        } else if (!acquired && leader.get()) {
          loseLeadership();
        }

        if (leader.get()) {
          // Session renewal handled by sessionKeeperLoop; here we just verify lock ownership
          Thread.sleep(config.getService().getHealthCheckInterval().getMillis());
          if (sessionId != null && !validateLockOwnership(sessionId)) {
            LOGGER.warn("Main Loop: Lost lock ownership check for [%s], stepping down", lockKey);
            loseLeadership();
          }
        } else {
          Thread.sleep(config.getService().getHealthCheckInterval().getMillis());
        }
        errorRetryCount = 0;
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
      catch (Exception e) {
        LOGGER.error(e, "Error in leader election loop");

        if (leader.get()) {
          loseLeadership();
        }
        sessionId = null;

        errorRetryCount++;
        long maxLeaderRetries = config.getLeader().getLeaderMaxErrorRetries();
        if (errorRetryCount > maxLeaderRetries) {
          LOGGER.error(
              "Leader selector for [%s] exceeded max error retries [%d], giving up.",
              lockKey,
              maxLeaderRetries
          );
          ConsulMetrics.emitCount(emitter, "consul/leader/giveup", "lock", lockKey);
          break;
        }
        long base = Math.max(1L, config.getWatch().getWatchRetryDelay().getMillis());
        int exp = (int) Math.min(6, errorRetryCount);
        long backoffCap = config.getLeader().getLeaderRetryBackoffMax().getMillis();
        long backoff = Math.min(backoffCap, base * (1L << exp));
        long sleepMs = (long) (backoff * (0.5 + ThreadLocalRandom.current().nextDouble()));
        try {
          Thread.sleep(sleepMs);
        }
        catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    LOGGER.info("Exiting leader election loop for [%s]", lockKey);
    ConsulMetrics.emitCount(emitter, "consul/leader/loop", "lock", lockKey, "state", "stop");
  }

  private void sessionKeeperLoop()
  {
    if (stopping) {
      return;
    }

    String currentSessionId = this.sessionId;
    if (currentSessionId != null) {
      final String truncatedId = shortSessionId(currentSessionId);
      try {
        Response<Session> response = consulClient.renewSession(
            currentSessionId,
            buildQueryParams(),
            config.getAuth().getAclToken()
        );

        if (response == null || response.getValue() == null) {
          LOGGER.warn("Session Keeper: Failed to renew session [%s], it may have expired", truncatedId);
          // Don't null it out here, main loop handles recreating if it fails to use it
          // But we can signal leadership loss if we thought we were leader
          if (leader.get()) {
            LOGGER.error("Session Keeper: Leader lost session [%s], triggering step down", truncatedId);
            // Trigger immediate check in main loop or let main loop fail on next check
            // Ideally, we could interrupt main loop, but for safety we let main loop handle state
          }
          ConsulMetrics.emitCount(emitter, "consul/leader/renew/fail", "lock", lockKey);
        } else {
          LOGGER.debug("Session Keeper: Successfully renewed session [%s]", truncatedId);
        }
      }
      catch (Exception e) {
        LOGGER.error(e, "Session Keeper: Exception renewing session [%s]", truncatedId);
        ConsulMetrics.emitCount(emitter, "consul/leader/renew/fail", "lock", lockKey);
      }
    }

    if (!stopping) {
      sessionKeeperService.schedule(
          this::sessionKeeperLoop,
          config.getService().getHealthCheckInterval().getMillis() / 3, // Renew more frequently than TTL
          TimeUnit.MILLISECONDS
      );
    }
  }

  private boolean isSessionValid(String sid)
  {
    try {
      Response<Session> resp = consulClient.getSessionInfo(sid, buildQueryParams());
      return resp != null && resp.getValue() != null;
    }
    catch (Exception e) {
      LOGGER.debug(e, "Failed to validate session [%s]", sid);
      return false;
    }
  }

  private String createSession()
  {
    NewSession newSession = new NewSession();
    newSession.setName(StringUtils.format("druid-leader-%s", self.getHostAndPortToUse()));

    long ttlSeconds = config.getLeader().getLeaderSessionTtl().getStandardSeconds();
    newSession.setTtl(StringUtils.format("%ds", ttlSeconds));

    newSession.setBehavior(Session.Behavior.DELETE);

    // Lock delay - prevents rapid re-acquisition after session invalidation (in seconds)
    newSession.setLockDelay(5L);

    Response<String> response = consulClient.sessionCreate(
        newSession,
        buildQueryParams(),
        config.getAuth().getAclToken()
    );
    String sessionId = response.getValue();

    LOGGER.info("Created session [%s...] for leader election, TTL=%ds",
                shortSessionIdWithoutEllipsis(sessionId),
                ttlSeconds);

    return sessionId;
  }

  private boolean tryAcquireLock(String sessionId)
  {
    try {
      String leaderValue = self.getServiceScheme() + "://" + self.getHostAndPortToUse();

      PutParams putParams = new PutParams();
      putParams.setAcquireSession(sessionId);
      Response<Boolean> response = consulClient.setKVValue(
          lockKey,
          leaderValue,
          config.getAuth().getAclToken(),
          putParams,
          buildQueryParams()
      );

      return response != null && Boolean.TRUE.equals(response.getValue());
    }
    catch (Exception e) {
      LOGGER.error(e, "Failed to acquire lock on key [%s]", lockKey);
      return false;
    }
  }

  private void becomeLeader()
  {
    String currentSession = this.sessionId;

    if (currentSession == null || stopping || Thread.currentThread().isInterrupted()) {
      LOGGER.warn("Aborting promotion: session=%s, stopping=%s",
                  currentSession != null ? currentSession.substring(0, Math.min(8, currentSession.length())) + "..." : "null",
                  stopping);
      return;
    }

    if (!validateLockOwnership(currentSession)) {
      LOGGER.warn("Ownership check failed for [%s]", lockKey);
      emitOwnershipMismatchMetric();
      return;
    }

    if (!leader.compareAndSet(false, true)) {
      LOGGER.info("Already leader for [%s]", lockKey);
      return;
    }

    // Re-validate after CAS to handle race conditions
    if (!validateLockOwnership(currentSession)) {
      leader.set(false);
      LOGGER.error("Lost ownership during promotion for [%s]", lockKey);
      emitOwnershipMismatchMetric();
      return;
    }

    int newTerm = term.incrementAndGet();

    try {
      listener.becomeLeader();
      LOGGER.info("Became leader for [%s], term=%d", lockKey, newTerm);
      ConsulMetrics.emitCount(emitter, "consul/leader/become", "lock", lockKey);
    }
    catch (Exception ex) {
      LOGGER.error(ex, "Listener failed during promotion, destroyed session");
      leader.set(false);
      destroySession(currentSession);
      throw ex;
    }
  }

  private void destroySession(String sessionId)
  {
    if (sessionId != null) {
      try {
        consulClient.sessionDestroy(sessionId, buildQueryParams(), config.getAuth().getAclToken());
      }
      catch (Exception e) {
        LOGGER.error(e, "Failed to destroy session [%s]", sessionId);
      }
    }
  }

  private boolean validateLockOwnership(String expectedSessionId)
  {
    try {
      Response<GetValue> response = consulClient.getKVValue(
          lockKey,
          config.getAuth().getAclToken(),
          buildQueryParams()
      );
      if (response == null || response.getValue() == null) {
        LOGGER.warn("Lock key [%s] missing when validating ownership", lockKey);
        return false;
      }
      String actualSessionId = response.getValue().getSession();
      if (actualSessionId == null) {
        LOGGER.warn("Lock key [%s] has no session owner", lockKey);
        return false;
      }
      boolean matches = expectedSessionId.equals(actualSessionId);
      if (!matches) {
        LOGGER.warn(
            "Lock key [%s] owned by session [%s], expected [%s]",
            lockKey,
            actualSessionId,
            expectedSessionId
        );
      }
      return matches;
    }
    catch (Exception e) {
      LOGGER.error(e, "Failed to validate lock ownership for [%s]", lockKey);
      return false;
    }
  }

  private void emitOwnershipMismatchMetric()
  {
    ConsulMetrics.emitCount(
        emitter,
        "consul/leader/ownership_mismatch",
        "lock",
        lockKey
    );
  }

  private void loseLeadership()
  {
    LOGGER.info("Losing leadership for [%s]", lockKey);

    leader.set(false);

    try {
      listener.stopBeingLeader();
      LOGGER.info("Successfully stepped down as leader for [%s]", lockKey);
      ConsulMetrics.emitCount(emitter, "consul/leader/stop", "lock", lockKey);
    }
    catch (Exception ex) {
      LOGGER.error(ex, "listener.stopBeingLeader() failed");
    }
  }

  private QueryParams buildQueryParams()
  {
    if (config.getService().getDatacenter() != null) {
      return new QueryParams(config.getService().getDatacenter());
    }
    return QueryParams.DEFAULT;
  }

  private static String shortSessionId(@Nullable String sessionId)
  {
    if (sessionId == null) {
      return "null";
    }
    if (sessionId.length() <= 8) {
      return sessionId;
    }
    return sessionId.substring(0, 8) + "...";
  }

  private static String shortSessionIdWithoutEllipsis(@Nullable String sessionId)
  {
    if (sessionId == null) {
      return "null";
    }
    if (sessionId.length() <= 8) {
      return sessionId;
    }
    return sessionId.substring(0, 8);
  }
}
