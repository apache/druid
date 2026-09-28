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

package org.apache.druid.indexing.kafka;

import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.actions.RenewShareInboxClaimsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.ShareInboxRenewRequest;
import org.apache.druid.indexing.overlord.ShareInboxRenewResult;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

final class ShareInboxClaimLease implements Closeable
{
  private final TaskActionClient taskActionClient;
  private final ShareInboxRenewRequest renewRequest;
  private final Set<String> manifestIds;
  private final AtomicBoolean valid = new AtomicBoolean(true);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final AtomicReference<Throwable> failure = new AtomicReference<>();
  private final AtomicReference<ScheduledFuture<?>> renewalFuture = new AtomicReference<>();

  ShareInboxClaimLease(
      TaskActionClient taskActionClient,
      String dataSource,
      String inboxId,
      String specFingerprint,
      String claimOwner,
      Map<String, Long> claims,
      long claimDurationMillis
  )
  {
    this.taskActionClient = Preconditions.checkNotNull(taskActionClient, "taskActionClient");
    final Map<String, Long> sortedClaims = new TreeMap<>(Preconditions.checkNotNull(claims, "claims"));
    this.renewRequest = new ShareInboxRenewRequest(
        dataSource,
        inboxId,
        specFingerprint,
        claimOwner,
        sortedClaims,
        claimDurationMillis
    );
    this.manifestIds = Set.copyOf(sortedClaims.keySet());
  }

  void start(ScheduledExecutorService scheduler, long renewalPeriodMillis)
  {
    Preconditions.checkNotNull(scheduler, "scheduler");
    Preconditions.checkArgument(renewalPeriodMillis > 0, "renewalPeriodMillis must be positive");
    Preconditions.checkState(!closed.get(), "claim lease already closed");
    Preconditions.checkState(renewalFuture.get() == null, "claim lease already started");
    final ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(
        this::renewNow,
        renewalPeriodMillis,
        renewalPeriodMillis,
        TimeUnit.MILLISECONDS
    );
    if (!renewalFuture.compareAndSet(null, future)) {
      future.cancel(false);
      throw new IllegalStateException("claim lease already started");
    }
  }

  void assertValid()
  {
    if (!valid.get()) {
      final Throwable cause = failure.get();
      if (cause == null) {
        throw new ISE("Share inbox claim lease was lost");
      }
      throw new ISE(cause, "Share inbox claim lease was lost");
    }
  }

  boolean isValid()
  {
    return valid.get();
  }

  void invalidate(Throwable cause)
  {
    failure.compareAndSet(null, cause);
    valid.set(false);
  }

  void renewNow()
  {
    if (closed.get() || !valid.get()) {
      return;
    }
    try {
      final ShareInboxRenewResult result = taskActionClient.submit(new RenewShareInboxClaimsAction(renewRequest));
      if (!matchesAllClaims(result)) {
        valid.set(false);
      }
    }
    catch (Exception e) {
      invalidate(e);
    }
  }

  private boolean matchesAllClaims(@Nullable ShareInboxRenewResult result)
  {
    if (result == null || result.getRenewedManifestIds().size() != manifestIds.size()) {
      return false;
    }
    return new HashSet<>(result.getRenewedManifestIds()).equals(manifestIds);
  }

  @Override
  public void close()
  {
    closed.set(true);
    final ScheduledFuture<?> future = renewalFuture.get();
    if (future != null) {
      future.cancel(false);
    }
  }
}
