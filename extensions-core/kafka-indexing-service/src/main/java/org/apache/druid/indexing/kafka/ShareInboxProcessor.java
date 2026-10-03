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
import org.apache.druid.data.input.kafka.KafkaRecordEntity;
import org.apache.druid.data.input.kafka.KafkaTopicPartition;
import org.apache.druid.indexing.common.actions.ClaimShareInboxManifestsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.ShareInboxClaimRequest;
import org.apache.druid.indexing.overlord.ShareInboxClaimResult;
import org.apache.druid.indexing.overlord.ShareInboxManifest;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

final class ShareInboxProcessor
{
  private final TaskActionClient taskActionClient;
  private final ShareInboxRecordSource recordSource;
  private final ShareInboxBatchHandler batchHandler;
  private final ScheduledExecutorService claimRenewalExecutor;
  private final ShareInboxClaimRequest claimRequest;
  private final long renewalPeriodMillis;
  private final long pollPeriodMillis;
  private final AtomicBoolean stopRequested = new AtomicBoolean(false);
  private final AtomicReference<ShareInboxClaimLease> activeLease = new AtomicReference<>();
  private final Object pollWaitMonitor = new Object();

  ShareInboxProcessor(
      TaskActionClient taskActionClient,
      ShareInboxRecordSource recordSource,
      ShareInboxBatchHandler batchHandler,
      ScheduledExecutorService claimRenewalExecutor,
      String dataSource,
      String inboxId,
      String specFingerprint,
      String taskId,
      int maxManifests,
      int maxRecords,
      long maxBytes,
      long claimDurationMillis,
      long renewalPeriodMillis,
      long pollPeriodMillis
  )
  {
    this.taskActionClient = Preconditions.checkNotNull(taskActionClient, "taskActionClient");
    this.recordSource = Preconditions.checkNotNull(recordSource, "recordSource");
    this.batchHandler = Preconditions.checkNotNull(batchHandler, "batchHandler");
    this.claimRenewalExecutor = Preconditions.checkNotNull(claimRenewalExecutor, "claimRenewalExecutor");
    this.claimRequest = new ShareInboxClaimRequest(
        dataSource,
        inboxId,
        specFingerprint,
        taskId,
        maxManifests,
        maxRecords,
        maxBytes,
        claimDurationMillis
    );
    Preconditions.checkArgument(renewalPeriodMillis > 0, "renewalPeriodMillis must be positive");
    Preconditions.checkArgument(
        renewalPeriodMillis < claimDurationMillis,
        "renewalPeriodMillis must be less than claimDurationMillis"
    );
    Preconditions.checkArgument(pollPeriodMillis > 0, "pollPeriodMillis must be positive");
    this.renewalPeriodMillis = renewalPeriodMillis;
    this.pollPeriodMillis = pollPeriodMillis;
  }

  void run() throws Exception
  {
    while (!stopRequested.get()) {
      try {
        if (!processNext()) {
          waitForWork();
        }
      }
      catch (Exception e) {
        if (!stopRequested.get()) {
          throw e;
        }
      }
    }
  }

  boolean processNext() throws Exception
  {
    if (stopRequested.get()) {
      return false;
    }
    final ShareInboxClaimResult claimResult = taskActionClient.submit(
        new ClaimShareInboxManifestsAction(claimRequest)
    );
    if (claimResult == null) {
      throw new ISE("Share inbox claim action returned no result");
    }
    final List<ShareInboxManifest> manifests = claimResult.getManifests();
    if (manifests.isEmpty()) {
      return false;
    }
    if (stopRequested.get()) {
      return false;
    }

    final Map<String, Long> claims = validateClaims(manifests);
    final ShareInboxClaimLease claimLease = new ShareInboxClaimLease(
        taskActionClient,
        claimRequest.getDataSource(),
        claimRequest.getInboxId(),
        claimRequest.getSpecFingerprint(),
        claimRequest.getClaimOwner(),
        claims,
        claimRequest.getClaimDurationMillis()
    );
    if (!activeLease.compareAndSet(null, claimLease)) {
      throw new ISE("Another share inbox claim lease is already active");
    }
    try {
      claimLease.start(claimRenewalExecutor, renewalPeriodMillis);
      final List<OrderedPartitionableRecord<KafkaTopicPartition, Long, KafkaRecordEntity>> records =
          new ArrayList<>();
      for (ShareInboxManifest manifest : manifests) {
        claimLease.assertValid();
        records.addAll(recordSource.read(manifest));
      }
      claimLease.assertValid();
      batchHandler.process(manifests, records, claimLease);
      return true;
    }
    finally {
      activeLease.compareAndSet(claimLease, null);
      claimLease.close();
    }
  }

  void requestStop()
  {
    synchronized (pollWaitMonitor) {
      stopRequested.set(true);
      pollWaitMonitor.notifyAll();
    }
    final ShareInboxClaimLease claimLease = activeLease.get();
    if (claimLease != null) {
      claimLease.invalidate(new InterruptedException("Share inbox processor stopping"));
    }
  }

  private Map<String, Long> validateClaims(List<ShareInboxManifest> manifests)
  {
    final Map<String, Long> claims = new TreeMap<>();
    for (ShareInboxManifest manifest : manifests) {
      if (!claimRequest.getDataSource().equals(manifest.getDataSource())
          || !claimRequest.getInboxId().equals(manifest.getInboxId())
          || !claimRequest.getSpecFingerprint().equals(manifest.getSpecFingerprint())
          || !claimRequest.getClaimOwner().equals(manifest.getClaimOwner())) {
        throw new ISE("Share inbox claim action returned a manifest outside the requested generation");
      }
      if (claims.put(manifest.getManifestId(), manifest.getClaimEpoch()) != null) {
        throw new ISE("Share inbox claim action returned duplicate manifest[%s]", manifest.getManifestId());
      }
    }
    return claims;
  }

  private void waitForWork() throws InterruptedException
  {
    synchronized (pollWaitMonitor) {
      if (!stopRequested.get()) {
        pollWaitMonitor.wait(pollPeriodMillis);
      }
    }
  }
}
