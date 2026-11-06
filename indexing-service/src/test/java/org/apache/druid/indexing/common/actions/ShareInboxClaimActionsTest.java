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

package org.apache.druid.indexing.common.actions;

import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.ShareInboxClaimRequest;
import org.apache.druid.indexing.overlord.ShareInboxClaimResult;
import org.apache.druid.indexing.overlord.ShareInboxRenewRequest;
import org.apache.druid.indexing.overlord.ShareInboxRenewResult;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareInboxClaimActionsTest
{
  @Test
  public void testClaimActionSerdeAndPerform() throws Exception
  {
    final ShareInboxClaimRequest request = claimRequest();
    final ClaimShareInboxManifestsAction original = new ClaimShareInboxManifestsAction(request);
    final TaskAction<?> deserialized = TestHelper.JSON_MAPPER.readValue(
        TestHelper.JSON_MAPPER.writeValueAsBytes(original),
        TaskAction.class
    );
    Assertions.assertInstanceOf(ClaimShareInboxManifestsAction.class, deserialized);
    Assertions.assertEquals(
        request.getClaimOwner(),
        ((ClaimShareInboxManifestsAction) deserialized).getRequest().getClaimOwner()
    );

    final Task task = task();
    final TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    final IndexerMetadataStorageCoordinator coordinator = mock(IndexerMetadataStorageCoordinator.class);
    final ShareInboxClaimResult expected = new ShareInboxClaimResult(List.of());
    when(toolbox.getIndexerMetadataStorageCoordinator()).thenReturn(coordinator);
    when(coordinator.claimShareInboxManifests(request)).thenReturn(expected);

    Assertions.assertSame(expected, original.perform(task, toolbox));
    verify(coordinator).claimShareInboxManifests(request);
  }

  @Test
  public void testRenewActionSerdeAndPerform() throws Exception
  {
    final ShareInboxRenewRequest request = renewRequest();
    final RenewShareInboxClaimsAction original = new RenewShareInboxClaimsAction(request);
    final TaskAction<?> deserialized = TestHelper.JSON_MAPPER.readValue(
        TestHelper.JSON_MAPPER.writeValueAsBytes(original),
        TaskAction.class
    );
    Assertions.assertInstanceOf(RenewShareInboxClaimsAction.class, deserialized);
    Assertions.assertEquals(
        request.getClaims(),
        ((RenewShareInboxClaimsAction) deserialized).getRequest().getClaims()
    );

    final Task task = task();
    final TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    final IndexerMetadataStorageCoordinator coordinator = mock(IndexerMetadataStorageCoordinator.class);
    final ShareInboxRenewResult expected = new ShareInboxRenewResult(List.of("manifest"));
    when(toolbox.getIndexerMetadataStorageCoordinator()).thenReturn(coordinator);
    when(coordinator.renewShareInboxClaims(request)).thenReturn(expected);

    Assertions.assertSame(expected, original.perform(task, toolbox));
    verify(coordinator).renewShareInboxClaims(request);
  }

  @Test
  public void testActionsRejectDifferentTaskOwner()
  {
    final ClaimShareInboxManifestsAction action = new ClaimShareInboxManifestsAction(claimRequest());
    final Task task = mock(Task.class);
    final TaskActionToolbox toolbox = mock(TaskActionToolbox.class);
    final IndexerMetadataStorageCoordinator coordinator = mock(IndexerMetadataStorageCoordinator.class);
    when(task.getDataSource()).thenReturn("datasource");
    when(task.getId()).thenReturn("other-owner");
    when(toolbox.getIndexerMetadataStorageCoordinator()).thenReturn(coordinator);

    Assertions.assertThrows(DruidException.class, () -> action.perform(task, toolbox));
    verify(coordinator, never()).claimShareInboxManifests(action.getRequest());
  }

  private static Task task()
  {
    final Task task = mock(Task.class);
    when(task.getDataSource()).thenReturn("datasource");
    when(task.getId()).thenReturn("owner");
    return task;
  }

  private static ShareInboxClaimRequest claimRequest()
  {
    return new ShareInboxClaimRequest(
        "datasource",
        "inbox",
        "fingerprint",
        "owner",
        10,
        100,
        1_000,
        60_000
    );
  }

  private static ShareInboxRenewRequest renewRequest()
  {
    return new ShareInboxRenewRequest(
        "datasource",
        "inbox",
        "fingerprint",
        "owner",
        Map.of("manifest", 1L),
        60_000
    );
  }
}
