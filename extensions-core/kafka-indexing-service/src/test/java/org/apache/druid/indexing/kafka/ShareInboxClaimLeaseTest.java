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

import org.apache.druid.indexing.common.actions.RenewShareInboxClaimsAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.overlord.ShareInboxRenewResult;
import org.apache.druid.java.util.common.ISE;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ShareInboxClaimLeaseTest
{
  @Test
  public void testExactRenewalKeepsLeaseValid() throws Exception
  {
    final TaskActionClient actionClient = mock(TaskActionClient.class);
    when(actionClient.submit(org.mockito.ArgumentMatchers.any(RenewShareInboxClaimsAction.class)))
        .thenReturn(new ShareInboxRenewResult(List.of("manifest-1", "manifest-2")));
    final ShareInboxClaimLease lease = lease(actionClient);

    lease.renewNow();

    lease.assertValid();
    final ArgumentCaptor<RenewShareInboxClaimsAction> captor =
        ArgumentCaptor.forClass(RenewShareInboxClaimsAction.class);
    verify(actionClient).submit(captor.capture());
    Assertions.assertEquals(Map.of("manifest-1", 1L, "manifest-2", 2L), captor.getValue().getRequest().getClaims());
  }

  @Test
  public void testPartialRenewalLosesLease() throws Exception
  {
    final TaskActionClient actionClient = mock(TaskActionClient.class);
    when(actionClient.submit(org.mockito.ArgumentMatchers.any(RenewShareInboxClaimsAction.class)))
        .thenReturn(new ShareInboxRenewResult(List.of("manifest-1")));
    final ShareInboxClaimLease lease = lease(actionClient);

    lease.renewNow();

    Assertions.assertFalse(lease.isValid());
    Assertions.assertThrows(ISE.class, lease::assertValid);
  }

  @Test
  public void testRenewalFailureLosesLease() throws Exception
  {
    final TaskActionClient actionClient = mock(TaskActionClient.class);
    when(actionClient.submit(org.mockito.ArgumentMatchers.any(RenewShareInboxClaimsAction.class)))
        .thenThrow(new IOException("overlord unavailable"));
    final ShareInboxClaimLease lease = lease(actionClient);

    lease.renewNow();

    final ISE exception = Assertions.assertThrows(ISE.class, lease::assertValid);
    Assertions.assertInstanceOf(IOException.class, exception.getCause());
  }

  private static ShareInboxClaimLease lease(TaskActionClient actionClient)
  {
    return new ShareInboxClaimLease(
        actionClient,
        "datasource",
        "inbox",
        "fingerprint",
        "task-1",
        Map.of("manifest-1", 1L, "manifest-2", 2L),
        60_000
    );
  }
}
