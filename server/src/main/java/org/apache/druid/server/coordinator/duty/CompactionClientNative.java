package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.annotation.JacksonInject;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.rpc.indexing.OverlordClient;

public class CompactionClientNative implements CompactionClient
{
  @JacksonInject
  OverlordClient overlordClient;
  @Override
  public String submitCompactionTask(ClientCompactionTaskQuery taskPayload)
  {

    FutureUtils.getUnchecked(overlordClient.runTask(taskPayload.getId(), taskPayload), true);
    return taskPayload.getId();
  }
}
