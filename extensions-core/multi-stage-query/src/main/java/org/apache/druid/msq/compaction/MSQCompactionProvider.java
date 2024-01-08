package org.apache.druid.msq.compaction;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.inject.Provider;
import org.apache.druid.server.coordinator.duty.CompactionClient;
import org.apache.druid.server.coordinator.duty.CompactionClientProvider;

@JsonTypeName(MSQCompactionDruidModule.SCHEME)
public class MSQCompactionProvider implements CompactionClientProvider
{
//  @JacksonInject
//  ClientCompactionTaskQuery compactionTaskQuery;
  @Override
  public CompactionClient get(){
    return new MSQCompaction();
  }
}
