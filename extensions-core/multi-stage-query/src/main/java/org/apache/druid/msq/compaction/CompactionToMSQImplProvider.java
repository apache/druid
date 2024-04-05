package org.apache.druid.msq.compaction;

import org.apache.druid.indexing.common.task.CompactionToMSQ;
import org.apache.druid.indexing.common.task.CompactionToMSQProvider;

//@JsonTypeName(MSQCompactionDruidModule.SCHEME)
public class CompactionToMSQImplProvider implements CompactionToMSQProvider
{
//  @JacksonInject
//  ClientCompactionTaskQuery compactionTaskQuery;
  @Override
  public CompactionToMSQ get(){
    return new CompactionToMSQImpl();
  }
}
