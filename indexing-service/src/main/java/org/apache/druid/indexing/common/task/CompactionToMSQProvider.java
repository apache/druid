package org.apache.druid.indexing.common.task;


import com.google.inject.Provider;

//@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "compactionType")
public interface CompactionToMSQProvider extends Provider<CompactionToMSQ>
{
}
