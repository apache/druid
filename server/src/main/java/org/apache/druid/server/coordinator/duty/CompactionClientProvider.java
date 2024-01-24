package org.apache.druid.server.coordinator.duty;


import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.inject.Provider;

//@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "compactionType")
public interface CompactionClientProvider extends Provider<CompactionClient>
{
}
