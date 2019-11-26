package org.apache.druid.indexing.overlord.setup;

public interface CategoriedWorkerSelectStrategy extends WorkerSelectStrategy
{
  WorkerCategorySpec getWorkerCategorySpec();
}
