package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TaskSegmentSchemaUtil
{

  public static SegmentSchemaMetadata getSegmentSchema(File segmentFile, IndexIO indexIO) throws IOException
  {
    final QueryableIndex queryableIndex = indexIO.loadIndex(segmentFile);
    final StorageAdapter storageAdapter = new QueryableIndexStorageAdapter(queryableIndex);
    final RowSignature rowSignature = storageAdapter.getRowSignature();
    final long numRows = storageAdapter.getNumRows();
    final AggregatorFactory[] aggregatorFactories = storageAdapter.getMetadata().getAggregators();
    Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
    if (null != aggregatorFactories) {
      for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
        aggregatorFactoryMap.put(aggregatorFactory.getName(), aggregatorFactory);
      }
    }
    return new SegmentSchemaMetadata(new SchemaPayload(rowSignature, aggregatorFactoryMap), numRows);
  }
}
