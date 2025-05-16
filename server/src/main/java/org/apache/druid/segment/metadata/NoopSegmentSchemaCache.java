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

package org.apache.druid.segment.metadata;

import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.SegmentMetadata;
import org.apache.druid.timeline.SegmentId;

import java.util.Map;

/**
 * Noop implementation of {@link SegmentSchemaCache}.
 */
public class NoopSegmentSchemaCache extends SegmentSchemaCache
{
  @Override
  public boolean isEnabled()
  {
    return false;
  }

  @Override
  public void setInitialized()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void onLeaderStop()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isInitialized()
  {
    return false;
  }

  @Override
  public void awaitInitialization() throws InterruptedException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void resetSchemaForPublishedSegments(
      Map<SegmentId, SegmentMetadata> usedSegmentIdToMetadata,
      Map<String, SchemaPayload> schemaFingerprintToPayload
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addRealtimeSegmentSchema(SegmentId segmentId, SchemaPayloadPlus schema)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addSchemaPendingBackfill(SegmentId segmentId, SchemaPayloadPlus schema)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void markSchemaPersisted(SegmentId segmentId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<SegmentId, SegmentMetadata> getSegmentMetadataMap()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSchemaCached(SegmentId segmentId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, SchemaPayload> getSchemaPayloadMap()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  SchemaPayloadPlus getTemporaryPublishedMetadataQueryResults(SegmentId id)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void segmentRemoved(SegmentId segmentId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void realtimeSegmentRemoved(SegmentId segmentId)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<String, Integer> getStats()
  {
    throw new UnsupportedOperationException();
  }
}
