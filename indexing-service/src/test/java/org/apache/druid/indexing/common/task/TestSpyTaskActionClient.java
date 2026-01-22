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

package org.apache.druid.indexing.common.task;

import org.apache.druid.indexing.common.actions.SegmentTransactionalAppendAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.SegmentTransactionalReplaceAction;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.timeline.DataSegment;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Test utility that wraps a {@link TaskActionClient} to track published segments and their schema mappings.
 * <p>
 * This spy intercepts {@link SegmentTransactionalInsertAction}, {@link SegmentTransactionalReplaceAction},
 * and {@link SegmentTransactionalAppendAction} submissions to collect the segments and schema mappings being
 * published. All other task actions are delegated to the wrapped client without modification.
 * <p>
 * Useful for verifying that tasks publish the expected segments and schemas in integration tests.
 */
public class TestSpyTaskActionClient implements TaskActionClient
{
  /**
   * Set of all segments published through {@link SegmentTransactionalInsertAction},
   * {@link SegmentTransactionalReplaceAction}, or {@link SegmentTransactionalAppendAction} submissions.
   */
  private final Set<DataSegment> publishedSegments = new HashSet<>();

  /**
   * Accumulated schema mapping for all published segments, merged from transactional actions.
   */
  private final SegmentSchemaMapping segmentSchemaMapping
      = new SegmentSchemaMapping(CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

  /**
   * The underlying task action client that handles actual task action submissions.
   */
  private final TaskActionClient client;

  /**
   * Creates a new spy wrapper around the provided task action client.
   *
   * @param client the task action client to wrap and spy on
   */
  TestSpyTaskActionClient(TaskActionClient client)
  {
    this.client = client;
  }

  /**
   * Submits a task action to the wrapped client and tracks published segments and schemas.
   * <p>
   * When the action is a {@link SegmentTransactionalInsertAction}, {@link SegmentTransactionalReplaceAction},
   * or {@link SegmentTransactionalAppendAction}, this method extracts and stores the published segments and merges
   * their schema mappings for later verification. All other actions are delegated without side effects.
   *
   * @param taskAction the task action to submit
   * @param <RetType>  the return type of the task action
   * @return the result from the wrapped client
   * @throws IOException if the wrapped client throws an exception
   */
  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    if (taskAction instanceof SegmentTransactionalInsertAction) {
      SegmentTransactionalInsertAction insertAction = (SegmentTransactionalInsertAction) taskAction;
      publishedSegments.addAll(insertAction.getSegments());
      if (insertAction.getSegmentSchemaMapping() != null) {
        segmentSchemaMapping.merge(insertAction.getSegmentSchemaMapping());
      }
    } else if (taskAction instanceof SegmentTransactionalReplaceAction) {
      SegmentTransactionalReplaceAction replaceAction = (SegmentTransactionalReplaceAction) taskAction;
      publishedSegments.addAll(replaceAction.getSegments());
      if (replaceAction.getSegmentSchemaMapping() != null) {
        segmentSchemaMapping.merge(replaceAction.getSegmentSchemaMapping());
      }
    } else if (taskAction instanceof SegmentTransactionalAppendAction) {
      SegmentTransactionalAppendAction appendAction = (SegmentTransactionalAppendAction) taskAction;
      publishedSegments.addAll(appendAction.getSegments());
      if (appendAction.getSegmentSchemaMapping() != null) {
        segmentSchemaMapping.merge(appendAction.getSegmentSchemaMapping());
      }
    }
    return client.submit(taskAction);
  }

  /**
   * Returns all segments published through transactional insert, replace, or append actions.
   *
   * @return the set of published segments collected by this spy
   */
  public Set<DataSegment> getPublishedSegments()
  {
    return publishedSegments;
  }

  /**
   * Returns the merged schema mapping for all published segments.
   *
   * @return the accumulated segment schema mapping
   */
  public SegmentSchemaMapping getSegmentSchemas()
  {
    return segmentSchemaMapping;
  }
}
