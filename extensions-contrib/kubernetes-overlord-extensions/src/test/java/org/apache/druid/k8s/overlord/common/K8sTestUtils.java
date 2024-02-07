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

package org.apache.druid.k8s.overlord.common;

import com.google.common.collect.ImmutableList;
import io.fabric8.kubernetes.api.model.PodSpec;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.client.utils.Serialization;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.data.input.impl.NoopInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexing.common.task.IndexTask;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;

import java.io.File;
import java.util.Collections;


public class K8sTestUtils
{

  private static final IndexSpec INDEX_SPEC = IndexSpec.DEFAULT;

  public static PodSpec getDummyPodSpec()
  {
    return new PodSpecBuilder()
        .addNewContainer()
        .withName("pi")
        .withImage("localhost:5000/busybox:stable")
        .withCommand("perl", "-Mbignum=bpi", "-wle", "print bpi(2000)")
        .endContainer()
        .build();
  }

  public static Task getTask()
  {
    return new IndexTask(
        null,
        null,
        new IndexTask.IndexIngestionSpec(
            new DataSchema(
                "foo",
                new TimestampSpec(null, null, null),
                DimensionsSpec.EMPTY,
                new AggregatorFactory[]{new DoubleSumAggregatorFactory("met", "met")},
                new UniformGranularitySpec(
                    Granularities.DAY,
                    null,
                    ImmutableList.of(Intervals.of("2010-01-01/P2D"))
                ),
                null
            ),
            new IndexTask.IndexIOConfig(
                null,
                new LocalInputSource(new File("lol"), "rofl"),
                new NoopInputFormat(),
                true,
                false
            ),
            new IndexTask.IndexTuningConfig(
                null,
                null,
                null,
                10,
                null,
                null,
                null,
                9999,
                null,
                null,
                new DynamicPartitionsSpec(10000, null),
                INDEX_SPEC,
                null,
                3,
                false,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                1L,
                null
            )
        ),
        null
    );
  }

  public static <T> T fileToResource(String contents, Class<T> type)
  {
    return Serialization.unmarshal(
        K8sTestUtils.class.getClassLoader().getResourceAsStream(contents),
        type
    );
  }

  public static NoopTask createTask(String id, int priority)
  {
    return new NoopTask(id, null, null, 0, 0, Collections.singletonMap(Tasks.PRIORITY_KEY, priority));
  }
}
