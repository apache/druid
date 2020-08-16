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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class OssDataSegmentArchiverTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper()
      .setInjectableValues(
          new InjectableValues()
          {
            @Override
            public Object findInjectableValue(
                Object valueId,
                DeserializationContext ctxt,
                BeanProperty forProperty,
                Object beanInstance
            )
            {
              return PULLER;
            }
          }
      )
      .registerModule(new SimpleModule("aliyun-oss-archive-test-module").registerSubtypes(OssLoadSpec.class));
  private static final OssDataSegmentArchiverConfig ARCHIVER_CONFIG = new OssDataSegmentArchiverConfig()
  {
    @Override
    public String getArchiveBucket()
    {
      return "archive_bucket";
    }

    @Override
    public String getArchiveBaseKey()
    {
      return "archive_base_key";
    }
  };
  private static final OssStorageConfig PUSHER_CONFIG = new OssStorageConfig();
  private static final OSS OSS_CLIENT = EasyMock.createStrictMock(OSSClient.class);
  private static final OssDataSegmentPuller PULLER = new OssDataSegmentPuller(OSS_CLIENT);
  private static final DataSegment SOURCE_SEGMENT = DataSegment
      .builder()
      .binaryVersion(1)
      .dataSource("dataSource")
      .dimensions(ImmutableList.of())
      .interval(Intervals.of("2015/2016"))
      .version("version")
      .loadSpec(ImmutableMap.of(
          "type",
          OssStorageDruidModule.SCHEME_ZIP,
          OssDataSegmentPuller.BUCKET,
          "source_bucket",
          OssDataSegmentPuller.KEY,
          "source_key"
      ))
      .size(0)
      .build();

  @BeforeClass
  public static void setUpStatic()
  {
    PUSHER_CONFIG.setPrefix("push_base");
    PUSHER_CONFIG.setBucket("push_bucket");
  }

  @Test
  public void testSimpleArchive() throws Exception
  {
    final DataSegment archivedSegment = SOURCE_SEGMENT
        .withLoadSpec(ImmutableMap.of(
            "type",
            OssStorageDruidModule.SCHEME_ZIP,
            OssDataSegmentPuller.BUCKET,
            ARCHIVER_CONFIG.getArchiveBucket(),
            OssDataSegmentPuller.KEY,
            ARCHIVER_CONFIG.getArchiveBaseKey() + "archived"
        ));
    final OssDataSegmentArchiver archiver = new OssDataSegmentArchiver(
        MAPPER,
        OSS_CLIENT,
        ARCHIVER_CONFIG,
        PUSHER_CONFIG
    )
    {
      @Override
      public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec)
      {
        return archivedSegment;
      }
    };
    Assert.assertEquals(archivedSegment, archiver.archive(SOURCE_SEGMENT));
  }

  @Test
  public void testSimpleArchiveDoesntMove() throws Exception
  {
    final OssDataSegmentArchiver archiver = new OssDataSegmentArchiver(
        MAPPER,
        OSS_CLIENT,
        ARCHIVER_CONFIG,
        PUSHER_CONFIG
    )
    {
      @Override
      public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec)
      {
        return SOURCE_SEGMENT;
      }
    };
    Assert.assertNull(archiver.archive(SOURCE_SEGMENT));
  }

  @Test
  public void testSimpleRestore() throws Exception
  {
    final DataSegment archivedSegment = SOURCE_SEGMENT
        .withLoadSpec(ImmutableMap.of(
            "type",
            OssStorageDruidModule.SCHEME_ZIP,
            OssDataSegmentPuller.BUCKET,
            ARCHIVER_CONFIG.getArchiveBucket(),
            OssDataSegmentPuller.KEY,
            ARCHIVER_CONFIG.getArchiveBaseKey() + "archived"
        ));
    final OssDataSegmentArchiver archiver = new OssDataSegmentArchiver(
        MAPPER,
        OSS_CLIENT,
        ARCHIVER_CONFIG,
        PUSHER_CONFIG
    )
    {
      @Override
      public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec)
      {
        return archivedSegment;
      }
    };
    Assert.assertEquals(archivedSegment, archiver.restore(SOURCE_SEGMENT));
  }

  @Test
  public void testSimpleRestoreDoesntMove() throws Exception
  {
    final OssDataSegmentArchiver archiver = new OssDataSegmentArchiver(
        MAPPER,
        OSS_CLIENT,
        ARCHIVER_CONFIG,
        PUSHER_CONFIG
    )
    {
      @Override
      public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec)
      {
        return SOURCE_SEGMENT;
      }
    };
    Assert.assertNull(archiver.restore(SOURCE_SEGMENT));
  }
}
