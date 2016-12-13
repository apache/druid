/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.storage.s3;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

public class S3DataSegmentArchiverTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper()
      .setInjectableValues(new InjectableValues()
      {
        @Override
        public Object findInjectableValue(
            Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
        )
        {
          return PULLER;
        }
      }).registerModule(new SimpleModule("s3-archive-test-module").registerSubtypes(S3LoadSpec.class));
  private static final S3DataSegmentArchiverConfig ARCHIVER_CONFIG = new S3DataSegmentArchiverConfig()
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
  private static final S3DataSegmentPusherConfig PUSHER_CONFIG = new S3DataSegmentPusherConfig();
  private static final RestS3Service S3_SERVICE = EasyMock.createStrictMock(RestS3Service.class);
  private static final S3DataSegmentPuller PULLER = new S3DataSegmentPuller(S3_SERVICE);
  private static final DataSegment SOURCE_SEGMENT = DataSegment
      .builder()
      .binaryVersion(1)
      .dataSource("dataSource")
      .dimensions(ImmutableList.<String>of())
      .interval(Interval.parse("2015/2016"))
      .version("version")
      .loadSpec(ImmutableMap.<String, Object>of(
          "type",
          S3StorageDruidModule.SCHEME,
          S3DataSegmentPuller.BUCKET,
          "source_bucket",
          S3DataSegmentPuller.KEY,
          "source_key"
      ))
      .build();

  @BeforeClass
  public static void setUpStatic()
  {
    PUSHER_CONFIG.setBaseKey("push_base");
    PUSHER_CONFIG.setBucket("push_bucket");
  }

  @Test
  public void testSimpleArchive() throws Exception
  {
    final DataSegment archivedSegment = SOURCE_SEGMENT
        .withLoadSpec(ImmutableMap.<String, Object>of(
            "type",
            S3StorageDruidModule.SCHEME,
            S3DataSegmentPuller.BUCKET,
            ARCHIVER_CONFIG.getArchiveBucket(),
            S3DataSegmentPuller.KEY,
            ARCHIVER_CONFIG.getArchiveBaseKey() + "archived"
        ));
    final S3DataSegmentArchiver archiver = new S3DataSegmentArchiver(MAPPER, S3_SERVICE, ARCHIVER_CONFIG, PUSHER_CONFIG)
    {
      @Override
      public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException
      {
        return archivedSegment;
      }
    };
    Assert.assertEquals(archivedSegment, archiver.archive(SOURCE_SEGMENT));
  }

  @Test
  public void testSimpleArchiveDoesntMove() throws Exception
  {
    final S3DataSegmentArchiver archiver = new S3DataSegmentArchiver(MAPPER, S3_SERVICE, ARCHIVER_CONFIG, PUSHER_CONFIG)
    {
      @Override
      public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException
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
        .withLoadSpec(ImmutableMap.<String, Object>of(
            "type",
            S3StorageDruidModule.SCHEME,
            S3DataSegmentPuller.BUCKET,
            ARCHIVER_CONFIG.getArchiveBucket(),
            S3DataSegmentPuller.KEY,
            ARCHIVER_CONFIG.getArchiveBaseKey() + "archived"
        ));
    final S3DataSegmentArchiver archiver = new S3DataSegmentArchiver(MAPPER, S3_SERVICE, ARCHIVER_CONFIG, PUSHER_CONFIG)
    {
      @Override
      public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException
      {
        return archivedSegment;
      }
    };
    Assert.assertEquals(archivedSegment, archiver.restore(SOURCE_SEGMENT));
  }

  @Test
  public void testSimpleRestoreDoesntMove() throws Exception
  {
    final S3DataSegmentArchiver archiver = new S3DataSegmentArchiver(MAPPER, S3_SERVICE, ARCHIVER_CONFIG, PUSHER_CONFIG)
    {
      @Override
      public DataSegment move(DataSegment segment, Map<String, Object> targetLoadSpec) throws SegmentLoadingException
      {
        return SOURCE_SEGMENT;
      }
    };
    Assert.assertNull(archiver.restore(SOURCE_SEGMENT));
  }
}
