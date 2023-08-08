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

package org.apache.druid.indexing.seekablestream;

import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;

public class SeekableStreamIndexTaskTuningConfigTest
{
  @Test
  public void testParsingThreadCountDefaultIsOne()
  {
    SeekableStreamIndexTaskTuningConfig config = new ThreadCountSeekableTuningConfig(null);
    Assert.assertEquals(1, config.getParsingThreadCount());
  }

  @Test
  public void testParsingThreadCountZeroIsAvailableProcessors()
  {
    SeekableStreamIndexTaskTuningConfig config = new ThreadCountSeekableTuningConfig(0);
    Assert.assertEquals(Runtime.getRuntime().availableProcessors(), config.getParsingThreadCount());
  }

  @Test
  public void testParsingThreadCountNegativeIsAvailableProcessors()
  {
    SeekableStreamIndexTaskTuningConfig config = new ThreadCountSeekableTuningConfig(-5);
    Assert.assertEquals(Runtime.getRuntime().availableProcessors(), config.getParsingThreadCount());
  }

  @Test
  public void testParsingThreadCountPositiveValueIsPositive()
  {
    SeekableStreamIndexTaskTuningConfig config = new ThreadCountSeekableTuningConfig(5);
    Assert.assertEquals(5, config.getParsingThreadCount());
  }

  static class ThreadCountSeekableTuningConfig extends SeekableStreamIndexTaskTuningConfig
  {
    public ThreadCountSeekableTuningConfig(@Nullable Integer parsingThreadCount)
    {
      super(
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            parsingThreadCount);
    }

    @Override
    public SeekableStreamIndexTaskTuningConfig withBasePersistDirectory(File dir)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public String toString()
    {
      throw new UnsupportedOperationException();
    }
  }
}
