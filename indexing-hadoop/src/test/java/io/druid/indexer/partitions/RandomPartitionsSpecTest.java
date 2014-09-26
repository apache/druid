/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexer.partitions;

import com.google.common.base.Throwables;
import io.druid.indexer.HadoopDruidIndexerConfigTest;
import org.junit.Assert;
import org.junit.Test;

/**
 */
public class RandomPartitionsSpecTest
{
  @Test
  public void testRandomPartitionsSpec() throws Exception
  {
    {
      final PartitionsSpec partitionsSpec;

      try {
        partitionsSpec = HadoopDruidIndexerConfigTest.jsonReadWriteRead(
            "{"
            + "   \"targetPartitionSize\":100,"
            + "   \"type\":\"random\""
            + "}",
            PartitionsSpec.class
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }

      Assert.assertEquals(
          "isDeterminingPartitions",
          partitionsSpec.isDeterminingPartitions(),
          true
      );

      Assert.assertEquals(
          "getTargetPartitionSize",
          partitionsSpec.getTargetPartitionSize(),
          100
      );

      Assert.assertEquals(
          "getMaxPartitionSize",
          partitionsSpec.getMaxPartitionSize(),
          150
      );

      Assert.assertTrue("partitionsSpec", partitionsSpec instanceof RandomPartitionsSpec);
    }
  }
}
