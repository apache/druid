package org.apache.druid.indexing.pulsar.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.seekablestream.common.OrderedPartitionableRecord;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Collections;
import java.util.List;

public class Fixtures
{
  private static List<ByteEntity> jb(String ts, String dim1, String dim2, String dimLong, String dimFloat,
                                     String met1)
  {
    try {
      return Collections.singletonList(new ByteEntity(new ObjectMapper().writeValueAsBytes(
          ImmutableMap.builder()
              .put("timestamp", ts)
              .put("dim1", dim1)
              .put("dim2", dim2)
              .put("dimLong", dimLong)
              .put("dimFloat", dimFloat)
              .put("met1", met1)
              .build()
      )));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static List<OrderedPartitionableRecord<Integer, Long, ByteEntity>> generateRecords(String stream)
  {
    return ImmutableList.of(
        new OrderedPartitionableRecord<>(stream, 1, 0l, jb("2008", "a", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, 1, 1l, jb("2009", "b", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(stream, 1, 2l, jb("2010", "c", "y", "10", "20.0", "1.0")),
        new OrderedPartitionableRecord<>(
            stream,
            1, 3l,
            jb("246140482-04-24T15:36:27.903Z", "x", "z", "10", "20.0", "1.0")
        ),
        new OrderedPartitionableRecord<>(
            stream,
            1, 4l,
            Collections.singletonList(new ByteEntity(StringUtils.toUtf8("unparseable")))
        ),
        new OrderedPartitionableRecord<>(stream, 1, 5l,
            Collections.singletonList(new ByteEntity(StringUtils.toUtf8("{}"))))
    );

  }
}
