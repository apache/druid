package org.apache.druid.query.context;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ResponseContextTest
{

  @Test
  public void mergeValueTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.merge(ResponseContext.Key.ETAG, "dummy-etag");
    Assert.assertEquals("dummy-etag", ctx.get(ResponseContext.Key.ETAG));
    ctx.merge(ResponseContext.Key.ETAG, "new-dummy-etag");
    Assert.assertEquals("new-dummy-etag", ctx.get(ResponseContext.Key.ETAG));

    final Interval interval01 = new Interval(0L, 1L);
    ctx.merge(ResponseContext.Key.UNCOVERED_INTERVALS, Collections.singletonList(interval01));
    Assert.assertArrayEquals(
        Collections.singletonList(interval01).toArray(),
        ((List) ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS)).toArray()
    );
    final Interval interval12 = new Interval(1L, 2L);
    final Interval interval23 = new Interval(2L, 3L);
    ctx.merge(ResponseContext.Key.UNCOVERED_INTERVALS, Arrays.asList(interval12, interval23));
    Assert.assertArrayEquals(
        Arrays.asList(interval01, interval12, interval23).toArray(),
        ((List) ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS)).toArray()
    );

    final SegmentDescriptor sd01 = new SegmentDescriptor(interval01, "01", 0);
    ctx.merge(ResponseContext.Key.MISSING_SEGMENTS, Collections.singletonList(sd01));
    Assert.assertArrayEquals(
        Collections.singletonList(sd01).toArray(),
        ((List) ctx.get(ResponseContext.Key.MISSING_SEGMENTS)).toArray()
    );
    final SegmentDescriptor sd12 = new SegmentDescriptor(interval12, "12", 1);
    final SegmentDescriptor sd23 = new SegmentDescriptor(interval23, "23", 2);
    ctx.merge(ResponseContext.Key.MISSING_SEGMENTS, Arrays.asList(sd12, sd23));
    Assert.assertArrayEquals(
        Arrays.asList(sd01, sd12, sd23).toArray(),
        ((List) ctx.get(ResponseContext.Key.MISSING_SEGMENTS)).toArray()
    );

    ctx.merge(ResponseContext.Key.COUNT, 0L);
    Assert.assertEquals(0L, ctx.get(ResponseContext.Key.COUNT));
    ctx.merge(ResponseContext.Key.COUNT, 1L);
    Assert.assertEquals(1L, ctx.get(ResponseContext.Key.COUNT));
    ctx.merge(ResponseContext.Key.COUNT, 3L);
    Assert.assertEquals(4L, ctx.get(ResponseContext.Key.COUNT));

    ctx.merge(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED, false);
    Assert.assertEquals(false, ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED));
    ctx.merge(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED, true);
    Assert.assertEquals(true, ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED));
    ctx.merge(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED, false);
    Assert.assertEquals(true, ctx.get(ResponseContext.Key.UNCOVERED_INTERVALS_OVERFLOWED));
  }

  @Test
  public void mergeResponseContextTest()
  {
    final ResponseContext ctx1 = ResponseContext.createEmpty();
    ctx1.put(ResponseContext.Key.ETAG, "dummy-etag-1");
    final Interval interval01 = new Interval(0L, 1L);
    ctx1.put(ResponseContext.Key.UNCOVERED_INTERVALS, Collections.singletonList(interval01));
    ctx1.put(ResponseContext.Key.COUNT, 1L);

    final ResponseContext ctx2 = ResponseContext.createEmpty();
    ctx2.put(ResponseContext.Key.ETAG, "dummy-etag-2");
    final Interval interval12 = new Interval(1L, 2L);
    ctx2.put(ResponseContext.Key.UNCOVERED_INTERVALS, Collections.singletonList(interval12));
    final SegmentDescriptor sd01 = new SegmentDescriptor(interval01, "01", 0);
    ctx2.put(ResponseContext.Key.MISSING_SEGMENTS, Collections.singletonList(sd01));
    ctx2.put(ResponseContext.Key.COUNT, 2L);

    ctx1.merge(ctx2);
    Assert.assertEquals("dummy-etag-2", ctx1.get(ResponseContext.Key.ETAG));
    Assert.assertEquals(3L, ctx1.get(ResponseContext.Key.COUNT));
    Assert.assertArrayEquals(
        Arrays.asList(interval01, interval12).toArray(),
        ((List) ctx1.get(ResponseContext.Key.UNCOVERED_INTERVALS)).toArray()
    );
    Assert.assertArrayEquals(
        Collections.singletonList(sd01).toArray(),
        ((List) ctx1.get(ResponseContext.Key.MISSING_SEGMENTS)).toArray()
    );
  }

  @Test
  public void serializeWith() throws JsonProcessingException
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.put(ResponseContext.Key.COUNT, 100L);
    ctx.put(ResponseContext.Key.ETAG, "long-string-that-is-supposed-to-be-removed-from-result");
    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    final String fullString = objectMapper.writeValueAsString(ctx.getDelegate());
    final ResponseContext.SerializationResult res1 = ctx.serializeWith(objectMapper, 1000);
    Assert.assertEquals(fullString, res1.getResult());
    final ResponseContext reducedCtx = ResponseContext.createEmpty();
    reducedCtx.merge(ctx);
    final ResponseContext.SerializationResult res2 = ctx.serializeWith(objectMapper, 20);
    reducedCtx.remove(ResponseContext.Key.ETAG);
    Assert.assertEquals(objectMapper.writeValueAsString(reducedCtx.getDelegate()), res2.getResult());
  }
}