package com.metamx.druid.query.group;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 */
public class DefaultLimitSpec implements LimitSpec
{
  private static final byte CACHE_TYPE_ID = 0x0;
  private static Joiner JOINER = Joiner.on("");

  private final List<String> orderBy;
  private final int limit;

  @JsonCreator
  public DefaultLimitSpec(
      @JsonProperty("orderBy") List<String> orderBy,
      @JsonProperty("limit") int limit
  )
  {
    this.orderBy = (orderBy == null) ? Lists.<String>newArrayList() : orderBy;
    this.limit = limit;
  }

  public DefaultLimitSpec()
  {
    this.orderBy = Lists.newArrayList();
    this.limit = 0;
  }

  @JsonProperty
  @Override
  public List<String> getOrderBy()
  {
    return orderBy;
  }

  @JsonProperty
  @Override
  public int getLimit()
  {
    return limit;
  }

  @Override
  public Comparator getComparator()
  {
    return null;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] orderByBytes = JOINER.join(orderBy).getBytes();

    byte[] limitBytes = Ints.toByteArray(limit);

    return ByteBuffer.allocate(1 + orderByBytes.length + limitBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(orderByBytes)
                     .put(limitBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "DefaultLimitSpec{" +
           "orderBy='" + orderBy + '\'' +
           ", limit=" + limit +
           '}';
  }
}
