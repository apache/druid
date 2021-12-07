package org.apache.druid.segment.data;

import com.fasterxml.jackson.annotation.JsonValue;

import java.io.Serializable;
import java.util.StringJoiner;

public class ComparableArray implements Comparable<ComparableArray>, Serializable
{
  final String[] delegate;

  public ComparableArray(String[] toTranform)
  {
    delegate = toTranform;
  }

  // We only want to set the delegate arrray as part of the serialization
  @JsonValue
  public String[] getDelegate()
  {
    return delegate;
  }

  @Override
  public String toString()
  {
    StringJoiner stringJoiner = new StringJoiner(",");
    for (String object : delegate) {
      stringJoiner.add(object);
    }
    return stringJoiner.toString();
  }

  @Override
  public int compareTo(ComparableArray rhs)
  {
    //noinspection ArrayEquality
    if (this.delegate == rhs.getDelegate()) {
      return 0;
    } else if (delegate.length > rhs.getDelegate().length) {
      return 1;
    } else if (delegate.length < rhs.getDelegate().length) {
      return -1;
    } else {
      for (int i = 0; i < delegate.length; i++) {
        final int cmp = delegate[i].compareTo(rhs.getDelegate()[i]);
        if (cmp == 0) {
          continue;
        }
        return cmp;
      }
      return 0;
    }
  }
}