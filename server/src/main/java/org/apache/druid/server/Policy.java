package org.apache.druid.server;

import org.apache.druid.query.filter.DimFilter;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Optional;

public class Policy
{
  public static final Policy NO_RESTRICTION = new Policy(null);

  private final Optional<DimFilter> rowFilter;

  public Policy(@Nullable DimFilter rowFilter) {
    this.rowFilter = Optional.ofNullable(rowFilter);
  }

  public static Policy fromRowFilter(@Nonnull DimFilter rowFilter) {
    return new Policy(rowFilter);
  }

  public Optional<DimFilter> getRowFilter() {
    return rowFilter;
  }
}
