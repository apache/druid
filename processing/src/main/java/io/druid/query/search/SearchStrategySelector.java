package io.druid.query.search;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import io.druid.java.util.common.ISE;
import io.druid.query.search.search.AutoStrategy;
import io.druid.query.search.search.CursorBasedStrategy;
import io.druid.query.search.search.IndexOnlyStrategy;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.search.search.SearchStrategy;

public class SearchStrategySelector
{
  private final SearchQueryConfig config;

  @Inject
  public SearchStrategySelector(Supplier<SearchQueryConfig> configSupplier)
  {
    this.config = configSupplier.get();
  }

  public SearchStrategy strategize(SearchQuery query)
  {
    final String strategyString = config.withOverrides(query).getSearchStrategy();

    switch (strategyString) {
      case AutoStrategy.NAME:
        return new AutoStrategy(query);
      case IndexOnlyStrategy.NAME:
        return new IndexOnlyStrategy(query);
      case CursorBasedStrategy.NAME:
        return new CursorBasedStrategy(query);
      default:
        throw new ISE("Unknown strategy[%s]", strategyString);
    }
  }
}
