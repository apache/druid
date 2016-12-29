package io.druid.query.search;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.java.util.common.ISE;
import io.druid.query.search.search.AutoStrategy;
import io.druid.query.search.search.CursorBasedStrategy;
import io.druid.query.search.search.IndexOnlyStrategy;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import io.druid.query.search.search.SearchStrategy;

public class SearchStrategySelector
{
  private static final EmittingLogger log = new EmittingLogger(SearchStrategySelector.class);
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
        log.debug("Auto strategy is selected");
        return new AutoStrategy(query);
      case IndexOnlyStrategy.NAME:
        log.debug("Index-only execution strategy is selected");
        return new IndexOnlyStrategy(query);
      case CursorBasedStrategy.NAME:
        log.debug("Cursor-based execution strategy is selected");
        return new CursorBasedStrategy(query);
      default:
        throw new ISE("Unknown strategy[%s]", strategyString);
    }
  }
}
