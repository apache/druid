package org.apache.druid.client.cache; 

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ForwardingListeningExecutorService;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.client.CacheUtil;
import org.apache.druid.client.CachingClusteredClientTestUtils;
import org.apache.druid.client.CachingQueryRunner;
import org.apache.druid.client.CachingQueryRunnerTest;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNResultValue;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** 
* BackgroundCachePopulator Tester. 
* 
* @author <Authors name> 
* @since <pre>06/19/2020</pre> 
* @version 1.0 
*/ 
public class BackgroundCachePopulatorTest {
	private static final ObjectMapper JSON_MAPPER = CachingClusteredClientTestUtils.createObjectMapper();
	private static final Object[] OBJECTS = new Object[]{
			DateTimes.of("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
			DateTimes.of("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
			DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
			DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
			DateTimes.of("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
	};
	private static final List<AggregatorFactory> AGGS = Arrays.asList(
			new CountAggregatorFactory("rows"),
			new LongSumAggregatorFactory("imps", "imps"),
			new LongSumAggregatorFactory("impers", "imps")
	);
	private BackgroundCachePopulator backgroundCachePopulator;
	private QueryToolChest toolchest;
	private Cache cache;
	private Query query;
	private QueryRunner baseRunner;
	private AssertingClosable closable;

	@Before
	public void before() throws Exception {

		this.backgroundCachePopulator = new BackgroundCachePopulator(
				Execs.multiThreaded(2, "CachingQueryRunnerTest-%d"),
				JSON_MAPPER,
				new CachePopulatorStats(),
				-1
		);


		TopNQueryBuilder builder = new TopNQueryBuilder()
				.dataSource("ds")
				.dimension("top_dim")
				.metric("imps")
				.threshold(3)
				.intervals("2011-01-05/2011-01-10")
				.aggregators(AGGS)
				.granularity(Granularities.ALL);

		this.query = builder.build();
		this.toolchest = new TopNQueryQueryToolChest(new TopNQueryConfig());
		List<Result> expectedRes = makeTopNResults(false, OBJECTS);

		this.closable = new AssertingClosable();
		final Sequence resultSeq = Sequences.wrap(
				Sequences.simple(expectedRes),
				new SequenceWrapper()
				{
					@Override
					public void before()
					{
						Assert.assertFalse(closable.isClosed());
					}

					@Override
					public void after(boolean isDone, Throwable thrown)
					{
						closable.close();
					}
				}
		);
		this.baseRunner = (queryPlus, responseContext) -> resultSeq;

		this.cache = new Cache()
		{
			private final ConcurrentMap<NamedKey, byte[]> baseMap = new ConcurrentHashMap<>();

			@Override
			public byte[] get(NamedKey key)
			{
				return baseMap.get(key);
			}

			@Override
			public void put(NamedKey key, byte[] value)
			{
				baseMap.put(key, value);
			}

			@Override
			public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
			{
				return null;
			}

			@Override
			public void close(String namespace)
			{
			}

			@Override
			public void close()
			{
			}

			@Override
			public CacheStats getStats()
			{
				return null;
			}

			@Override
			public boolean isLocal()
			{
				return true;
			}

			@Override
			public void doMonitor(ServiceEmitter emitter)
			{
			}
		};
	}

	/**
	*
	* Method: wrap(final Sequence<T> sequence, final Function<T, CacheType> cacheFn, final Cache cache, final Cache.NamedKey cacheKey)
	*
	*/
	@Test
	public void testWrap() throws Exception {


		String cacheId = "segment";
		SegmentDescriptor segmentDescriptor = new SegmentDescriptor(Intervals.of("2011/2012"), "version", 0);


		CacheStrategy cacheStrategy = toolchest.getCacheStrategy(query);
		Cache.NamedKey cacheKey = CacheUtil.computeSegmentCacheKey(
				cacheId,
				segmentDescriptor,
				cacheStrategy.computeCacheKey(query)
		);

		Sequence res = this.backgroundCachePopulator.wrap(this.baseRunner.run(QueryPlus.wrap(query),  ResponseContext.createEmpty()),
				(value) -> cacheStrategy.prepareForSegmentLevelCache().apply(value), cache, cacheKey);
		Assert.assertFalse("sequence must not be closed", closable.isClosed());
		Assert.assertNull("cache must be empty", cache.get(cacheKey));

		List results = res.toList();
		Assert.assertTrue(closable.isClosed());
		List<Result> expectedRes = makeTopNResults(false, OBJECTS);
		Assert.assertEquals(expectedRes.toString(), results.toString());
		Assert.assertEquals(5, results.size());
	}

	private List<Result> makeTopNResults(boolean cachedResults, Object... objects)
	{
		List<Result> retVal = new ArrayList<>();
		int index = 0;
		while (index < objects.length) {
			DateTime timestamp = (DateTime) objects[index++];

			List<Map<String, Object>> values = new ArrayList<>();
			while (index < objects.length && !(objects[index] instanceof DateTime)) {
				if (objects.length - index < 3) {
					throw new ISE(
							"expect 3 values for each entry in the top list, had %d values left.", objects.length - index
					);
				}
				final double imps = ((Number) objects[index + 2]).doubleValue();
				final double rows = ((Number) objects[index + 1]).doubleValue();

				if (cachedResults) {
					values.add(
							ImmutableMap.of(
									"top_dim", objects[index],
									"rows", rows,
									"imps", imps,
									"impers", imps
							)
					);
				} else {
					values.add(
							ImmutableMap.of(
									"top_dim", objects[index],
									"rows", rows,
									"imps", imps,
									"impers", imps,
									"avg_imps_per_row", imps / rows
							)
					);
				}
				index += 3;
			}

			retVal.add(new Result<>(timestamp, new TopNResultValue(values)));
		}
		return retVal;
	}

	private static class AssertingClosable implements Closeable
	{

		private final AtomicBoolean closed = new AtomicBoolean(false);

		@Override
		public void close()
		{
			Assert.assertFalse(closed.get());
			Assert.assertTrue(closed.compareAndSet(false, true));
		}

		public boolean isClosed()
		{
			return closed.get();
		}
	}


} 
