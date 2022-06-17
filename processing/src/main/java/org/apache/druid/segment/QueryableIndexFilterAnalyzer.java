package org.apache.druid.segment;

import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.BitmapColumnIndex;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class QueryableIndexFilterAnalyzer
{
}
