/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.loading;

import org.joda.time.format.ISODateTimeFormat;

import com.google.common.base.Joiner;
import com.metamx.druid.client.DataSegment;

/**
 */
public class DataSegmentPusherUtil
{
	private static final Joiner JOINER = Joiner.on("/").skipNulls();

	public static String getStorageDir(DataSegment segment)
	{
		return JOINER.join(
		    segment.getDataSource(),
		    String.format(
		        "%s_%s",
		        segment.getInterval().getStart(),
		        segment.getInterval().getEnd()
		        ),
		    segment.getVersion(),
		    segment.getShardSpec().getPartitionNum()
		    );
	}

	/**
	 * Due to https://issues.apache.org/jira/browse/HDFS-13 ":" are not allowed in
	 * path names. So we format paths differently for HDFS.
	 */
	public static String getHdfsStorageDir(DataSegment segment)
	{
		return JOINER.join(
		    segment.getDataSource(),
		    String.format(
		        "%s_%s",
		        segment.getInterval().getStart().toString(ISODateTimeFormat.basicDateTime()),
		        segment.getInterval().getEnd().toString(ISODateTimeFormat.basicDateTime())
		        ),
		    segment.getVersion().replaceAll(":", "_"),
		    segment.getShardSpec().getPartitionNum()
		    );
	}
}
