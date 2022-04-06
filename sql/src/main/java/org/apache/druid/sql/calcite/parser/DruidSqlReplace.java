/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.sql.calcite.parser;

import com.google.common.base.Preconditions;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.Interval;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

/**
 * Extends the 'replace' call to hold custom parameters specific to Druid i.e. PARTITIONED BY and the PARTITION SPECS
 * This class extends the {@link SqlInsert} so that this SqlNode can be used in
 * {@link org.apache.calcite.sql2rel.SqlToRelConverter} for getting converted into RelNode, and further processing
 */
public class DruidSqlReplace extends SqlInsert
{
  public static final String SQL_REPLACE_TIME_CHUNKS = "sqlReplaceTimeChunks";

  public static final SqlOperator OPERATOR = SqlInsert.OPERATOR;

  private final Granularity partitionedBy;
  private final String partitionedByStringForUnparse;

  private final List<String> replaceTimeChunks;

  /**
   * While partitionedBy and partitionedByStringForUnparse can be null as arguments to the constructor, this is
   * disallowed (semantically) and the constructor performs checks to ensure that. This helps in producing friendly
   * errors when the PARTITIONED BY custom clause is not present, and keeps its error separate from JavaCC/Calcite's
   * custom errors which can be cryptic when someone accidentally forgets to explicitly specify the PARTITIONED BY clause
   */
  public DruidSqlReplace(
      @Nonnull SqlInsert insertNode,
      @Nullable Granularity partitionedBy,
      @Nullable String partitionedByStringForUnparse,
      @Nonnull List<String> partitionSpecs
  ) throws ParseException
  {
    super(
        insertNode.getParserPosition(),
        (SqlNodeList) insertNode.getOperandList().get(0), // No better getter to extract this
        insertNode.getTargetTable(),
        insertNode.getSource(),
        insertNode.getTargetColumnList()
    );
    if (partitionedBy == null) {
      throw new ParseException("REPLACE statements must specify PARTITIONED BY clause explictly");
    }
    this.partitionedBy = partitionedBy;

    this.partitionedByStringForUnparse = Preconditions.checkNotNull(partitionedByStringForUnparse);

    // Partition Spec validation

    if (partitionSpecs.size() == 1 && "all".equals(partitionSpecs.get(0))) {
      // If the partition spec is set to replace the whole datasource, no further validation for aligning the
      // partitions needs to be done.
      this.replaceTimeChunks = partitionSpecs;
    } else {

      // Validate that the query is not partitioned by ALL.
      if (!(partitionedBy instanceof PeriodGranularity)) {
        throw new ParseException("FOR must only contain ALL TIME if it is PARTITIONED BY ALL granularity");
      }

      List<String> replaceTimeChunks = new ArrayList<>();

      for (String partitionSpec : partitionSpecs) {
        Interval interval;

        if (partitionSpec.contains("/")) {
          // It must be a partition interval
          interval = Intervals.of(partitionSpec);
        } else {
          // It must be an SQL timestamp.
          String timestamp = Timestamp.valueOf(partitionSpec)
                                      .toLocalDateTime()
                                      .toInstant(ZoneOffset.UTC)
                                      .toString();
          String period = ((PeriodGranularity) partitionedBy).getPeriod().toString();
          interval = Intervals.of(timestamp + "/" + period);
        }

        if (!partitionedBy.isAligned(interval)) {
          throw new ParseException("FOR contains a partitionSpec which is not aligned with PARTITIONED BY granularity");
        }
        replaceTimeChunks.add(interval.toString());
      }

      this.replaceTimeChunks = replaceTimeChunks;
    }
  }

  public List<String> getReplaceTimeChunks()
  {
    return replaceTimeChunks;
  }

  public Granularity getPartitionedBy()
  {
    return partitionedBy;
  }

  @Nonnull
  @Override
  public SqlOperator getOperator()
  {
    return OPERATOR;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.startList(SqlWriter.FrameTypeEnum.SELECT);
    writer.sep("REPLACE INTO");
    final int opLeft = getOperator().getLeftPrec();
    final int opRight = getOperator().getRightPrec();
    getTargetTable().unparse(writer, opLeft, opRight);

    writer.keyword("FOR");
    List<String> replaceTimeChunks = getReplaceTimeChunks();
    if (replaceTimeChunks.size() == 1) {
      unparseTimeChunk(writer, replaceTimeChunks.get(0));
    } else {
      final SqlWriter.Frame frame = writer.startList("(", ")");
      for (String replaceTimeChunk : replaceTimeChunks) {
        writer.sep(","); // sep() takes care of not printing the first separator
        unparseTimeChunk(writer, replaceTimeChunk);
      }
      writer.endList(frame);
    }

    if (getTargetColumnList() != null) {
      getTargetColumnList().unparse(writer, opLeft, opRight);
    }
    writer.newlineAndIndent();
    getSource().unparse(writer, 0, 0);
    writer.keyword("PARTITIONED BY");
    writer.keyword(partitionedByStringForUnparse);
  }

  private void unparseTimeChunk(SqlWriter sqlWriter, String timeChunkString)
  {
    if ("all".equalsIgnoreCase(timeChunkString)) {
      sqlWriter.keyword("ALL TIME");
    } else {
      sqlWriter.keyword("PARTITION");
      sqlWriter.literal(timeChunkString);
    }
  }
}
