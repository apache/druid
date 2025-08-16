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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.output.OutputDestination;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.http.ClientSqlQuery;
import org.apache.druid.server.compaction.CompactionCandidate;
import org.apache.druid.server.compaction.CompactionSlotManager;
import org.apache.druid.server.compaction.DataSourceCompactibleSegmentIterator;
import org.apache.druid.server.coordinator.duty.CompactSegments;
import org.joda.time.Interval;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Compaction template that creates MSQ SQL jobs using a SQL template. The
 * template provided by can contain template variables of the format
 * {@code ${variableName}} for fields such as datasource name and interval start
 * and end.
 * <p>
 * Compaction is triggered for an interval only if the current compaction state
 * of the underlying segments does not match with the {@link #stateMatcher}.
 */
public class MSQCompactionJobTemplate extends CompactionJobTemplate
{
  public static final String TYPE = "compactMsq";

  public static final String VAR_DATASOURCE = "${dataSource}";
  public static final String VAR_START_TIMESTAMP = "${startTimestamp}";
  public static final String VAR_END_TIMESTAMP = "${endTimestamp}";

  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");

  private final ClientSqlQuery sqlTemplate;
  private final CompactionStateMatcher stateMatcher;

  @JsonCreator
  public MSQCompactionJobTemplate(
      @JsonProperty("sqlTemplate") ClientSqlQuery sqlTemplate,
      @JsonProperty("stateMatcher") CompactionStateMatcher stateMatcher
  )
  {
    this.sqlTemplate = sqlTemplate;
    this.stateMatcher = stateMatcher;
  }

  @JsonProperty
  public ClientSqlQuery getSqlTemplate()
  {
    return sqlTemplate;
  }

  @JsonProperty
  public CompactionStateMatcher getStateMatcher()
  {
    return stateMatcher;
  }

  @Override
  List<CompactionJob> createCompactionJobs(
      InputSource source,
      OutputDestination destination,
      CompactionJobParams jobParams
  )
  {
    final DruidInputSource druidInputSource = ensureDruidInputSource(source);
    final String dataSource = druidInputSource.getDataSource();

    // Identify the compactible candidate segments
    final CompactionConfigBasedJobTemplate delegate =
        CompactionConfigBasedJobTemplate.create(dataSource, stateMatcher);
    final DataSourceCompactibleSegmentIterator candidateIterator =
        delegate.getCompactibleCandidates(source, destination, jobParams);

    // Create MSQ jobs for each candidate by interpolating the template variables
    final List<CompactionJob> jobs = new ArrayList<>();
    while (candidateIterator.hasNext()) {
      final CompactionCandidate candidate = candidateIterator.next();
      jobs.add(
          new CompactionJob(
              createQueryForJob(dataSource, candidate.getCompactionInterval()),
              candidate,
              CompactionSlotManager.getMaxTaskSlotsForMSQCompactionTask(sqlTemplate.getContext())
          )
      );
    }

    return jobs;
  }

  private ClientSqlQuery createQueryForJob(String dataSource, Interval compactionInterval)
  {
    final String formattedSql = formatSql(
        sqlTemplate.getQuery(),
        Map.of(
            VAR_DATASOURCE, dataSource,
            VAR_START_TIMESTAMP, compactionInterval.getStart().toString(TIMESTAMP_FORMATTER),
            VAR_END_TIMESTAMP, compactionInterval.getEnd().toString(TIMESTAMP_FORMATTER)
        )
    );

    final Map<String, Object> context = new HashMap<>();
    if (sqlTemplate.getContext() != null) {
      context.putAll(sqlTemplate.getContext());
    }
    context.put(CompactSegments.STORE_COMPACTION_STATE_KEY, true);

    return new ClientSqlQuery(
        formattedSql,
        sqlTemplate.getResultFormat(),
        sqlTemplate.isHeader(),
        sqlTemplate.isTypesHeader(),
        sqlTemplate.isSqlTypesHeader(),
        context,
        sqlTemplate.getParameters()
    );
  }

  /**
   * Formats the given SQL by replacing the template variables.
   */
  public static String formatSql(String sqlTemplate, Map<String, String> templateVariables)
  {
    String sql = sqlTemplate;
    for (Map.Entry<String, String> variable : templateVariables.entrySet()) {
      sql = StringUtils.replace(sql, variable.getKey(), variable.getValue());
    }

    return sql;
  }

  @Override
  public boolean equals(Object object)
  {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    MSQCompactionJobTemplate that = (MSQCompactionJobTemplate) object;
    return Objects.equals(sqlTemplate, that.sqlTemplate)
           && Objects.equals(stateMatcher, that.stateMatcher);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sqlTemplate, stateMatcher);
  }

  @Override
  public String getType()
  {
    return TYPE;
  }
}
