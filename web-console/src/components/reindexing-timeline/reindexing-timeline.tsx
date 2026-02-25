/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Button, Callout, Card, Dialog, Intent, Tag, Tooltip } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import { Duration, Timezone } from 'chronoshift';
import copy from 'copy-to-clipboard';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';
import AceEditor from 'react-ace';

import { useQueryManager } from '../../hooks';
import { Api, AppToaster } from '../../singletons';
import { downloadFile } from '../../utils';
import { Loader } from '../loader/loader';

import './reindexing-timeline.scss';

const TIMELINE_INTERVAL_COLORS = [
  '#5C7080', // gray
  '#738694', // light gray
  '#8A9BA8', // lighter gray
  '#394B59', // dark gray
  '#4A5568', // medium dark gray
  '#6B7E91', // medium gray
  '#2F343C', // darker gray
];

const SKIPPED_INTERVAL_COLOR = 'rgba(219, 55, 55, 0.15)';
const JSON_VIEWER_HEIGHT = '500px';

function getIntervalColor(index: number): string {
  return TIMELINE_INTERVAL_COLORS[index % TIMELINE_INTERVAL_COLORS.length];
}

interface ReindexingTimelineProps {
  supervisorId: string;
}

interface GranularitySpec {
  segmentGranularity?: string;
  queryGranularity?: string;
  rollup?: boolean;
}

interface CompactionConfig {
  granularitySpec?: GranularitySpec;
  metricsSpec?: Record<string, unknown>[];
  dimensionsSpec?: {
    dimensions?: Record<string, unknown>[];
  };
  projections?: Record<string, unknown>[];
  transformSpec?: Record<string, unknown>;
  tuningConfig?: Record<string, unknown>;
  ioConfig?: Record<string, unknown>;
}

interface ReindexingRule {
  type: string;
  id?: string;
  olderThan?: string;
}

interface IntervalConfig {
  interval: string;
  ruleCount: number;
  config: CompactionConfig;
  appliedRules: ReindexingRule[];
}

interface SkipOffsetInfo {
  type: string;
  period: string;
  isApplied: boolean;
  effectiveEndTime?: string;
  reason?: string;
}

interface ValidationError {
  errorType: string;
  message: string;
  olderInterval?: string;
  olderGranularity?: string;
  newerInterval?: string;
  newerGranularity?: string;
}

interface ReindexingTimelineData {
  dataSource: string;
  referenceTime: string;
  skipOffset?: SkipOffsetInfo;
  intervals: IntervalConfig[];
  validationError?: ValidationError;
}

export const ReindexingTimeline = React.memo(function ReindexingTimeline(
  props: ReindexingTimelineProps,
) {
  const { supervisorId } = props;
  const [selectedIntervalIndex, setSelectedIntervalIndex] = useState<number | undefined>();
  const [queriedMaxTime, setQueriedMaxTime] = useState<string | undefined>();
  const [queryingMaxTime, setQueryingMaxTime] = useState(false);

  const [timelineState] = useQueryManager<string, ReindexingTimelineData>({
    query: supervisorId,
    processQuery: async (supervisorId, signal) => {
      const resp = await Api.instance.get<ReindexingTimelineData>(
        `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}/reindexingTimeline`,
        { signal },
      );
      return resp.data;
    },
  });

  if (timelineState.loading) {
    return <Loader />;
  }

  if (timelineState.error) {
    return (
      <div className="reindexing-timeline">
        <Callout intent={Intent.DANGER} title="Error loading reindexing timeline">
          {timelineState.getErrorMessage()}
        </Callout>
      </div>
    );
  }

  const timelineData = timelineState.data;
  if (!timelineData) {
    return null;
  }

  const { intervals, skipOffset, referenceTime, validationError } = timelineData;

  const handleQueryMaxTime = async () => {
    setQueryingMaxTime(true);
    try {
      const query = {
        queryType: 'timeBoundary',
        dataSource: timelineData.dataSource,
      };
      const resp = await Api.instance.post('/druid/v2', query);
      const result = resp.data;
      if (result && result.length > 0 && result[0].result) {
        const maxTime = result[0].result.maxTime;
        setQueriedMaxTime(maxTime);
      } else {
        AppToaster.show({
          message: 'No data found in datasource',
          intent: Intent.WARNING,
        });
      }
    } catch (e) {
      AppToaster.show({
        message: `Failed to query max time: ${e.message}`,
        intent: Intent.DANGER,
      });
    } finally {
      setQueryingMaxTime(false);
    }
  };

  // Calculate effective end time if we have queried max time and skipOffsetFromLatest
  let effectiveEndTime: Date | undefined;
  if (queriedMaxTime && skipOffset && !skipOffset.isApplied) {
    const period = skipOffset.period;
    try {
      const duration = new Duration(period);
      effectiveEndTime = duration.shift(new Date(queriedMaxTime), Timezone.UTC, -1);
    } catch (e) {
      console.error('Failed to parse skip offset period:', period, e);
      AppToaster.show({
        message: `Invalid skip offset period format: ${period}`,
        intent: Intent.WARNING,
      });
    }
  }

  // Display validation error if present
  if (validationError) {
    return (
      <div className="reindexing-timeline">
        <Callout intent={Intent.DANGER} title="Invalid Supervisor Configuration">
          <p>
            <strong>
              The segment granularity rule definitions have created an illegal segment granularity
              timeline.
            </strong>
          </p>
          {validationError.errorType === 'INVALID_GRANULARITY_TIMELINE' &&
            validationError.olderInterval && (
              <>
                <p>
                  Segment granularity must stay the same or become <strong>coarser</strong> as data
                  ages from present to past. Your configuration violates this constraint:
                </p>
                <ul>
                  <li>
                    <strong>Older interval</strong> (further in the past):{' '}
                    <code>{formatInterval(validationError.olderInterval)}</code> has{' '}
                    <strong>finer</strong> granularity:{' '}
                    <Tag intent={Intent.PRIMARY}>{validationError.olderGranularity}</Tag>
                  </li>
                  <li>
                    <strong>Newer interval</strong> (more recent):{' '}
                    <code>{formatInterval(validationError.newerInterval!)}</code> has{' '}
                    <strong>coarser</strong> granularity:{' '}
                    <Tag intent={Intent.WARNING}>{validationError.newerGranularity}</Tag>
                  </li>
                </ul>
                <p>
                  <strong>To fix this:</strong> Adjust your segment granularity rules so that as
                  time moves from present to past, the granularity either stays the same or gets
                  coarser (e.g., HOUR → DAY → MONTH → YEAR).
                </p>
              </>
            )}
          {validationError.errorType !== 'INVALID_GRANULARITY_TIMELINE' && (
            <p>{validationError.message}</p>
          )}
        </Callout>
      </div>
    );
  }

  if (intervals.length === 0) {
    return (
      <div className="reindexing-timeline">
        <Callout intent={Intent.WARNING}>
          No reindexing intervals found. This may indicate that the rule provider is not ready or no
          rules are configured.
        </Callout>
      </div>
    );
  }

  const selectedInterval =
    selectedIntervalIndex !== undefined ? intervals[selectedIntervalIndex] : undefined;

  return (
    <div className="reindexing-timeline">
      <div className="timeline-header">
        <div className="header-info">
          <strong>DataSource:</strong> {timelineData.dataSource}
          <span className="spacer">|</span>
          <Tooltip
            content="The reference time used in conjunction with each rule's 'olderThan' attribute to calculate which intervals a rule should apply to."
            position="bottom"
          >
            <strong className="help-hint">Reference Time:</strong>
          </Tooltip>{' '}
          {formatDateTimeUTC(referenceTime)}
        </div>
        {skipOffset && (
          <div className="skip-offset-info">
            {skipOffset.isApplied && (
              <Tag intent={Intent.SUCCESS} icon={IconNames.TICK}>
                Skip Offset: {skipOffset.type} ({skipOffset.period})
              </Tag>
            )}
            {!skipOffset.isApplied && !queriedMaxTime && (
              <>
                <Tooltip
                  content={
                    `This supervisor is configured to skip compaction of any search interval that is covered by ` +
                    `or overlaps the threshold of the latest timestamp in the data minus ${skipOffset.period}. ` +
                    `However, the underlying segment data is not available in this preview, so the timeline does not ` +
                    `reflect which intervals will be skipped during actual compaction.`
                  }
                  position="bottom"
                >
                  <Tag intent={Intent.WARNING} icon={IconNames.WARNING_SIGN}>
                    {skipOffset.type} ({skipOffset.period}): Not reflected in this preview
                  </Tag>
                </Tooltip>
                <Button
                  text="Query latest timestamp"
                  icon={IconNames.REFRESH}
                  small
                  onClick={() => void handleQueryMaxTime()}
                  loading={queryingMaxTime}
                  className="query-button"
                />
              </>
            )}
            {!skipOffset.isApplied && queriedMaxTime && effectiveEndTime && (
              <Tag intent={Intent.SUCCESS} icon={IconNames.TICK}>
                {skipOffset.type} ({skipOffset.period}): Applied (latest:{' '}
                {formatDateTimeUTC(queriedMaxTime)})
              </Tag>
            )}
          </div>
        )}
      </div>

      <Card className="timeline-bar-container">
        <div className="timeline-bar" role="toolbar" aria-label="Reindexing timeline intervals">
          {intervals.map((interval, idx) => {
            const [start, end] = interval.interval.split('/');
            const isSelected = selectedIntervalIndex === idx;

            // Check if interval is skipped due to skip offset (either from API or from queried max time)
            let isSkipped = interval.ruleCount === 0;
            if (!isSkipped && effectiveEndTime) {
              const intervalEnd = new Date(end);
              isSkipped = intervalEnd > effectiveEndTime;
            }

            return (
              <div
                key={interval.interval}
                className={`timeline-segment ${isSelected ? 'selected' : ''} ${
                  isSkipped ? 'skipped' : ''
                }`}
                style={{
                  backgroundColor: isSkipped ? SKIPPED_INTERVAL_COLOR : getIntervalColor(idx),
                  flex: 1,
                }}
                role={isSkipped ? undefined : 'button'}
                tabIndex={isSkipped ? undefined : 0}
                aria-label={
                  isSkipped
                    ? `Skipped interval ${interval.interval}`
                    : `Interval ${interval.interval} with ${interval.ruleCount} rule${
                        interval.ruleCount !== 1 ? 's' : ''
                      }`
                }
                aria-selected={isSelected}
                onClick={() => !isSkipped && setSelectedIntervalIndex(idx)}
                onKeyDown={e => {
                  if (!isSkipped && (e.key === 'Enter' || e.key === ' ')) {
                    e.preventDefault();
                    setSelectedIntervalIndex(idx);
                  }
                }}
                title={
                  isSkipped
                    ? `${interval.interval}\nSkipped (beyond skip offset)`
                    : `${interval.interval}\n${interval.ruleCount} rule(s) applied`
                }
              >
                <div className="segment-label">
                  <div className="segment-date">{formatDateShort(start)}</div>
                  <div className="segment-date">{formatDateShort(end)}</div>
                  <div className="segment-rules">
                    <Tag minimal intent={isSkipped ? Intent.DANGER : undefined}>
                      {isSkipped
                        ? 'skipped'
                        : `${interval.ruleCount} rule${interval.ruleCount !== 1 ? 's' : ''}`}
                    </Tag>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
        <div className="timeline-hint">Click on an interval to view its configuration details</div>
      </Card>

      {selectedInterval && selectedInterval.ruleCount > 0 && (
        <IntervalDetailPanel
          interval={selectedInterval}
          onClose={() => setSelectedIntervalIndex(undefined)}
        />
      )}
    </div>
  );
});

function formatDateShort(isoDate: string): string {
  if (isoDate.startsWith('-')) {
    return '-INF';
  }

  const d = new Date(isoDate);
  return `${UTC_MONTH_NAMES[d.getUTCMonth()]} ${d.getUTCDate()}, ${d.getUTCFullYear()}`;
}

const UTC_MONTH_NAMES = [
  'Jan',
  'Feb',
  'Mar',
  'Apr',
  'May',
  'Jun',
  'Jul',
  'Aug',
  'Sep',
  'Oct',
  'Nov',
  'Dec',
];

function formatDateTimeUTC(isoDate: string): string {
  // Handle start of time / very old dates
  if (isoDate.startsWith('-')) {
    return '-INF';
  }

  // Format using UTC methods to avoid local timezone shifting.
  // date-fns format() uses the browser's local timezone, so we extract UTC
  // components manually to ensure the displayed time is actually UTC.
  const d = new Date(isoDate);
  const month = UTC_MONTH_NAMES[d.getUTCMonth()];
  const day = d.getUTCDate();
  const year = d.getUTCFullYear();
  const hours = d.getUTCHours();
  const minutes = String(d.getUTCMinutes()).padStart(2, '0');
  const ampm = hours >= 12 ? 'PM' : 'AM';
  const displayHours = hours % 12 || 12;
  return `${month} ${day}, ${year} ${displayHours}:${minutes} ${ampm} UTC`;
}

function formatInterval(interval: string): string {
  const [start, end] = interval.split('/');
  const formattedStart = start.startsWith('-') ? '-INF' : start;
  const formattedEnd = end ? end : 'now';
  return `${formattedStart}/${formattedEnd}`;
}

function handleCopyToClipboard(data: CompactionConfig | ReindexingRule[], label: string): void {
  const jsonValue = JSONBig.stringify(data, undefined, 2);
  copy(jsonValue, { format: 'text/plain' });
  AppToaster.show({
    message: `${label} copied to clipboard`,
    intent: Intent.SUCCESS,
  });
}

function handleDownloadJson(data: CompactionConfig | ReindexingRule[], filename: string): void {
  const jsonValue = JSONBig.stringify(data, undefined, 2);
  downloadFile(jsonValue, 'json', filename);
}

interface IntervalDetailPanelProps {
  interval: IntervalConfig;
  onClose: () => void;
}

function IntervalDetailPanel({ interval, onClose }: IntervalDetailPanelProps) {
  const [showFullConfig, setShowFullConfig] = useState(false);
  const [showRawRules, setShowRawRules] = useState(false);
  const { config } = interval;

  const deletionRuleCount = interval.appliedRules.filter(r => r.type === 'deletion').length;
  const metricsCount = config.metricsSpec?.length || 0;
  const dimensionsCount = config.dimensionsSpec?.dimensions?.length || 0;
  const projectionsCount = config.projections?.length || 0;

  return (
    <>
      <Card className="interval-detail-panel">
        <div className="detail-header">
          <div>
            <h3>Interval: {formatInterval(interval.interval)}</h3>
            <Tag intent={Intent.PRIMARY} large>
              {interval.ruleCount} rule{interval.ruleCount !== 1 ? 's' : ''} applied
            </Tag>
          </div>
          <Button
            icon={IconNames.CROSS}
            minimal
            onClick={onClose}
            aria-label="Close interval details"
          />
        </div>

        <div className="detail-content">
          <div className="config-badges">
            {config.granularitySpec?.segmentGranularity && (
              <Tag intent={Intent.PRIMARY} large icon={IconNames.CALENDAR}>
                Segment: {config.granularitySpec.segmentGranularity}
              </Tag>
            )}
            {config.granularitySpec?.queryGranularity && (
              <Tag intent={Intent.PRIMARY} large icon={IconNames.TIME}>
                Query: {config.granularitySpec.queryGranularity}
              </Tag>
            )}
            {metricsCount > 0 && (
              <Tag intent={Intent.SUCCESS} large icon={IconNames.CALCULATOR}>
                {metricsCount} metric{metricsCount !== 1 ? 's' : ''}
              </Tag>
            )}
            {dimensionsCount > 0 && (
              <Tag intent={Intent.SUCCESS} large icon={IconNames.TAG}>
                {dimensionsCount} dimension{dimensionsCount !== 1 ? 's' : ''}
              </Tag>
            )}
            {projectionsCount > 0 && (
              <Tag intent={Intent.SUCCESS} large icon={IconNames.CUBE}>
                {projectionsCount} projection{projectionsCount !== 1 ? 's' : ''}
              </Tag>
            )}
            {deletionRuleCount > 0 && (
              <Tag intent={Intent.WARNING} large icon={IconNames.TRASH}>
                {deletionRuleCount} deletion rule{deletionRuleCount !== 1 ? 's' : ''}
              </Tag>
            )}
          </div>

          <div className="full-config-section">
            <Button
              icon={IconNames.CODE}
              text="View Full Configuration"
              intent={Intent.PRIMARY}
              onClick={() => setShowFullConfig(true)}
            />
            <Button
              icon={IconNames.PROPERTIES}
              text="View Raw Rules"
              intent={Intent.PRIMARY}
              onClick={() => setShowRawRules(true)}
            />
          </div>
        </div>
      </Card>

      <Dialog
        isOpen={showFullConfig}
        onClose={() => setShowFullConfig(false)}
        title={
          <Tooltip content={interval.interval} position="bottom">
            <span>{formatInterval(interval.interval)}</span>
          </Tooltip>
        }
        className="reindexing-config-dialog"
        canOutsideClickClose
      >
        <div className="bp5-dialog-body">
          <ConfigJsonViewer config={config} />
        </div>
        <div className="bp5-dialog-footer">
          <div className="bp5-dialog-footer-actions">
            <Button
              text="Copy"
              icon={IconNames.DUPLICATE}
              intent={Intent.PRIMARY}
              onClick={() => handleCopyToClipboard(config, 'Configuration')}
            />
            <Button
              text="Download"
              icon={IconNames.DOWNLOAD}
              intent={Intent.PRIMARY}
              onClick={() =>
                handleDownloadJson(
                  config,
                  `reindexing-config-${interval.interval.replace(/\//g, '-')}.json`,
                )
              }
            />
          </div>
        </div>
      </Dialog>

      <Dialog
        isOpen={showRawRules}
        onClose={() => setShowRawRules(false)}
        title={
          <Tooltip
            content={`${interval.appliedRules.length} rules for ${interval.interval}`}
            position="bottom"
          >
            <span>Rules for {formatInterval(interval.interval)}</span>
          </Tooltip>
        }
        className="reindexing-config-dialog"
        canOutsideClickClose
      >
        <div className="bp5-dialog-body">
          <ConfigJsonViewer config={interval.appliedRules} isRulesList />
        </div>
        <div className="bp5-dialog-footer">
          <div className="bp5-dialog-footer-actions">
            <Button
              text="Copy"
              icon={IconNames.DUPLICATE}
              intent={Intent.PRIMARY}
              onClick={() => handleCopyToClipboard(interval.appliedRules, 'Rules')}
            />
            <Button
              text="Download"
              icon={IconNames.DOWNLOAD}
              intent={Intent.PRIMARY}
              onClick={() =>
                handleDownloadJson(
                  interval.appliedRules,
                  `reindexing-rules-${interval.interval.replace(/\//g, '-')}.json`,
                )
              }
            />
          </div>
        </div>
      </Dialog>
    </>
  );
}

interface ConfigJsonViewerProps {
  config: CompactionConfig | ReindexingRule[];
  isRulesList?: boolean;
}

function ConfigJsonViewer({ config, isRulesList }: ConfigJsonViewerProps) {
  let jsonValue: string;

  if (isRulesList && Array.isArray(config)) {
    // Group rules by type for better readability
    const rulesByType: Record<string, ReindexingRule[]> = {};
    config.forEach((rule: ReindexingRule) => {
      const type = getRuleTypeName(rule);
      if (!rulesByType[type]) {
        rulesByType[type] = [];
      }
      rulesByType[type].push(rule);
    });
    jsonValue = JSONBig.stringify(rulesByType, undefined, 2);
  } else {
    jsonValue = JSONBig.stringify(config, undefined, 2);
  }

  return (
    <div className="config-json-viewer">
      <AceEditor
        mode="hjson"
        theme="solarized_dark"
        name="config-json-editor"
        value={jsonValue}
        readOnly
        width="100%"
        height={JSON_VIEWER_HEIGHT}
        showPrintMargin={false}
        showGutter
        editorProps={{ $blockScrolling: Infinity }}
        setOptions={{
          showLineNumbers: true,
          tabSize: 2,
        }}
      />
    </div>
  );
}

function getRuleTypeName(rule: ReindexingRule): string {
  // Use the explicit type field provided by Jackson serialization
  const typeMap: Record<string, string> = {
    deletion: 'Deletion Rules',
    dataSchema: 'Data Schema Rules',
    segmentGranularity: 'Segment Granularity Rules',
    tuningConfig: 'Tuning Config Rules',
    ioConfig: 'IO Config Rules',
  };

  return typeMap[rule.type] || 'Other Rules';
}
