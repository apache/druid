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
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';
import AceEditor from 'react-ace';

import { useQueryManager } from '../../hooks';
import { Api, AppToaster } from '../../singletons';
import { downloadFile } from '../../utils';
import { Loader } from '../loader/loader';

import './reindexing-timeline.scss';

interface ReindexingTimelineProps {
  supervisorId: string;
}

interface IntervalConfig {
  interval: string;
  ruleCount: number;
  config: any;
  appliedRules: any[];
}

interface SkipOffsetInfo {
  applied?: {
    type: string;
    period: string;
    effectiveEndTime: string;
  };
  notApplied?: {
    type: string;
    period: string;
    reason: string;
  };
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

  // Display validation error if present
  if (validationError) {
    return (
      <div className="reindexing-timeline">
        <Callout intent={Intent.DANGER} title="Invalid Supervisor Configuration">
          <p>
            <strong>The segment granularity rule definitions have created an illegal segment granularity timeline.</strong>
          </p>
          {validationError.errorType === 'INVALID_GRANULARITY_TIMELINE' && validationError.olderInterval && (
            <>
              <p>
                Segment granularity must stay the same or become <strong>coarser</strong> as data ages from present to past.
                Your configuration violates this constraint:
              </p>
              <ul>
                <li>
                  <strong>Older interval</strong> (further in the past): <code>{formatInterval(validationError.olderInterval)}</code>
                  {' '}has <strong>finer</strong> granularity: <Tag intent={Intent.PRIMARY}>{validationError.olderGranularity}</Tag>
                </li>
                <li>
                  <strong>Newer interval</strong> (more recent): <code>{formatInterval(validationError.newerInterval!)}</code>
                  {' '}has <strong>coarser</strong> granularity: <Tag intent={Intent.WARNING}>{validationError.newerGranularity}</Tag>
                </li>
              </ul>
              <p>
                <strong>To fix this:</strong> Adjust your segment granularity rules so that as time moves from present to past,
                the granularity either stays the same or gets coarser (e.g., HOUR → DAY → MONTH → YEAR).
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
          No reindexing intervals found. This may indicate that the rule provider is not ready or
          no rules are configured.
        </Callout>
      </div>
    );
  }

  const selectedInterval = selectedIntervalIndex !== undefined ? intervals[selectedIntervalIndex] : undefined;

  // Generate a color based on interval index using blue/gray scale theme
  const getIntervalColor = (index: number) => {
    const colors = [
      '#5C7080', // gray
      '#738694', // light gray
      '#8A9BA8', // lighter gray
      '#394B59', // dark gray
      '#4A5568', // medium dark gray
      '#6B7E91', // medium gray
      '#2F343C', // darker gray
    ];
    return colors[index % colors.length];
  };

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
            <strong style={{ borderBottom: '1px dotted #5c7080', cursor: 'help' }}>
              Reference Time:
            </strong>
          </Tooltip>{' '}
          {new Date(referenceTime).toLocaleString()}
        </div>
        {skipOffset && (
          <div className="skip-offset-info">
            {skipOffset.applied && (
              <Tag intent={Intent.SUCCESS} icon={IconNames.TICK}>
                Skip Offset: {skipOffset.applied.type} ({skipOffset.applied.period})
              </Tag>
            )}
            {skipOffset.notApplied && (
              <Tooltip
                content={
                  `This supervisor is configured to skip compaction of any search interval that is covered by ` +
                  `or overlaps the threshold of the latest timestamp in the data minus ${skipOffset.notApplied.period}. ` +
                  `However, the underlying segment data is not available in this preview, so the timeline does not ` +
                  `reflect which intervals will be skipped during actual compaction.`
                }
                position="bottom"
              >
                <Tag intent={Intent.WARNING} icon={IconNames.WARNING_SIGN}>
                  {skipOffset.notApplied.type} ({skipOffset.notApplied.period}): Not reflected in this preview
                </Tag>
              </Tooltip>
            )}
          </div>
        )}
      </div>

      <Card className="timeline-bar-container">
        <div className="timeline-bar">
          {intervals.map((interval, idx) => {
            const [start, end] = interval.interval.split('/');
            const isSelected = selectedIntervalIndex === idx;
            const isSkipped = interval.ruleCount === 0;
            return (
              <div
                key={idx}
                className={`timeline-segment ${isSelected ? 'selected' : ''} ${isSkipped ? 'skipped' : ''}`}
                style={{
                  backgroundColor: isSkipped ? 'rgba(219, 55, 55, 0.15)' : getIntervalColor(idx),
                  flex: 1,
                  opacity: isSelected ? 1 : 0.7,
                  cursor: isSkipped ? 'default' : 'pointer',
                }}
                onClick={() => !isSkipped && setSelectedIntervalIndex(idx)}
                title={isSkipped
                  ? `${interval.interval}\nSkipped (beyond skip offset)`
                  : `${interval.interval}\n${interval.ruleCount} rule(s) applied`}
              >
                <div className="segment-label">
                  <div className="segment-date">{formatDateShort(start)}</div>
                  <div className="segment-date">{formatDateShort(end)}</div>
                  <div className="segment-rules">
                    <Tag minimal intent={isSkipped ? Intent.DANGER : undefined}>
                      {isSkipped ? 'skipped' : `${interval.ruleCount} rule${interval.ruleCount !== 1 ? 's' : ''}`}
                    </Tag>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
        <div className="timeline-hint">
          Click on an interval to view its configuration details
        </div>
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
  // Handle start of time / very old dates
  if (isoDate.startsWith('-')) {
    return '-INF';
  }

  const date = new Date(isoDate);

  const month = date.toLocaleString('default', { month: 'short', timeZone: 'UTC' });
  const day = date.getUTCDate();
  const year = date.getUTCFullYear();
  return `${month} ${day}, ${year}`;
}

function formatInterval(interval: string): string {
  const [start, end] = interval.split('/');
  const formattedStart = start.startsWith('-') ? '-INF' : start;
  const formattedEnd = end ? end : 'now';
  return `${formattedStart}/${formattedEnd}`;
}

interface IntervalDetailPanelProps {
  interval: IntervalConfig;
  onClose: () => void;
}

function IntervalDetailPanel({ interval, onClose }: IntervalDetailPanelProps) {
  const [showFullConfig, setShowFullConfig] = useState(false);
  const [showRawRules, setShowRawRules] = useState(false);
  const { config } = interval;

  // Count deletion rules from transform spec
  const deletionRuleCount = countDeletionRules(config.transformSpec);
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
          <Button icon={IconNames.CROSS} minimal onClick={onClose} />
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
              onClick={() => {
                const jsonValue = JSONBig.stringify(config, undefined, 2);
                navigator.clipboard.writeText(jsonValue);
                AppToaster.show({
                  message: 'Configuration copied to clipboard',
                  intent: Intent.SUCCESS,
                });
              }}
            />
            <Button
              text="Download"
              icon={IconNames.DOWNLOAD}
              intent={Intent.PRIMARY}
              onClick={() => {
                const jsonValue = JSONBig.stringify(config, undefined, 2);
                const downloadFilename = `reindexing-config-${interval.interval.replace(/\//g, '-')}.json`;
                downloadFile(jsonValue, 'json', downloadFilename);
              }}
            />
          </div>
        </div>
      </Dialog>

      <Dialog
        isOpen={showRawRules}
        onClose={() => setShowRawRules(false)}
        title={
          <Tooltip content={`${interval.appliedRules.length} rules for ${interval.interval}`} position="bottom">
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
              onClick={() => {
                const jsonValue = JSONBig.stringify(interval.appliedRules, undefined, 2);
                navigator.clipboard.writeText(jsonValue);
                AppToaster.show({
                  message: 'Rules copied to clipboard',
                  intent: Intent.SUCCESS,
                });
              }}
            />
            <Button
              text="Download"
              icon={IconNames.DOWNLOAD}
              intent={Intent.PRIMARY}
              onClick={() => {
                const jsonValue = JSONBig.stringify(interval.appliedRules, undefined, 2);
                const downloadFilename = `reindexing-rules-${interval.interval.replace(/\//g, '-')}.json`;
                downloadFile(jsonValue, 'json', downloadFilename);
              }}
            />
          </div>
        </div>
      </Dialog>
    </>
  );
}

function countDeletionRules(transformSpec: any): number {
  if (!transformSpec || !transformSpec.filter) {
    return 0;
  }

  const filter = transformSpec.filter;

  // Check if it's a NotDimFilter with fields
  if (filter.type === 'not' && filter.field) {
    // If the field is an 'or' filter, count the number of fields in it
    if (filter.field.type === 'or' && filter.field.fields) {
      return filter.field.fields.length;
    }
    // Otherwise it's a single deletion rule
    return 1;
  }

  return 0;
}

interface ConfigJsonViewerProps {
  config: any;
  isRulesList?: boolean;
}

function ConfigJsonViewer({ config, isRulesList }: ConfigJsonViewerProps) {
  let displayValue: any;
  let jsonValue: string;

  if (isRulesList && Array.isArray(config)) {
    // Group rules by type for better readability
    const rulesByType: Record<string, any[]> = {};
    config.forEach((rule: any) => {
      const type = getRuleTypeName(rule);
      if (!rulesByType[type]) {
        rulesByType[type] = [];
      }
      rulesByType[type].push(rule);
    });
    displayValue = rulesByType;
    jsonValue = JSONBig.stringify(displayValue, undefined, 2);
  } else {
    displayValue = config;
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
        height="500px"
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

function getRuleTypeName(rule: any): string {
  // Use the explicit type field provided by Jackson serialization
  const typeMap: Record<string, string> = {
    'deletion': 'Deletion Rules',
    'dataSchema': 'Data Schema Rules',
    'segmentGranularity': 'Segment Granularity Rules',
    'tuningConfig': 'Tuning Config Rules',
    'ioConfig': 'IO Config Rules',
  };

  return typeMap[rule.type] || 'Other Rules';
}
