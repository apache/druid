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

import { act, fireEvent, render } from '@testing-library/react';

import type { useQueryManager as UseQueryManager } from '../../hooks';
import * as hooks from '../../hooks';
import * as singletons from '../../singletons';
import { QueryState } from '../../utils';

import { ReindexingTimeline } from './reindexing-timeline';

// Mock the useQueryManager hook
jest.mock('../../hooks', () => ({
  useQueryManager: jest.fn(),
}));

const mockUseQueryManager = hooks.useQueryManager as jest.MockedFunction<typeof UseQueryManager>;

// Mock Api singleton
jest.mock('../../singletons', () => ({
  Api: {
    instance: {
      get: jest.fn(),
      post: jest.fn(),
      encodePath: jest.fn((path: string) => path),
    },
  },
  AppToaster: {
    show: jest.fn(),
  },
}));

// Get mock references after mocking
const mockPost = singletons.Api.instance.post as jest.Mock;
const mockAppToasterShow = singletons.AppToaster.show as jest.Mock;

// Mock chronoshift Duration
jest.mock('chronoshift', () => ({
  Duration: jest.fn().mockImplementation((period: string) => {
    if (period === 'INVALID') {
      throw new Error('Invalid period format');
    }
    return {
      shift: jest.fn((date: Date) => new Date(date.getTime() - 7 * 24 * 60 * 60 * 1000)),
    };
  }),
  Timezone: {
    UTC: 'UTC',
  },
}));

// Sample test data
const mockTimelineData = {
  dataSource: 'test-datasource',
  referenceTime: '2024-11-15T12:00:00.000Z',
  intervals: [
    {
      interval: '2024-11-01T00:00:00.000Z/2024-11-08T00:00:00.000Z',
      ruleCount: 3,
      config: {
        granularitySpec: {
          segmentGranularity: 'DAY',
          queryGranularity: 'HOUR',
          rollup: true,
        },
        metricsSpec: [{ type: 'count', name: 'count' }],
      },
      appliedRules: [
        { type: 'dataSchema', id: 'schema-1' },
        { type: 'partitioning', id: 'part-1' },
        { type: 'indexSpec', id: 'idx-1' },
      ],
    },
    {
      interval: '2024-11-08T00:00:00.000Z/2024-11-15T00:00:00.000Z',
      ruleCount: 0,
      config: {},
      appliedRules: [],
    },
  ],
};

describe('ReindexingTimeline', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  // ====================================================================
  // PHASE 1: UTILITY FUNCTIONS
  // ====================================================================
  describe('utility functions', () => {
    // Note: These are internal functions, so we'll test them indirectly through component behavior
    // For direct testing, we'd need to export them, which may not be desired

    describe('date formatting', () => {
      it('formats dates consistently across the component', () => {
        mockUseQueryManager.mockReturnValue([
          new QueryState({ data: mockTimelineData }),
          {} as any,
        ]);

        const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

        // Reference time should be formatted with UTC
        expect(container.textContent).toContain('Nov 15, 2024');
      });
    });

    describe('interval color generation', () => {
      it('applies different colors to different intervals', () => {
        mockUseQueryManager.mockReturnValue([
          new QueryState({ data: mockTimelineData }),
          {} as any,
        ]);

        const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

        const segments = container.querySelectorAll('.timeline-segment');
        expect(segments.length).toBe(2);

        // Each segment should have a background color
        segments.forEach(segment => {
          const htmlSegment = segment as HTMLElement;
          expect(htmlSegment.style.backgroundColor).toBeTruthy();
        });
      });
    });
  });

  // ====================================================================
  // PHASE 2: COMPONENT RENDERING
  // ====================================================================
  describe('component rendering', () => {
    it('matches snapshot for loading state', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ loading: true }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot for error state', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({ error: new Error('Failed to load timeline') }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot for empty intervals', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            intervals: [],
          },
        }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot with validation error', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            intervals: [],
            validationError: {
              errorType: 'INVALID_GRANULARITY_TIMELINE',
              message: 'Granularity must become coarser over time',
              olderInterval: '2024-01-01/2024-02-01',
              olderGranularity: 'HOUR',
              newerInterval: '2024-02-01/2024-03-01',
              newerGranularity: 'DAY',
            },
          },
        }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot with normal intervals', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot with skip offset applied', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              type: 'skipOffsetFromNow',
              period: 'P7D',
              isApplied: true,
              effectiveEndTime: '2024-11-08T12:00:00.000Z',
            },
          },
        }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot with skip offset not applied', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              type: 'skipOffsetFromLatest',
              period: 'P7D',
              isApplied: false,
              reason: 'Requires actual segment timeline data',
            },
          },
        }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('marks skipped intervals with special styling', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const segments = container.querySelectorAll('.timeline-segment');
      const skippedSegments = container.querySelectorAll('.timeline-segment.skipped');

      expect(segments.length).toBe(2);
      expect(skippedSegments.length).toBe(1); // Second interval has ruleCount: 0
    });

    it('displays interval details when interval is selected', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      // Initially no detail panel
      expect(container.querySelector('.interval-detail-panel')).toBeNull();

      // Click first interval
      const firstSegment = container.querySelector('.timeline-segment:not(.skipped)')!;
      fireEvent.click(firstSegment);

      // Detail panel should appear
      expect(container.querySelector('.interval-detail-panel')).toBeTruthy();
    });

    describe('interval detail panel UX', () => {
      function renderWithDetailPanel() {
        const dataWithDeletion = {
          ...mockTimelineData,
          intervals: [
            {
              ...mockTimelineData.intervals[0],
              config: {
                ...mockTimelineData.intervals[0].config,
                tuningConfig: {
                  partitionsSpec: { type: 'dynamic' },
                },
              },
              appliedRules: [
                ...mockTimelineData.intervals[0].appliedRules,
                { type: 'deletion', id: 'del-1' },
              ],
              ruleCount: 4,
            },
            mockTimelineData.intervals[1],
          ],
        };

        mockUseQueryManager.mockReturnValue([
          new QueryState({ data: dataWithDeletion }),
          {} as any,
        ]);

        const result = render(<ReindexingTimeline supervisorId="test-supervisor" />);

        // Click first interval to open detail panel
        const firstSegment = result.container.querySelector('.timeline-segment:not(.skipped)')!;
        fireEvent.click(firstSegment);

        return result;
      }

      it('shows "Effective Configuration" label in detail panel', () => {
        const { container } = renderWithDetailPanel();
        const label = container.querySelector('.config-summary-label');
        expect(label).toBeTruthy();
        expect(label!.textContent).toBe('Effective Configuration');
      });

      it('does not show standalone "N rules applied" tag in header', () => {
        const { container } = renderWithDetailPanel();
        const header = container.querySelector('.detail-header')!;
        // Header should contain the interval text and close button, but no "rules applied" tag
        expect(header.textContent).not.toContain('rules applied');
        expect(header.textContent).not.toContain('rule applied');
      });

      it('shows rule count in View Raw Rules button', () => {
        const { getByText } = renderWithDetailPanel();
        expect(getByText('View 4 Raw Rules')).toBeTruthy();
      });

      it('shows "deletion clauses" not "deletion rules" in badge', () => {
        const { container } = renderWithDetailPanel();
        const badges = container.querySelector('.config-badges')!;
        expect(badges.textContent).toContain('deletion clause');
        expect(badges.textContent).not.toContain('deletion rule');
      });

      it('shows partitioning badge when tuningConfig has partitionsSpec', () => {
        const { container } = renderWithDetailPanel();
        const badges = container.querySelector('.config-badges')!;
        expect(badges.textContent).toContain('Partitioning: dynamic');
      });
    });
  });

  // ====================================================================
  // PHASE 3: ERROR HANDLING
  // ====================================================================
  describe('error handling', () => {
    it('handles invalid Duration period gracefully', () => {
      const mockDuration = jest.requireMock('chronoshift').Duration;
      mockDuration.mockImplementationOnce(() => {
        throw new Error('Invalid ISO 8601 duration');
      });

      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              type: 'skipOffsetFromLatest',
              period: 'INVALID',
              isApplied: false,
              reason: 'Test',
            },
          },
        }),
        {} as any,
      ]);

      // Should not crash
      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container).toBeTruthy();
    });

    it('handles API errors in handleQueryMaxTime', async () => {
      mockPost.mockRejectedValueOnce(new Error('Network error'));

      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              type: 'skipOffsetFromLatest',
              period: 'P7D',
              isApplied: false,
              reason: 'Test',
            },
          },
        }),
        {} as any,
      ]);

      const { getByText } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const button = getByText('Query latest timestamp');
      await act(async () => {
        fireEvent.click(button);
        await Promise.resolve();
      });

      // Should show error toast
      expect(mockAppToasterShow).toHaveBeenCalledWith(
        expect.objectContaining({
          intent: 'danger',
        }),
      );
    });

    it('handles empty response from timeBoundary query', async () => {
      mockPost.mockResolvedValueOnce({ data: [] });

      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              type: 'skipOffsetFromLatest',
              period: 'P7D',
              isApplied: false,
              reason: 'Test',
            },
          },
        }),
        {} as any,
      ]);

      const { getByText } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const button = getByText('Query latest timestamp');
      await act(async () => {
        fireEvent.click(button);
        await Promise.resolve();
      });

      // Should show warning toast
      expect(mockAppToasterShow).toHaveBeenCalledWith(
        expect.objectContaining({
          intent: 'warning',
          message: 'No data found in datasource',
        }),
      );
    });

    it('handles missing result in timeBoundary response', async () => {
      mockPost.mockResolvedValueOnce({
        data: [{ result: null }],
      });

      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              type: 'skipOffsetFromLatest',
              period: 'P7D',
              isApplied: false,
              reason: 'Test',
            },
          },
        }),
        {} as any,
      ]);

      const { getByText } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const button = getByText('Query latest timestamp');
      await act(async () => {
        fireEvent.click(button);
        await Promise.resolve();
      });

      // Should show warning toast
      expect(mockAppToasterShow).toHaveBeenCalledWith(
        expect.objectContaining({
          intent: 'warning',
        }),
      );
    });
  });

  // ====================================================================
  // ACCESSIBILITY
  // ====================================================================
  describe('accessibility', () => {
    it('has proper ARIA attributes on interactive intervals', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const interactiveSegment = container.querySelector('.timeline-segment:not(.skipped)')!;

      expect(interactiveSegment.getAttribute('role')).toBe('button');
      expect(interactiveSegment.getAttribute('tabindex')).toBe('0');
      expect(interactiveSegment.getAttribute('aria-label')).toBeTruthy();
    });

    it('does not make skipped intervals interactive', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const skippedSegment = container.querySelector('.timeline-segment.skipped')!;

      expect(skippedSegment.getAttribute('role')).toBeNull();
      expect(skippedSegment.getAttribute('tabindex')).toBeNull();
    });

    it('has toolbar role on timeline container', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const timeline = container.querySelector('.timeline-bar');
      expect(timeline?.getAttribute('role')).toBe('toolbar');
      expect(timeline?.getAttribute('aria-label')).toBeTruthy();
    });
  });
});
