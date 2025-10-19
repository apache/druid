# Kubernetes Dynamic Config Feature - Development History

This document tracks all PRs and commits related to the Kubernetes Task Runner Dynamic Config feature in Apache Druid.

## Overview

The dynamic config feature allows Druid operators to dynamically tune certain features within the K8s extension without restarting the Overlord service. This includes dynamic pod template selection based on task properties like tags, task type, and data source.

---

## 1. Initial Implementation - PR #16510

**Status:** ✅ Merged  
**Date:** June 12, 2024  
**Author:** YongGang (mail.che@gmail.com)  
**Commit:** `46dbc7405390200eda87a04cb6eb70f8be7d3d18`

### Title
Support Dynamic Peon Pod Template Selection in K8s extension

### GitHub Link
https://github.com/apache/druid/pull/16510

### Changes
- **20 files changed**
- **+1,485 insertions, -40 deletions**

### What Was Added

**New API Endpoints:**
- `GET /druid/indexer/v1/k8s/taskrunner/executionconfig` - Retrieve current dynamic execution config
- `POST /druid/indexer/v1/k8s/taskrunner/executionconfig` - Update dynamic execution config
- `GET /druid/indexer/v1/k8s/taskrunner/executionconfig/history` - Retrieve configuration history

**New Core Classes:**
- `KubernetesTaskExecutionConfigResource.java` - REST API resource for managing dynamic config
- `KubernetesTaskRunnerDynamicConfig.java` - Interface for dynamic configuration
- `DefaultKubernetesTaskRunnerDynamicConfig.java` - Default implementation of dynamic config
- `PodTemplateSelectStrategy.java` - Strategy interface for pod template selection
- `SelectorBasedPodTemplateSelectStrategy.java` - Selector-based strategy implementation
- `TaskTypePodTemplateSelectStrategy.java` - Task type-based strategy implementation
- `Selector.java` - Selector matching logic for conditional pod template selection

**Enhanced Classes:**
- `PodTemplateTaskAdapter.java` - Updated to support dynamic pod template selection
- `KubernetesTaskRunnerFactory.java` - Integration of dynamic config

**Tests:**
- Added comprehensive test coverage for all new classes
- `KubernetesTaskExecutionConfigResourceTest.java`
- `SelectorBasedPodTemplateSelectStrategyTest.java`
- `SelectorTest.java`

**Documentation:**
- Added "Dynamic Pod Template Selection Config" section to k8s-jobs.md

### Key Features Introduced
- Dynamic pod template selection based on task properties
- Selector-based matching (task context tags, task type, data source)
- Priority-based selector ordering
- Configuration history tracking with audit information
- No Overlord restart required for config changes

---

## 2. Documentation Enhancement - PR #16600

**Status:** ✅ Merged  
**Date:** June 21, 2024 (9 days after initial PR)  
**Author:** Suneet Saldanha (suneet@apache.org)  
**Commit:** `4e0ea7823b128cc214df8dc774d370b306b91461`

### Title
Update docs for K8s TaskRunner Dynamic Config

### GitHub Link
https://github.com/apache/druid/pull/16600

### Changes
- **7 files changed**
- **+384 insertions, -84 deletions**

### What Was Added

**Major Documentation Improvements:**
- Comprehensive API documentation with detailed examples
- Added sample request/response bodies for all endpoints
- Detailed explanation of `PodTemplateSelectStrategy` types
- Added cURL and HTTP request examples
- Documented query parameters and header parameters
- Added configuration examples for selector-based strategy

**Code Improvements:**
- Refinements to `PodTemplateSelectStrategy.java` interface
- Enhanced `SelectorBasedPodTemplateSelectStrategy.java` with better null handling
- Removed unused code from `PodTemplateTaskAdapter.java`

**Test Updates:**
- Updated test cases in `KubernetesTaskRunnerDynamicConfigTest.java`
- Enhanced `SelectorBasedPodTemplateSelectStrategyTest.java`
- Updated `PodTemplateTaskAdapterTest.java`

### Impact
This PR significantly improved the usability of the feature by providing comprehensive documentation with real-world examples.

---

## 3. Documentation Fix - PR #16720

**Status:** ✅ Merged  
**Date:** July 10, 2024 (28 days after initial PR)  
**Author:** YongGang (mail.che@gmail.com)  
**Commit:** `4b293fc2a937da64cee8c1cd257a3ce85e333884`

### Title
Docs: Fix k8s dynamic config URL

### GitHub Link
https://github.com/apache/druid/pull/16720

### Changes
- **1 file changed**
- **+9 insertions, -9 deletions**

### What Was Fixed

**Documentation Corrections:**
- Fixed incorrect URL paths in documentation
- Corrected API endpoint references in k8s-jobs.md
- Ensured consistency between documented URLs and actual implementation

### Impact
Minor but important fix ensuring developers use the correct API endpoints.

---

## 4. Feature Enhancement - PR #16772

**Status:** ✅ Merged  
**Date:** July 23, 2024 (41 days after initial PR)  
**Author:** George Shiqi Wu (george.wu@imply.io)  
**Commit:** `a64e9a17462d34c246a8793b3efebcb4ad4a3736`

### Title
Add annotation for pod template

### GitHub Link
https://github.com/apache/druid/pull/16772

### Changes
- **15 files changed**
- **+291 insertions, -14 deletions**

### What Was Added

**New Functionality:**
- Added support for annotations in pod templates
- Created `PodTemplateWithName.java` class to encapsulate pod template with metadata

**Enhanced Classes:**
- `DruidK8sConstants.java` - Added annotation constants
- `PodTemplateSelectStrategy.java` - Enhanced to return `PodTemplateWithName`
- `SelectorBasedPodTemplateSelectStrategy.java` - Updated to handle annotations
- `TaskTypePodTemplateSelectStrategy.java` - Updated to support annotations
- `PodTemplateTaskAdapter.java` - Enhanced to apply annotations to pod templates

**New Test Files:**
- `PodTemplateWithNameTest.java` - Tests for the new wrapper class
- Multiple expected output YAML files for testing annotated pods

**Test Updates:**
- Enhanced `SelectorBasedPodTemplateSelectStrategyTest.java`
- Updated `PodTemplateTaskAdapterTest.java` with annotation test cases

### Impact
This enhancement allows users to add metadata annotations to dynamically selected pod templates, improving Kubernetes resource management and monitoring.

---

## 5. Bug Fix - PR #17400

**Status:** ✅ Merged  
**Date:** October 30, 2024 (140 days after initial PR)  
**Author:** Kiran Gadhave (kiran.gadhave@imply.io)  
**Commit:** `5fcf4205e4b1f17edccc4a37cf338ed5e3047ae1`

### Title
Handle empty values for task and datasource conditions in pod template selector

### GitHub Link
https://github.com/apache/druid/pull/17400

### Changes
- **2 files changed**
- **+62 insertions, -3 deletions**

### What Was Fixed

**Bug Fix:**
- Fixed handling of empty sets for `dataSourceCondition` in selectors
- Fixed handling of empty sets for `taskTypeCondition` in selectors
- Previously, empty collections would not match correctly

**Code Changes:**
- Updated `Selector.java` with proper empty set handling logic
- Changed matching logic to handle empty collections as "match all"

**Tests Added:**
- Added 57+ lines of test code in `SelectorTest.java`
- Comprehensive test cases for empty condition scenarios
- Tests for both empty dataSource and empty taskType conditions

### Impact
Critical bug fix ensuring that selectors with empty conditions work as expected. Empty conditions now correctly match all values for that dimension.

---

## 6. Documentation Update - PR #17464

**Status:** ✅ Merged  
**Date:** November 12, 2024 (153 days after initial PR)  
**Author:** Kiran Gadhave (kiran.gadhave@imply.io)  
**Commit:** `1dbd005df6a7821ddbdd1599de132403abde1fb2`

### Title
Updated docs with behavior for empty collections in pod template selector config

### GitHub Link
https://github.com/apache/druid/pull/17464

### Changes
- **1 file changed**
- **+34 insertions**

### What Was Added

**Documentation Updates:**
- Added detailed explanation of empty collection behavior in selectors
- Documented that empty arrays mean "match all values" for that condition
- Added examples showing how empty conditions work
- Clarified the matching logic for both empty and populated conditions

### Impact
Improved documentation clarity for users configuring selectors, especially explaining the "match all" behavior of empty collections.

---

## 7. Module Migration - PR #17614

**Status:** ✅ Merged  
**Date:** January 17, 2025 (219 days after initial PR)  
**Author:** George Shiqi Wu (george.wu@imply.io)  
**Commit:** `62a53ab41bfdd0c9f6eddd95630b42f82f8ba89b`

### Title
Make k8s ingestion core

### GitHub Link
https://github.com/apache/druid/pull/17614

### Changes
- **111 files changed**
- **+60 insertions, -11 deletions**

### What Was Changed

**Module Migration:**
- Moved K8s extension from `extensions-contrib/` to `extensions-core/`
- Updated all import paths and references
- Changed documentation paths from `extensions-contrib` to `extensions-core`
- Updated build configuration in `pom.xml`

**Documentation Updates:**
- Updated links throughout documentation
- Added redirects for old documentation URLs
- Added disclaimer about Druid 28 requirement
- Updated architecture and indexer documentation

**Configuration Updates:**
- Updated GitHub labeler configuration
- Updated distribution POM to include as core extension
- Updated extension configuration documentation

### Impact
This migration promotes the K8s extension (including dynamic config) from contributed extension to core extension status, indicating its maturity and importance to the project. **No functional changes to the dynamic config feature itself.**

---

## Summary Statistics

### Total Development Timeline
- **Start Date:** June 12, 2024
- **Latest Update:** January 17, 2025
- **Duration:** ~7 months of active development and refinement

### Overall Changes
- **7 Pull Requests** merged
- **156 files** changed across all PRs
- **+2,385 insertions, -175 deletions** (net +2,210 lines)

### Contributor Breakdown
- **YongGang** - Initial implementation + documentation fixes (2 PRs)
- **Suneet Saldanha** - Major documentation enhancement (1 PR)
- **George Shiqi Wu** - Annotation support + module migration (2 PRs)
- **Kiran Gadhave** - Bug fix + documentation (2 PRs)

### PR Categories
- **Feature Implementation:** 2 PRs (16510, 16772)
- **Bug Fixes:** 1 PR (17400)
- **Documentation:** 3 PRs (16600, 16720, 17464)
- **Infrastructure:** 1 PR (17614)

---

## Key Takeaways

1. **Iterative Development:** The feature underwent significant refinement after the initial implementation, with 6 follow-up PRs over 7 months.

2. **Documentation Focus:** 3 out of 7 PRs focused primarily on documentation improvements, showing the team's commitment to usability.

3. **Community Collaboration:** 4 different contributors worked on enhancing the feature, demonstrating good community involvement.

4. **Bug Fixes:** At least one critical bug (empty collection handling) was discovered and fixed after initial release.

5. **Production Ready:** The migration to `extensions-core` in PR #17614 indicates the feature has reached production-ready maturity.

---

## References

- **Apache Druid Documentation:** https://druid.apache.org/docs/latest/development/extensions-core/k8s-jobs#dynamic-config
- **GitHub Repository:** https://github.com/apache/druid
- **Extension Path:** `extensions-core/kubernetes-overlord-extensions/` (as of Druid 31+)

---

*Document created: January 2025*  
*Last updated: January 17, 2025*

