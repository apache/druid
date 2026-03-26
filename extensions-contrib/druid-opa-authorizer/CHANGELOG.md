# Changelog

## [Unreleased]

### Added

- Add support for Druid 35.0.1 ([#138]).
- Add support for Druid 34.0.0 (deprecated) ([#134], [#138]).

### Removed

- Remove support for Druid 33.0.0 ([#138]).
- Remove support for Druid 31.0.1 ([#134]).

[#134]: https://github.com/stackabletech/druid-opa-authorizer/pull/134
[#138]: https://github.com/stackabletech/druid-opa-authorizer/pull/138

## [0.7.0] - 2025-05-31

### Added

- Add support for Druid 30.0.1, 31.0.1, and 33.0.0 ([#110], [#112]).

### Changed

- Update dependencies ([#106]).
- Updated Maven plugin versions ([#110]).

### Removed

- Remove support for Druid 26.0.0, 27.0.0, 28.0.1, 30.0.0 ([#104], [#105], [#110], [#112]).

[#104]: https://github.com/stackabletech/druid-opa-authorizer/pull/104
[#105]: https://github.com/stackabletech/druid-opa-authorizer/pull/105
[#106]: https://github.com/stackabletech/druid-opa-authorizer/pull/106
[#110]: https://github.com/stackabletech/druid-opa-authorizer/pull/110
[#112]: https://github.com/stackabletech/druid-opa-authorizer/pull/112

## [0.6.0] - 2024-03-19

### Changed

- BREAKING: Add authenticationResult to OPA input ([#85]).

[#85]: https://github.com/stackabletech/druid-opa-authorizer/pull/85

## [0.5.0] - 2023-05-30

### Added

- Added support for Druid `26.0.0` ([#75]).

### Changed

- Changed build system to Maven ([#61]).
- Changed module prefix from `de` to `tech` ([#61]).

### Fixed

- Ignore additional JSON fields the OPA server is sending. This can e.g. be the cause when OPA decision logs are enabled ([#74]).

[#61]: https://github.com/stackabletech/druid-opa-authorizer/pull/61
[#74]: https://github.com/stackabletech/druid-opa-authorizer/pull/74
[#75]: https://github.com/stackabletech/druid-opa-authorizer/pull/75

## [0.3.0] - 2022-10-13

### Changed

- Changed Druid dependencies to 24.0.0 ([#57]).

[#57]: https://github.com/stackabletech/druid-opa-authorizer/pull/57

## [0.2.0] - 2022-03-22

### Changed

- Changed from Java 8 to Java 11 support ([#43]).
- Changed HTTP client to Java native ([#43]).

[#43]: https://github.com/stackabletech/druid-opa-authorizer/pull/43

## [0.1.0] - 2022-03-22

Initial Version
