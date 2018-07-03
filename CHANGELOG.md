# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog][changelog]
and this project adheres to [Semantic Versioning][semver].

[//]: =========================================================================
## [Unreleased]

[//]: =========================================================================
## [1.1.0] - 2018-03-03 - For [Batsim v2.0.0][Batsim v2.0.0]
### Added
- Added minimal bash completion via [taywee/args][taywee/args].
- Added ``--version`` option.
- New algorithms:
  - Crasher, whose purpose is to crash :).
    This is useful to test execution managers.
  - EnergyWatcher, whose purpose is just to query the energy consumption to
    Batsim
  - Random, whose purpose is notably to test time-sharing.
  - Sequencer, which is very simple and whose purpose is to be the base of
    other testing algorithms.
  - WaitingTimeEstimator, whose purpose is only to test the support of the
    ``estimate_waiting_time`` QUERY.

### Changed
- Batsched arguments are now parsed by [taywee/args][taywee/args]
  rather than [boost::po][boost::po].
- Support Batsim version 2.0.0:
  - two-way QUERY/REPLY (+estimate_waiting_time)
  - SET_JOB_METADATA
- Changed algorithms:
  - Conservative BF now supports the ``estimate_waiting_time`` QUERY.
  - Filler now supports a custom mapping and setting job metadata.
  - Submitter now supports setting job metadata.

[//]: =========================================================================
## 1.0.0 - 2017-11-20
Initial release.

[//]: =========================================================================
[changelog]: http://keepachangelog.com/en/1.0.0/
[semver]: http://semver.org/spec/v2.0.0.html
[taywee/args]: https://github.com/Taywee/args
[boost::po]: http://www.boost.org/doc/libs/1_66_0/doc/html/program_options.html

[Batsim v2.0.0]: https://github.com/oar-team/batsim/blob/master/doc/changelog.md#200---2018-02-20

[Unreleased]: https://gitlab.inria.fr/batsim/batsched/compare/v1.1.0...master
[1.1.0]: https://gitlab.inria.fr/batsim/batsched/compare/v1.0.0...v1.1.0
