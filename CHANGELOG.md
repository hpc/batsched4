# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog][changelog].
Batsched adheres to [Semantic Versioning][semver] and its public API is the following.

- Batched's command-line interface.
- The format of Batsched's input files.
- **Important note**: Changes in the Batsim protocol does not impact
  Batsched's public API.

[//]: =========================================================================
## [Unreleased]

[//]: =========================================================================
## [1.4.0] - 2020-07-29 - For [Batsim v4.0.0][Batsim v4.0.0]
### Added
- New `fcfs` algorithm (copied from `fcfs_fast`) that takes into account
  the resources selector given in parameter.

### Fixed
- The `easy_bf_fast` did not try to backfill previously submitted jobs in many
  events (when the priority job could not be executed).

[//]: =========================================================================
## [1.3.0] - 2019-01-15 - For [Batsim v3.0.0][Batsim v3.0.0]
### Added
- CLI: logging verbosity can now be set with `--verbosity`.

### Changed
- Dependencies: added [intervalset](https://framagit.org/batsim/intervalset).
- Dependencies: added [loguru](https://github.com/emilk/loguru).

[//]: =========================================================================
## [1.2.1] - 2018-07-03 - For [Batsim v2.0.0][Batsim v2.0.0]
### Fixed
- The `sleeper` algorithm continued to send requests when the simulation was
  finished, which should now be fixed.
- The `easy_bf_fast` algorithm rejected jobs that requested all the machines
  if they could not be executed directly after being submitted
  ([issue 6](https://gitlab.inria.fr/batsim/batsched/issues/6)).

### Changed
- The `submitter` algorithm now also sets metadata for usual jobs.

[//]: =========================================================================
## [1.2.0] - 2018-04-09 - For [Batsim v2.0.0][Batsim v2.0.0]
### Added
- New algorithms:
  - ``easy_bf_fast``, which is an efficient (usual) implementation of EASY
    backfilling. In contrast with the robust and general ``easy_bf``
    implementation, this one does floating-point computation, only handles
    jobs will walltimes, use ad-hoc structures for its simple backfilling
    mechanism (rather than a general-purpose 2D partition), only handles
    the FCFS queue order (rather than sorting the queue at each event),
    uses the first reservation of the priority job (rather than compressing
    the reservation as soon as possible at each event), and tries to only
    call the needed code depending on which event occured.
  - ``fcfs_fast``, which is essentially ``easy_bf_fast`` without backfilling.

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

[Batsim v2.0.0]: https://batsim.readthedocs.io/en/latest/changelog.html#v2-0-0
[Batsim v3.0.0]: https://batsim.readthedocs.io/en/latest/changelog.html#v3-0-0
[Batsim v4.0.0]: https://batsim.readthedocs.io/en/latest/changelog.html#v4-0-0

[Unreleased]: https://gitlab.inria.fr/batsim/batsched/compare/v1.4.0...master
[1.4.0]: https://gitlab.inria.fr/batsim/batsched/compare/v1.3.0...v1.4.0
[1.3.0]: https://gitlab.inria.fr/batsim/batsched/compare/v1.2.1...v1.3.0
[1.2.1]: https://gitlab.inria.fr/batsim/batsched/compare/v1.2.0...v1.2.1
[1.2.0]: https://gitlab.inria.fr/batsim/batsched/compare/v1.1.0...v1.2.0
[1.1.0]: https://gitlab.inria.fr/batsim/batsched/compare/v1.0.0...v1.1.0
