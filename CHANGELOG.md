# Changelog

## [0.2.22](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.21...v0.2.22) (2025-10-02)


### Bug Fixes

* set local_host in channel also (why is there a difference) ([5a74103](https://github.com/zalf-rpm/mas_python_fbp/commit/5a7410385b007175ea1db05157c91c8c0a44c448))

## [0.2.21](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.20...v0.2.21) (2025-10-02)


### Features

* add possibility to configure hostname of channels that are instantiated ([e9aff1b](https://github.com/zalf-rpm/mas_python_fbp/commit/e9aff1b11a41dcb56eb4b585818c2beef3b98b05))

## [0.2.20](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.19...v0.2.20) (2025-09-17)


### Bug Fixes

* id, name, description from config toml are being used now properly ([28378aa](https://github.com/zalf-rpm/mas_python_fbp/commit/28378aaca939f02803204ddf5d88f5cf87a8b711))

## [0.2.19](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.18...v0.2.19) (2025-09-12)


### Features

* fixed bug where accidentally all code was excluded from package ([6ff03ae](https://github.com/zalf-rpm/mas_python_fbp/commit/6ff03ae82e1da2747ac4b4c59f1ea766a0240da3))

## [0.2.18](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.17...v0.2.18) (2025-09-11)


### Bug Fixes

* for some reason some changes got lost ([748d013](https://github.com/zalf-rpm/mas_python_fbp/commit/748d013d1a984814753df5cc4d7ac7d974062301))

## [0.2.17](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.16...v0.2.17) (2025-09-11)


### Features

* use new import logic ([#18](https://github.com/zalf-rpm/mas_python_fbp/issues/18)) ([87a1fd5](https://github.com/zalf-rpm/mas_python_fbp/commit/87a1fd58f538f454d1c9156ad40583ab9b3cb8b5))

## [0.2.16](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.15...v0.2.16) (2025-08-29)


### Bug Fixes

* remove repository url and rely on default ([5299e41](https://github.com/zalf-rpm/mas_python_fbp/commit/5299e41146d859206f2b507abcd8dfd72b1f2ae2))

## [0.2.15](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.14...v0.2.15) (2025-08-29)


### Features

* add ruff config ([ff60a28](https://github.com/zalf-rpm/mas_python_fbp/commit/ff60a28a7e8744efb5e60f90566371c2da9b0d91))
* **CD:** push to pypi instead of test pypi ([e502def](https://github.com/zalf-rpm/mas_python_fbp/commit/e502def307be2a95e0db612e452be178fc451aca))

## [0.2.14](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.13...v0.2.14) (2025-08-25)


### Bug Fixes

* move other configs to config_dir ([ab5b0d1](https://github.com/zalf-rpm/mas_python_fbp/commit/ab5b0d1fab20bfc3a9e4ee2c4b278eb457ba9663))

## [0.2.13](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.12...v0.2.13) (2025-08-25)


### Bug Fixes

* import of components ([486f744](https://github.com/zalf-rpm/mas_python_fbp/commit/486f744ebf20996e10074b33de58df09bb45bc3f))

## [0.2.12](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.11...v0.2.12) (2025-08-25)


### Bug Fixes

* pull from test.pypi until everything is released to the real pypi ([fa61081](https://github.com/zalf-rpm/mas_python_fbp/commit/fa61081d27790c63ac44d12ce6706a3017f692ee))

## [0.2.11](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.10...v0.2.11) (2025-08-22)


### Features

* switch to trusted publishing on pypi ([319300e](https://github.com/zalf-rpm/mas_python_fbp/commit/319300eb9cbdd7d907eae72db5b99222d3cba44a))

## [0.2.10](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.9...v0.2.10) (2025-08-21)


### Features

* add executable docker image for run components ([#4](https://github.com/zalf-rpm/mas_python_fbp/issues/4)) ([3a378a8](https://github.com/zalf-rpm/mas_python_fbp/commit/3a378a828a521b43c686de4d41517fe1ad0586e8))
* add git example ([e6cb9fe](https://github.com/zalf-rpm/mas_python_fbp/commit/e6cb9fe8578136e46e01a9c8a77cb5a874537cb3))
* add git workflow for release and and push to docker and test.pypi ([676dc09](https://github.com/zalf-rpm/mas_python_fbp/commit/676dc091d48478fd974f8ab9e0df9911f7aae68d))
* switch to pep-621 compliant pyproject.toml storing project metadata under [project] ([85ce846](https://github.com/zalf-rpm/mas_python_fbp/commit/85ce8463c4967ec3b276c1f5bb9cee528e34f53b))
* switch to pep-621 compliant pyproject.toml storing project metadata under [project] ([6feb55d](https://github.com/zalf-rpm/mas_python_fbp/commit/6feb55d61fc2615b500c2fffb3d4683caf2ea8bb))
* try release workflow ([676dc09](https://github.com/zalf-rpm/mas_python_fbp/commit/676dc091d48478fd974f8ab9e0df9911f7aae68d))


### Bug Fixes

* add write permissions for issues ([676dc09](https://github.com/zalf-rpm/mas_python_fbp/commit/676dc091d48478fd974f8ab9e0df9911f7aae68d))
* revert back to poetry specific dependencies for local files ([6d6a529](https://github.com/zalf-rpm/mas_python_fbp/commit/6d6a5296a0674cc33aa0d4fcbaad89630d41a97e))
