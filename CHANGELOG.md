# Changelog

## [0.2.34](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.33...v0.2.34) (2026-07-23)


### Features

* add aditional activity state and refactor port disconnect callback ([6b4c3f0](https://github.com/zalf-rpm/mas_python_fbp/commit/6b4c3f0a69730c8c060165f3e4be44524880e1ae))
* add component templates ([c727526](https://github.com/zalf-rpm/mas_python_fbp/commit/c72752677c40458fe63fd09c797cf6a714ea41c0))
* add loadbalancing component ([1eb5a3c](https://github.com/zalf-rpm/mas_python_fbp/commit/1eb5a3c23fe59e1b62854a5871840c897e37ce83))
* add more dakis components ([4dd2e63](https://github.com/zalf-rpm/mas_python_fbp/commit/4dd2e6335bebac85c286acb0bcc8355a19c4b4a0))
* add next available distribution strategy and make load balancer configurable in that regard ([9ea9de4](https://github.com/zalf-rpm/mas_python_fbp/commit/9ea9de48a83944cc9351a170c66cfa3d0464d949))
* add option to type and validate the config ([c75893c](https://github.com/zalf-rpm/mas_python_fbp/commit/c75893c276f0813e1de407e1fe908ea46677e7c1))
* add per channel instantiation gateway registration ([c421796](https://github.com/zalf-rpm/mas_python_fbp/commit/c421796ab7a37edb9de9c095c0a664fee107068e))
* add process info instead of error to gain more information ([2f457c0](https://github.com/zalf-rpm/mas_python_fbp/commit/2f457c0b3971f38448daabd03bbf8a590c4956ff))
* add rbs download component for dakis ([00e1580](https://github.com/zalf-rpm/mas_python_fbp/commit/00e1580cbd15f2e4728f3d40720b75e7ef8ade7e))
* allow also json when passing unstructured text ([18c87d1](https://github.com/zalf-rpm/mas_python_fbp/commit/18c87d123ed7b2e538340d4f714bf0bbca232574))
* **channel-startup:** add config option to reususe registry for channel sturdyrefs for globally accessible channels ([ec219af](https://github.com/zalf-rpm/mas_python_fbp/commit/ec219afae4ff2268ba7baa74135ac9f2b5e25cd3))
* **components:** apply startup conf handling ([af5dab9](https://github.com/zalf-rpm/mas_python_fbp/commit/af5dab9063e05b6754f0a078fa8e7b00d0960e91))
* extract input and output code ([1520f6d](https://github.com/zalf-rpm/mas_python_fbp/commit/1520f6d9fdac63fb64f74754422b8f0bac4fa784))
* further refactor process class ([d976284](https://github.com/zalf-rpm/mas_python_fbp/commit/d9762849a578c58e26d52e082f1162cf7e2c1505))
* have docker dev and prod stage for easier hot reloading  also use pixi locked install for reproducability ([e91d48b](https://github.com/zalf-rpm/mas_python_fbp/commit/e91d48b94de8d24dd896788adcaf7cf8c2b91300))
* improve type safety by having a metadata pydantic model and validating it also for command line args ([f5d8d0c](https://github.com/zalf-rpm/mas_python_fbp/commit/f5d8d0ca0128503ad422a4fb803d5caffa8ea41e))
* make exception handling more clear and inline with linting rules ([ed1d2c9](https://github.com/zalf-rpm/mas_python_fbp/commit/ed1d2c96beef485b08429362b17d84ce9a3a0469))
* migrate to typed metadata shape ([f8b09b7](https://github.com/zalf-rpm/mas_python_fbp/commit/f8b09b76fb43c113e93b7bfb1c22b4ac91db92c8))
* moved a few components to Process pattern ([96cb8a3](https://github.com/zalf-rpm/mas_python_fbp/commit/96cb8a3568fe3bf2dc7ffde94ce926d6ae549d1e))
* moved local config to Python datastructures instead of common_capnp:Value's ([5e8986a](https://github.com/zalf-rpm/mas_python_fbp/commit/5e8986a1fac3bd99405234771dd65917be3140b0))
* **process:** add startup config support ([b7fe35e](https://github.com/zalf-rpm/mas_python_fbp/commit/b7fe35e0d2ddc385e904212b2bdf037d681d1250))
* refactor process and add new mimetyped blop ip ([dbb45aa](https://github.com/zalf-rpm/mas_python_fbp/commit/dbb45aa140a57d295c4f03619f26e1f819dcca1e))
* resolve type through the new helper in to string and use sys attribute on ip as principal type description ([704dbca](https://github.com/zalf-rpm/mas_python_fbp/commit/704dbcae73f3448e5a50a8794fa339522989f438))
* reuse the component config to generate the metadata format to have a single source of truth ([60ecbc9](https://github.com/zalf-rpm/mas_python_fbp/commit/60ecbc99ce6656a16f7729c7e5f1e660bdc44095))
* save last error and return it on demand incase of crash to report feedback  to the ui or consumer ([461dacd](https://github.com/zalf-rpm/mas_python_fbp/commit/461dacdb7da0e952d48eb4ff9eb5429af8dd0ce1))


### Bug Fixes

* activity state for automatic bracketet ports ([b8ee423](https://github.com/zalf-rpm/mas_python_fbp/commit/b8ee42357fde6336b4774aa0c442d28e9e73a710))
* add environment to basedpyright config so pycharm can use it without the plugin ([f7c864f](https://github.com/zalf-rpm/mas_python_fbp/commit/f7c864f24a051a9d0ae3ddbeafcfd60a74f659c9))
* **array port:** broadcast strategy sending one by one instead of all at once ([4330ba8](https://github.com/zalf-rpm/mas_python_fbp/commit/4330ba84d770461ec2deb519d893457cbdab32d1))
* check name for port ([87bfb7a](https://github.com/zalf-rpm/mas_python_fbp/commit/87bfb7afe6f42eed646859c0fd4834b9cac749cd))
* copy op is unnecessary and doesn't take open/close brackets etc ([2c3c482](https://github.com/zalf-rpm/mas_python_fbp/commit/2c3c482cb925ed7eccd6808abcce187744a393c8))
* finl correction of project description and save enablement of ruff and pyright ([ea1d979](https://github.com/zalf-rpm/mas_python_fbp/commit/ea1d979bfc057e5a91f24350279c7e44323903f5))
* fixed casting ([32ee55d](https://github.com/zalf-rpm/mas_python_fbp/commit/32ee55dd5b9eb69b4f159e0a45092721b94e4aea))
* fixed port access ([4e0337c](https://github.com/zalf-rpm/mas_python_fbp/commit/4e0337c99f255b0d1fc70c4166ca7e10bb8dd2b0))
* fixed some issues due to new method of casting ([30d872a](https://github.com/zalf-rpm/mas_python_fbp/commit/30d872a5ea613e882c3287976ccbed2eb9dd3ce4))
* now sending/receiving msgs works from worker thread (via main ([5fff016](https://github.com/zalf-rpm/mas_python_fbp/commit/5fff016f81b744940d3003ac92964fedbbf6fa19))
* pyproject.toml to use correct pixi environments and put dev deps in the correct environment ([a24f47d](https://github.com/zalf-rpm/mas_python_fbp/commit/a24f47d59babb8f766b744287fdd19fa3ede3acb))
* remove .t accessor from distribution strategy in load balancer ([f50c10d](https://github.com/zalf-rpm/mas_python_fbp/commit/f50c10db8176612b97c35059c3325b7a45a2812a))
* remove last usage of .t accessor ([0ebeb13](https://github.com/zalf-rpm/mas_python_fbp/commit/0ebeb13835ffce080ab633b3f848ea8505acf7ac))
* remove poetry.lock copy in dockerfile that doesnt exist anymore also update deps ([09ab059](https://github.com/zalf-rpm/mas_python_fbp/commit/09ab059498adf4453ce8e7a14b1b256eafde617e))
* rename to automatic chunking to respect other bracketed applications ([fcd7f46](https://github.com/zalf-rpm/mas_python_fbp/commit/fcd7f461205b3307e0d3c5b477475cbe2ea8968c))
* selectively turned off pyright errors ([730d0a7](https://github.com/zalf-rpm/mas_python_fbp/commit/730d0a795732796738144307587af86eb2ea6b58))
* sent not the value, but the inside list ([bb409a6](https://github.com/zalf-rpm/mas_python_fbp/commit/bb409a62a25a74c8deba8d310b57f23a25ca6f86))
* set correct env for pycharm (maybe exclude it from git) ([8b19439](https://github.com/zalf-rpm/mas_python_fbp/commit/8b19439f799d1ebf2214e01201820fde3b6ecf80))
* use correct type for config_type ([d6e3777](https://github.com/zalf-rpm/mas_python_fbp/commit/d6e377794c9cb3919882f8616b5b7338d6b6c3a3))
* user correct type for value and lb instead of for boolean lists ([cf300fe](https://github.com/zalf-rpm/mas_python_fbp/commit/cf300fee75f26197461caed380e68352f20aa0c6))

## [0.2.33](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.32...v0.2.33) (2026-05-05)


### Features

* add documentation on how to create a component ([f07c32c](https://github.com/zalf-rpm/mas_python_fbp/commit/f07c32c84cea07b9f19cde130b8e53810d380104))
* add first dakis component for creating and empty geotiff ([ccfb4b1](https://github.com/zalf-rpm/mas_python_fbp/commit/ccfb4b1bf4e7480782d267573df96b4846fbb2f9))
* align the process and the port connector variant further and improve type safety by adding proper accessors ([868ac4e](https://github.com/zalf-rpm/mas_python_fbp/commit/868ac4ea0264f742aa54caa4d45b41d56bfd006a))
* **dakis:** filter geoparquet by raster ([44f6d4d](https://github.com/zalf-rpm/mas_python_fbp/commit/44f6d4d93ea129a22c5baa8ec65d3c6ed94ef343))
* **dakis:** relabel geoparquet geometries ([355fce0](https://github.com/zalf-rpm/mas_python_fbp/commit/355fce0689205e43f2f9de794b0d6d25429d0574))
* **dakis:** write geoparquet to disk ([177db6a](https://github.com/zalf-rpm/mas_python_fbp/commit/177db6ae598fac9508fa8ddbc02718d914178b1d))
* differntiate array ports and normal ports and clear up function and variable names ([2b666e4](https://github.com/zalf-rpm/mas_python_fbp/commit/2b666e471558a54aca13af07984fa6ec31db1512))
* **process:** add array ports and lifecycle helpers ([aaa153a](https://github.com/zalf-rpm/mas_python_fbp/commit/aaa153ae2698d8a6c96809762d6ba28b4c9d49cd))
* **process:** add managed process handles ([e716529](https://github.com/zalf-rpm/mas_python_fbp/commit/e71652922521da2e2cba6e4d1b37710cd4970a84))
* switch from print to logger ([1e3bcd5](https://github.com/zalf-rpm/mas_python_fbp/commit/1e3bcd51fa7697987ac254b58c2317037122743f))
* switch to duckdb for more efficient querying ([c5b83a6](https://github.com/zalf-rpm/mas_python_fbp/commit/c5b83a6207e2d882ebff204ec02a527f916f32df))


### Bug Fixes

* **console:** write output to stdout ([529da11](https://github.com/zalf-rpm/mas_python_fbp/commit/529da1144eb34f43a0d11581a0918d37e73df6f3))
* docker image permissions to edit the components cache ([0e89dd3](https://github.com/zalf-rpm/mas_python_fbp/commit/0e89dd3ac44e5bf726c115a074e72a8c3ea5a9c6))
* ensure output dir with proper permissions exists ([01d602e](https://github.com/zalf-rpm/mas_python_fbp/commit/01d602e42f6f4fb1641897c76ff602abbe115b70))
* missing import of Counter ([db9fa12](https://github.com/zalf-rpm/mas_python_fbp/commit/db9fa122d0f9273eb8100e79f993f3d2b999a170))
* **process:** handle default config values ([d66236e](https://github.com/zalf-rpm/mas_python_fbp/commit/d66236e6ff8c08c30f5185d2f7781fb85972b2f7))
* uuid of split string into list service ([ddddb04](https://github.com/zalf-rpm/mas_python_fbp/commit/ddddb04dfe1cc8b54effaebc27977e02673e66eb))
* wrong config key ([226dbcc](https://github.com/zalf-rpm/mas_python_fbp/commit/226dbccffe3e60bed1cf91948f167976fbf2d5d4))
* wrong schema for IP instantiation ([284bdb0](https://github.com/zalf-rpm/mas_python_fbp/commit/284bdb0fbaf46371e319803df9520d0b6dd32545))


### Documentation

* **components:** document bracket substreams ([7177bcb](https://github.com/zalf-rpm/mas_python_fbp/commit/7177bcb73213fb2236b9d6bf46ae5304e64c0909))

## [0.2.32](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.31...v0.2.32) (2026-04-21)


### Features

* local_components_cache.json ([8193a7c](https://github.com/zalf-rpm/mas_python_fbp/commit/8193a7cf1a4fead510876f846a17db85ea3ce208))

## [0.2.31](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.30...v0.2.31) (2026-04-20)


### Bug Fixes

* update channel binary ([367cccc](https://github.com/zalf-rpm/mas_python_fbp/commit/367cccc01ffffcd2a77d71743770bb263306c938))

## [0.2.30](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.29...v0.2.30) (2026-04-20)


### Features

* update pixi and and shipped channel binary ([e2db313](https://github.com/zalf-rpm/mas_python_fbp/commit/e2db3133c40658d12718d70d8289338709f372db))


### Bug Fixes

* add .pixi to docker ignore for lcoal dev ([2ede1f7](https://github.com/zalf-rpm/mas_python_fbp/commit/2ede1f7582c683127d96004b02db92d1828d56f9))

## [0.2.29](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.28...v0.2.29) (2026-02-12)


### Features

* bump minimum python version to 3.12 ([4b95f99](https://github.com/zalf-rpm/mas_python_fbp/commit/4b95f99297512b89db7fa149600de3f1fc737f51))
* stabilze versions ahead of python update ([9d12724](https://github.com/zalf-rpm/mas_python_fbp/commit/9d1272434c57e962345d48353af424917e77941f))

## [0.2.28](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.27...v0.2.28) (2026-01-15)


### Bug Fixes

* pin pixi to 0.62.0 due to a breaking change ([365af0e](https://github.com/zalf-rpm/mas_python_fbp/commit/365af0eb93d7b05f6fc0d6585cfbac53d173ea4e))

## [0.2.27](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.26...v0.2.27) (2025-11-19)


### Features

* use new capnproto schemas with stubs for type checking and push all dependencies to also require it ([093a3b9](https://github.com/zalf-rpm/mas_python_fbp/commit/093a3b930cd0f72f9c98b792d138667953d4822a))

## [0.2.26](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.25...v0.2.26) (2025-10-27)


### Features

* **deps:** update maximum python version to 3.13 and dependencies accordingly ([10b1840](https://github.com/zalf-rpm/mas_python_fbp/commit/10b18402536bf4540a6b7e9efa7d088224659c8e))
* use last version mas_python_common ([bfc22e7](https://github.com/zalf-rpm/mas_python_fbp/commit/bfc22e7e7d1f124ab922539b85829d243b17291a))

## [0.2.25](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.24...v0.2.25) (2025-10-02)


### Bug Fixes

* make output unbuffered otherwise docker cant read the console output of the subprocess ([3f3eed1](https://github.com/zalf-rpm/mas_python_fbp/commit/3f3eed11ddd62db220c42f0b46e6f377f9bb5648))

## [0.2.24](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.23...v0.2.24) (2025-10-02)


### Bug Fixes

* make channel host optional and make config extraction more robust ([f940a8e](https://github.com/zalf-rpm/mas_python_fbp/commit/f940a8ee31fa0efb695ac0e27257ef07c020ec23))

## [0.2.23](https://github.com/zalf-rpm/mas_python_fbp/compare/v0.2.22...v0.2.23) (2025-10-02)


### Bug Fixes

* use updated binary that resolves the ip address correctly ([cb05321](https://github.com/zalf-rpm/mas_python_fbp/commit/cb05321b571f618fc9fd461698e18eed2d2b474f))

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
