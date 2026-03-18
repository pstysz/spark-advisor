# Changelog

## [0.1.13](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.12...spark-advisor-v0.1.13) (2026-03-18)


### Bug Fixes

* missing fe packages in Dockerfile ([6e93e72](https://github.com/pstysz/spark-advisor/commit/6e93e72bec67a412cab9584d1b472fe0375fbc85))
* missing fe packages in Dockerfile ([72a0a7e](https://github.com/pstysz/spark-advisor/commit/72a0a7e04a2e61fd5ce79b9d7d668a0e99c1212c))

## [0.1.12](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.11...spark-advisor-v0.1.12) (2026-03-18)


### Bug Fixes

* release please version ([aa2c3ea](https://github.com/pstysz/spark-advisor/commit/aa2c3ead971d62acb5854c6fe7af9e2adc814e10))

## [0.1.11](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.10...spark-advisor-v0.1.11) (2026-03-18)


### Features

* observability added ([0f36172](https://github.com/pstysz/spark-advisor/commit/0f361721b15470960ca20bee2b937f9f867c04fe))

## [0.1.10](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.9...spark-advisor-v0.1.10) (2026-03-16)


### Features

* frontend ([9e121e1](https://github.com/pstysz/spark-advisor/commit/9e121e17b4abc8df647e7564635e0f50be87c2f9))

## [0.1.9](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.8...spark-advisor-v0.1.9) (2026-03-15)


### Features

* endpoints for frontend ([b844df8](https://github.com/pstysz/spark-advisor/commit/b844df8dc52ee4363ca8751a8677459b2fc3001e))

## [0.1.8](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.7...spark-advisor-v0.1.8) (2026-03-15)


### Features

* added rerun tasks deduplication and hs poller with gateway integration ([b23c18b](https://github.com/pstysz/spark-advisor/commit/b23c18b3a074631644b81164d00d1f995694b8fc))

## [0.1.7](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.6...spark-advisor-v0.1.7) (2026-03-13)


### Features

* switch from in memory tasks store to sqlite + sqlalchemy ([6463350](https://github.com/pstysz/spark-advisor/commit/64633503f9acd5344ecb24bf8345ee076161e254))

## [0.1.6](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.5...spark-advisor-v0.1.6) (2026-03-06)


### Features

* added separate readme and pyproject descriptions in all modules ([f55ebfd](https://github.com/pstysz/spark-advisor/commit/f55ebfdcb4f7ca4e10d0753df9bc3b0a9d8664c2))


### Bug Fixes

* update internal dependencies to use pinned versions ([a3b2fe2](https://github.com/pstysz/spark-advisor/commit/a3b2fe265d064a408357e700a7464f6e8d949b7d))

## [0.1.5](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.4...spark-advisor-v0.1.5) (2026-03-05)


### Bug Fixes

* refactor + bugfixing ([99c7ca3](https://github.com/pstysz/spark-advisor/commit/99c7ca3567ecc198d0a3ea7d482831f550937b0e))
* refactor + bugfixing ([0232401](https://github.com/pstysz/spark-advisor/commit/023240115dd9c55cd4e48519ca56ff0d7a215ed5))

## [0.1.4](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.3...spark-advisor-v0.1.4) (2026-03-05)


### Features

* add helm charts ([e7b719a](https://github.com/pstysz/spark-advisor/commit/e7b719a0942a885dbecda3098f2d4e47215bd099))
* add k8s integration ([a20ee00](https://github.com/pstysz/spark-advisor/commit/a20ee0051fcdf3d5a555f918930ab386b2361a9e))
* added make command to deploy the whole stack on minikube ([090cad6](https://github.com/pstysz/spark-advisor/commit/090cad6e7ce85fd4868532776ea700d482710f5f))
* docker publish added ([ec028d7](https://github.com/pstysz/spark-advisor/commit/ec028d7f12779787b6828b69adf1b9ac5f60f55c))

## [0.1.3](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.2...spark-advisor-v0.1.3) (2026-03-05)


### Bug Fixes

* invalid uv build directory ([54f22bc](https://github.com/pstysz/spark-advisor/commit/54f22bcf7beb683a3a6f1a6f861363c9d934823b))

## [0.1.2](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.1...spark-advisor-v0.1.2) (2026-03-05)


### Bug Fixes

* cli readme bug ([834293c](https://github.com/pstysz/spark-advisor/commit/834293cf1ad70e3540007285bcb6c1bae8342fcc))

## [0.1.1](https://github.com/pstysz/spark-advisor/compare/spark-advisor-v0.1.0...spark-advisor-v0.1.1) (2026-03-05)


### Features

* add agent mode with multi-turn Claude tool_use loop ([89e67e7](https://github.com/pstysz/spark-advisor/commit/89e67e7067ed061d195cf8b817ee205d0256652b))
* add Docker Compose infrastructure and NATS error handling ([7096f86](https://github.com/pstysz/spark-advisor/commit/7096f86f1e81de6ded804f1540af9ed8831ec1e7))
* add MCP server with 5 tools for Claude Desktop and Cursor integration ([4f8cb47](https://github.com/pstysz/spark-advisor/commit/4f8cb47482de7296034f204322981bc5edfedc48))
* add spark-advisor-gateway REST API with NATS orchestration ([e2ba38a](https://github.com/pstysz/spark-advisor/commit/e2ba38ad6cab5d5be482a2551095b8e01a7d1de1))
* extract spark-advisor-analyzer as FastStream NATS service ([049d34d](https://github.com/pstysz/spark-advisor/commit/049d34df71c9cbab790eabb5679f0d6f882f202a))
* extract spark-advisor-cli as standalone CLI package ([1229117](https://github.com/pstysz/spark-advisor/commit/1229117c43e8c3a8f12c115c6a47804c63f40739))
* extract spark-advisor-hs-connector as FastStream NATS service ([f48ad30](https://github.com/pstysz/spark-advisor/commit/f48ad30acf0847ccf30d49cf79936d1de2d19b5d))
* extract spark-advisor-models as shared foundation package ([3022e34](https://github.com/pstysz/spark-advisor/commit/3022e346e06b85ec19dbb8b606b7859b40f573c0))
* extract spark-advisor-rules as dedicated rules engine package ([0b2289e](https://github.com/pstysz/spark-advisor/commit/0b2289e098406b685c8fd18ee373e77cdc5f7928))
* harden MVP with 5 new rules, CLI output options, and parser edge cases ([3f03b28](https://github.com/pstysz/spark-advisor/commit/3f03b289c7d765bd3ae69ad4148c0cf82ee59500))
* remove legacy packages and Kafka infrastructure ([671b10b](https://github.com/pstysz/spark-advisor/commit/671b10be2bd4d5b5a1c28c9d2f5a61488651bf91))
* versioning ([376273a](https://github.com/pstysz/spark-advisor/commit/376273a990b1a77d57db13af0092bbe121e6f425))


### Bug Fixes

* invalid logo url + docs ignore fix ([4b3b9eb](https://github.com/pstysz/spark-advisor/commit/4b3b9eb52748425a1d6c85182c15dbb08969f453))
