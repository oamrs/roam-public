# Changelog

## [0.7.0](https://github.com/oamrs/roam/compare/oam-v0.6.0...oam-v0.7.0) (2026-04-20)


### Features

* OrgRateLimitProvider, SchemaModePolicy, AgentSessionPolicy moat traits ([93bb7dd](https://github.com/oamrs/roam/commit/93bb7ddbdea20584566229b2628e3c80cd33000e))
* ORM metadata introspection + E2E Dolt branch isolation ([07809ba](https://github.com/oamrs/roam/commit/07809ba9f975f8d69b1443e650c07b253be54de9))
* ORM metadata introspection + E2E Dolt branch isolation ([bc33ca6](https://github.com/oamrs/roam/commit/bc33ca636809beb43bebc36145dddcc9dbbad8ec))
* RLS/CLS authorization middleware with DoltPolicyProvider ([4796b1d](https://github.com/oamrs/roam/commit/4796b1dc649386b07880c2e96e6764d253fa67e9))
* RLS/CLS authorization middleware with DoltPolicyProvider ([ee51c0f](https://github.com/oamrs/roam/commit/ee51c0fbb4455c670dd85fef0de5db2ca2d09825))
* widen open-core moat — AgentMemory, AuditExporter, PolicyPlugin, ApprovalGate, IdentityProvider traits ([c2e2f1c](https://github.com/oamrs/roam/commit/c2e2f1c886e754847df0ed3600372dbb378cc6d5))


### Bug Fixes

* address Copilot review — security, correctness and doc issues ([18875b6](https://github.com/oamrs/roam/commit/18875b6f411afcda4ab1f24f3a3694d4004fe273))
* address PR review comments ([4e4e792](https://github.com/oamrs/roam/commit/4e4e792fc98df2d6bb21ef470936a342d872443c))

## [0.6.0](https://github.com/oamrs/roam/compare/oam-v0.5.0...oam-v0.6.0) (2026-04-19)


### Features

* enforce schema validation in grpc_executor before executing queries ([ca72588](https://github.com/oamrs/roam/commit/ca72588da9256ba3eb3ba3f32507f1e0de382d58))
* **mirror:** add default_value to Column and non-unique Index support ([17c196d](https://github.com/oamrs/roam/commit/17c196d07fe57cd736515d167107c9017bf571c7))
* **mirror:** add default_value to Column and non-unique Index support ([bb9f18d](https://github.com/oamrs/roam/commit/bb9f18de9a7b75673e617d387f5c2e727f035907)), closes [#36](https://github.com/oamrs/roam/issues/36)


### Bug Fixes

* propagate augmentation event metadata through ValidationResponse ([e4bb405](https://github.com/oamrs/roam/commit/e4bb405bf0cbb018509a5b81d1eaa24efbe26f84))
* rustfmt formatting and remove registered table list from error message ([924ed08](https://github.com/oamrs/roam/commit/924ed089dc1028fdb26430a1ae8bceb88a53c1a2))

## [0.5.0](https://github.com/oamrs/roam/compare/oam-v0.4.0...oam-v0.5.0) (2026-04-19)


### Features

* Agent Memory in Dolt branches ([ece6c90](https://github.com/oamrs/roam/commit/ece6c90b5cbed1421124d7b008b6232b95b65351))

## [0.4.0](https://github.com/oamrs/roam/compare/oam-v0.3.0...oam-v0.4.0) (2026-04-18)


### Features

* constraint-to-JSON-schema mapping across Rust, Python, and .NET SDKs ([a7802dd](https://github.com/oamrs/roam/commit/a7802dd90e45ffb4e991c14ac98391c9fdc51bff))


### Bug Fixes

* address PR review comments ([b2c8ae0](https://github.com/oamrs/roam/commit/b2c8ae00ec169f9e4f6ea5685406b06a023daa43))
* **deps:** bump oam-schema and oam-proto deps to 0.3.0 in roam-public ([1fca63c](https://github.com/oamrs/roam/commit/1fca63c03a61973c95e980a0ab6076f63414e036))
* **publish:** remove invalid 'memory:' file from roam-public ([7748567](https://github.com/oamrs/roam/commit/7748567c2ac74bfa71258aacb8bb7318f33f95fe))

## [0.3.0](https://github.com/oamrs/roam/compare/oam-v0.2.0...oam-v0.3.0) (2026-04-18)


### Features

* add prompt hook audit events ([f694cc9](https://github.com/oamrs/roam/commit/f694cc9be5abf97d944494b8c23825a3115e1aa2))
* build prompt hook resolve requests ([b1081fa](https://github.com/oamrs/roam/commit/b1081fa6eabbab1c9b7e0e236f68e6a2e07ebb3b))
* **cd:** publish oam-proto and oam to crates.io on public-v* tag ([d815629](https://github.com/oamrs/roam/commit/d815629a43c2a8a703ef1ae69b0872788442c2f1))
* **cd:** publish roam-schema, roam-proto, oam to crates.io on public-v* tag ([4f0db02](https://github.com/oamrs/roam/commit/4f0db020525f0f3930d9d11f0e09260e8d9c8a1a))
* **cd:** publish roam-schema, roam-proto, oam to crates.io on public… ([9c50303](https://github.com/oamrs/roam/commit/9c503038460a8dcec5f048c0af5a00d9b21e861c))
* execution engine architecture with connection pool ([5ee9ca6](https://github.com/oamrs/roam/commit/5ee9ca636545e702f1eefb0911113fa0b5c20e6f))
* implement asynchronous execution engine with result management and task cancellation ([14996c9](https://github.com/oamrs/roam/commit/14996c99c51445f37e68471e604abae7bdfa9012))
* implement connection pooling and improve error handling ([df170bf](https://github.com/oamrs/roam/commit/df170bfdec36bd95ab561caf5a88bcc3b2d9f750))
* JSON-based request/response over TCP (simplified from full gRPC for MVP) ([02b0996](https://github.com/oamrs/roam/commit/02b099698a4027327a116e2f15270c4ec09ccb30))
* LLM-ready schemas from SeaORM models and gRPC request execution with tonic ([b8ee351](https://github.com/oamrs/roam/commit/b8ee35172bd333a66f39a795cbd2c517e6cd5a75))
* **policy:** add semantic P2SQL engine with neutral OSS policy context ([c672aa0](https://github.com/oamrs/roam/commit/c672aa0f8459244c00001886e2e853205f27f7bb))
* **policy:** add semantic SQL policy engine and neutral OSS policy context ([236f885](https://github.com/oamrs/roam/commit/236f885dd7624dbef962bb20224f4db1db00bf41))
* propagate runtime query context ([06fce2f](https://github.com/oamrs/roam/commit/06fce2fe245b29d39d6ac9b8d8654c67ed013f56))
* **python-sdk:** implement api-key auth & integration test runner ([c1327e8](https://github.com/oamrs/roam/commit/c1327e87ca1f55e31de327dd00279a0fd3748ae1))
* **python:** replace rust bindings with idiomatic python sdk ([47c1982](https://github.com/oamrs/roam/commit/47c19821db68b2b0de825537088f9b0f8cef2743))
* **python:** replace rust bindings with idiomatic python sdk ([5f8c37e](https://github.com/oamrs/roam/commit/5f8c37e2db4084f96625b27ff01795b015e2c516))
* query execution with event dispatch integration ([e699ef3](https://github.com/oamrs/roam/commit/e699ef3eb7aa59c812a42b5a3e55c1ba09a94413))
* register runtime agent sessions ([1f1a690](https://github.com/oamrs/roam/commit/1f1a690bf5009c37a458b25596630f8b8249c4ff))
* **release:** add release-please automation and rename roam-schema to oam-schema ([4c21e5b](https://github.com/oamrs/roam/commit/4c21e5b0fe19723e8f50853de7d0311971d6ac84))
* Scaffold SDKs, Hardware lib, and update Docs infrastructure ([3c4f0d5](https://github.com/oamrs/roam/commit/3c4f0d502a63c9d9a2fa3f47c5b434b4f47873e5))
* **sdk+backend:** Enforce DATA_ONLY mode restrictions & add TODO for backend validation ([abcae6f](https://github.com/oamrs/roam/commit/abcae6f7398f4f13817065cc5f5d5a6b9d89d256))
* **sdk+backend:** implement CODE_FIRST mode & backend enforcement TODO ([b2924e6](https://github.com/oamrs/roam/commit/b2924e66851df80509a8ba166a14aeea4fe92c20))
* **sdk+backend:** improve CODE_FIRST validation & add backend TODOs ([b37949c](https://github.com/oamrs/roam/commit/b37949c8fdc02222f2cbe99b90ab26a99e912a87))
* **sdk:** refactor python sdk to top-level library and add pypi publishing workflow ([c979aef](https://github.com/oamrs/roam/commit/c979aef3e2c32cc11fe39c083739b515aa6171b6))
* wire persisted prompt hook resolution ([b5d809d](https://github.com/oamrs/roam/commit/b5d809d98d1bfc280344a7c64e6960004cbc3ea9))
* wire runtime prompt hook resolution ([eccd2ed](https://github.com/oamrs/roam/commit/eccd2edc5dc4890df148730885d57fec5efc63c4))


### Bug Fixes

* Address code review feedback ([19ac575](https://github.com/oamrs/roam/commit/19ac5754511a9190a6bad61d67596a1560300d1b))
* address prompt hook review feedback ([a236048](https://github.com/oamrs/roam/commit/a236048e598384a3593471fd2c18eaaa7685e551))
* align pre-commit with ci checks ([5de1dcb](https://github.com/oamrs/roam/commit/5de1dcbf42f5cdf69ea996b816e2fc77df9a4a85))
* **backend:** resolve rocket_okapi and roam-public compile errors ([0edf8c8](https://github.com/oamrs/roam/commit/0edf8c8ab2107baac5b52a7d7e1aabdab8c0d3f9))
* deduplicate executor policy validation ([f8f144d](https://github.com/oamrs/roam/commit/f8f144d355ee700ffedc93571890d34899ab4879))
* handle qualified subquery tables in policy engine ([7d271cc](https://github.com/oamrs/roam/commit/7d271cc6e89d0c1447146dffcc1bfc9e998b3149))
* parse executor FROM clauses by SQL token ([b3af3b1](https://github.com/oamrs/roam/commit/b3af3b164d96ef9c44fc6c06d78ed42f661c2924))
* **quality:** Address lints in FFI and proto definition ([08054be](https://github.com/oamrs/roam/commit/08054be7bffb8cc4df5922fa2a1d29d47c44d04d))
* resolved copilot comments ([170074f](https://github.com/oamrs/roam/commit/170074fa6c1dd33bed639b2f30b0760e0baaaee0))
* **roam-public:** avoid direct IndexMap/BTreeMap construction in to_json_schema ([a213cbc](https://github.com/oamrs/roam/commit/a213cbccf3fd35899b8f2bfa051c3380fcf4d953))
* **rust:** resolve clippy warnings ([e802738](https://github.com/oamrs/roam/commit/e80273809637270ec1d23351e8357578e054cd02))
* stabilize ci grpc startup ([b05fb14](https://github.com/oamrs/roam/commit/b05fb1448fc11323c98fb818f958c9faae0af677))
* support quoted subquery identifiers ([8e767a7](https://github.com/oamrs/roam/commit/8e767a7ee8b3f2c1522a0da9514122f6b53ffa86))
* tighten subquery policy enforcement ([da28c9f](https://github.com/oamrs/roam/commit/da28c9f5e8e2e904080405dad696ced8af9e4a3a))
* tighten sync and TCP policy handling ([fc075f4](https://github.com/oamrs/roam/commit/fc075f4cf4c2b772b0e9243c1765a0308421d783))


### Performance Improvements

* **ci:** run quality-checks before tests, collapse test+coverage builds ([ece8887](https://github.com/oamrs/roam/commit/ece8887487aabde9935db548da73c0048d36b777))

## [0.2.0](https://github.com/oamrs/roam/compare/oam-vv0.1.1...oam-vv0.2.0) (2026-04-18)


### Features

* add prompt hook audit events ([f694cc9](https://github.com/oamrs/roam/commit/f694cc9be5abf97d944494b8c23825a3115e1aa2))
* build prompt hook resolve requests ([b1081fa](https://github.com/oamrs/roam/commit/b1081fa6eabbab1c9b7e0e236f68e6a2e07ebb3b))
* **cd:** publish oam-proto and oam to crates.io on public-v* tag ([d815629](https://github.com/oamrs/roam/commit/d815629a43c2a8a703ef1ae69b0872788442c2f1))
* **cd:** publish roam-schema, roam-proto, oam to crates.io on public-v* tag ([4f0db02](https://github.com/oamrs/roam/commit/4f0db020525f0f3930d9d11f0e09260e8d9c8a1a))
* **cd:** publish roam-schema, roam-proto, oam to crates.io on public… ([9c50303](https://github.com/oamrs/roam/commit/9c503038460a8dcec5f048c0af5a00d9b21e861c))
* execution engine architecture with connection pool ([5ee9ca6](https://github.com/oamrs/roam/commit/5ee9ca636545e702f1eefb0911113fa0b5c20e6f))
* implement asynchronous execution engine with result management and task cancellation ([14996c9](https://github.com/oamrs/roam/commit/14996c99c51445f37e68471e604abae7bdfa9012))
* implement connection pooling and improve error handling ([df170bf](https://github.com/oamrs/roam/commit/df170bfdec36bd95ab561caf5a88bcc3b2d9f750))
* JSON-based request/response over TCP (simplified from full gRPC for MVP) ([02b0996](https://github.com/oamrs/roam/commit/02b099698a4027327a116e2f15270c4ec09ccb30))
* LLM-ready schemas from SeaORM models and gRPC request execution with tonic ([b8ee351](https://github.com/oamrs/roam/commit/b8ee35172bd333a66f39a795cbd2c517e6cd5a75))
* **policy:** add semantic P2SQL engine with neutral OSS policy context ([c672aa0](https://github.com/oamrs/roam/commit/c672aa0f8459244c00001886e2e853205f27f7bb))
* **policy:** add semantic SQL policy engine and neutral OSS policy context ([236f885](https://github.com/oamrs/roam/commit/236f885dd7624dbef962bb20224f4db1db00bf41))
* propagate runtime query context ([06fce2f](https://github.com/oamrs/roam/commit/06fce2fe245b29d39d6ac9b8d8654c67ed013f56))
* **python-sdk:** implement api-key auth & integration test runner ([c1327e8](https://github.com/oamrs/roam/commit/c1327e87ca1f55e31de327dd00279a0fd3748ae1))
* **python:** replace rust bindings with idiomatic python sdk ([47c1982](https://github.com/oamrs/roam/commit/47c19821db68b2b0de825537088f9b0f8cef2743))
* **python:** replace rust bindings with idiomatic python sdk ([5f8c37e](https://github.com/oamrs/roam/commit/5f8c37e2db4084f96625b27ff01795b015e2c516))
* query execution with event dispatch integration ([e699ef3](https://github.com/oamrs/roam/commit/e699ef3eb7aa59c812a42b5a3e55c1ba09a94413))
* register runtime agent sessions ([1f1a690](https://github.com/oamrs/roam/commit/1f1a690bf5009c37a458b25596630f8b8249c4ff))
* **release:** add release-please automation and rename roam-schema to oam-schema ([4c21e5b](https://github.com/oamrs/roam/commit/4c21e5b0fe19723e8f50853de7d0311971d6ac84))
* Scaffold SDKs, Hardware lib, and update Docs infrastructure ([3c4f0d5](https://github.com/oamrs/roam/commit/3c4f0d502a63c9d9a2fa3f47c5b434b4f47873e5))
* **sdk+backend:** Enforce DATA_ONLY mode restrictions & add TODO for backend validation ([abcae6f](https://github.com/oamrs/roam/commit/abcae6f7398f4f13817065cc5f5d5a6b9d89d256))
* **sdk+backend:** implement CODE_FIRST mode & backend enforcement TODO ([b2924e6](https://github.com/oamrs/roam/commit/b2924e66851df80509a8ba166a14aeea4fe92c20))
* **sdk+backend:** improve CODE_FIRST validation & add backend TODOs ([b37949c](https://github.com/oamrs/roam/commit/b37949c8fdc02222f2cbe99b90ab26a99e912a87))
* **sdk:** refactor python sdk to top-level library and add pypi publishing workflow ([c979aef](https://github.com/oamrs/roam/commit/c979aef3e2c32cc11fe39c083739b515aa6171b6))
* wire persisted prompt hook resolution ([b5d809d](https://github.com/oamrs/roam/commit/b5d809d98d1bfc280344a7c64e6960004cbc3ea9))
* wire runtime prompt hook resolution ([eccd2ed](https://github.com/oamrs/roam/commit/eccd2edc5dc4890df148730885d57fec5efc63c4))


### Bug Fixes

* Address code review feedback ([19ac575](https://github.com/oamrs/roam/commit/19ac5754511a9190a6bad61d67596a1560300d1b))
* address prompt hook review feedback ([a236048](https://github.com/oamrs/roam/commit/a236048e598384a3593471fd2c18eaaa7685e551))
* align pre-commit with ci checks ([5de1dcb](https://github.com/oamrs/roam/commit/5de1dcbf42f5cdf69ea996b816e2fc77df9a4a85))
* **backend:** resolve rocket_okapi and roam-public compile errors ([0edf8c8](https://github.com/oamrs/roam/commit/0edf8c8ab2107baac5b52a7d7e1aabdab8c0d3f9))
* deduplicate executor policy validation ([f8f144d](https://github.com/oamrs/roam/commit/f8f144d355ee700ffedc93571890d34899ab4879))
* handle qualified subquery tables in policy engine ([7d271cc](https://github.com/oamrs/roam/commit/7d271cc6e89d0c1447146dffcc1bfc9e998b3149))
* parse executor FROM clauses by SQL token ([b3af3b1](https://github.com/oamrs/roam/commit/b3af3b164d96ef9c44fc6c06d78ed42f661c2924))
* **quality:** Address lints in FFI and proto definition ([08054be](https://github.com/oamrs/roam/commit/08054be7bffb8cc4df5922fa2a1d29d47c44d04d))
* resolved copilot comments ([170074f](https://github.com/oamrs/roam/commit/170074fa6c1dd33bed639b2f30b0760e0baaaee0))
* **roam-public:** avoid direct IndexMap/BTreeMap construction in to_json_schema ([a213cbc](https://github.com/oamrs/roam/commit/a213cbccf3fd35899b8f2bfa051c3380fcf4d953))
* **rust:** resolve clippy warnings ([e802738](https://github.com/oamrs/roam/commit/e80273809637270ec1d23351e8357578e054cd02))
* stabilize ci grpc startup ([b05fb14](https://github.com/oamrs/roam/commit/b05fb1448fc11323c98fb818f958c9faae0af677))
* support quoted subquery identifiers ([8e767a7](https://github.com/oamrs/roam/commit/8e767a7ee8b3f2c1522a0da9514122f6b53ffa86))
* tighten subquery policy enforcement ([da28c9f](https://github.com/oamrs/roam/commit/da28c9f5e8e2e904080405dad696ced8af9e4a3a))
* tighten sync and TCP policy handling ([fc075f4](https://github.com/oamrs/roam/commit/fc075f4cf4c2b772b0e9243c1765a0308421d783))


### Performance Improvements

* **ci:** run quality-checks before tests, collapse test+coverage builds ([ece8887](https://github.com/oamrs/roam/commit/ece8887487aabde9935db548da73c0048d36b777))
