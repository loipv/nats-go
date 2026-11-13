# Changelog

## 1.0.0 (2026-11-13)


### ⚠ BREAKING CHANGES

* a *nats.ServiceError returned by App.Request is no longer forwarded when a handler returns it: return a new *nats.ServiceError to pass a downstream failure on.
* nats.Logger logs one Debug line per handled message ('nats: handled', with the duration and the error if any) and no longer logs failures at Error; the App already does.
* BatchMsg is PublishBatchMsg, BatchAck is AtomicBatchAck, Report is HealthReport, ServiceRef and GroupRef are Service and Group, WithJSPublishOpts is WithJSPublishOptions. The service example is examples/lifecycle.
* SubscribeReply, endpoints and Msg.RespondError no longer send the text of a handler error. Return a *nats.ServiceError to choose the code and the description. Other errors arrive as 500 "internal error", or 400 "bad request" when Terminal. Decode, validation and idempotency errors are still sent.
* scope idempotency per handler; replace Idempotent with WithIdempotency
* move atomicbatch.Publish to App.PublishJSAtomicBatch

### Features

* accept WithMsgID on core Publish and Request ([d513286](https://github.com/loipv/nats-go/commit/d5132862149f30c79f261c561d7ec7eb2cbc520c))
* add App lifecycle and connection options ([098452e](https://github.com/loipv/nats-go/commit/098452e55e5e5b7fdea8353c48ffd249b7a3299f))
* add App.Hook for start and stop hooks ([4fc72b1](https://github.com/loipv/nats-go/commit/4fc72b11f85ee917b91ca6fc2113dded1b095bcb))
* add atomic batch publish ([edfafdb](https://github.com/loipv/nats-go/commit/edfafdb09c5271f2458959f2478759c0dfce5515))
* add AutoInProgress for long-running JetStream handlers ([dfde660](https://github.com/loipv/nats-go/commit/dfde660dc8b85eb0503bf6fea1a5ae3dae15763c))
* add backoff, max attempts and DLQ for JetStream consumers ([b65a982](https://github.com/loipv/nats-go/commit/b65a98266ff0ea9a148fd2014b987e5929c17825))
* add CloudEvents emitter and handlers ([596e6e7](https://github.com/loipv/nats-go/commit/596e6e72cd780bd5b84c28998dc9714494df6812))
* add Consume and ConsumeOrdered for JetStream ([b538405](https://github.com/loipv/nats-go/commit/b538405fc9773a0df794c10895cfd0a63fbff3e3))
* add ConsumeBatch ([706f40d](https://github.com/loipv/nats-go/commit/706f40d2a2911ee64a0ad212ae5752c708ca3289))
* add event callback options ([347dbea](https://github.com/loipv/nats-go/commit/347dbea1aa182932d961fcf8954bdc61e94c6d16))
* add health report and bounded graceful shutdown ([87db97e](https://github.com/loipv/nats-go/commit/87db97ea7b3df6d1392a7c66f46c8acf68832273))
* add Idempotent middleware with in-memory store ([5540149](https://github.com/loipv/nats-go/commit/55401493b14a479db450c7625fe6edab89f66b9d))
* add KV-backed idempotency store ([ab00295](https://github.com/loipv/nats-go/commit/ab00295be8ab68ace2c63158264828c250c8dbad))
* add Msg.Respond and Msg.RespondError for Subscribe handlers ([709d0e1](https://github.com/loipv/nats-go/commit/709d0e1ee13131c2064b0cbc2d53a2fc9e5a7eff))
* add NATS micro services with typed endpoints ([854fc87](https://github.com/loipv/nats-go/commit/854fc873d685c1dee8eb0b1a70d1673f993cd6cc))
* add OpenTelemetry middleware ([eda5992](https://github.com/loipv/nats-go/commit/eda5992b1141b3f7b339640c80747e938a450b86))
* add Publish, Request and PublishJS with publish middleware ([551d346](https://github.com/loipv/nats-go/commit/551d346a3cd7642cef395b95a2b1604ac6280641))
* add PublishJSBatch ([2b2e687](https://github.com/loipv/nats-go/commit/2b2e68765d463a654053b89b811e8823e3972984))
* add retry, DLQ and batch handler options ([1a0c0e0](https://github.com/loipv/nats-go/commit/1a0c0e0b4dcd500541b56060a591edac763eed82))
* add Subscribe and Reply for core NATS ([8a7cf92](https://github.com/loipv/nats-go/commit/8a7cf9272fc7784b765684c046944f80a65c5c88))
* add typed handler, middleware chain and ack mapping ([1eecc5c](https://github.com/loipv/nats-go/commit/1eecc5c4b485db61c139cff219ac396b0a806a5c))
* declare and sync streams and consumers on start ([fd02239](https://github.com/loipv/nats-go/commit/fd02239760bf30d8304413f004e584ca194b69be))
* do not forward a ServiceError received from another service ([80beb1a](https://github.com/loipv/nats-go/commit/80beb1a3befeab7159b71a5fa68ace512459de86))
* emit asynchronous error events ([a208eb5](https://github.com/loipv/nats-go/commit/a208eb5b188ebc9b684f5bae0e0b8e7f41582061))
* emit connection events and chain user nats.go callbacks ([3422191](https://github.com/loipv/nats-go/commit/342219111f6b221f2642227393b164ccb182362c))
* emit handler restart and stop events ([e6f04d8](https://github.com/loipv/nats-go/commit/e6f04d83023b093643d181284d7b7a33c52e9b4e))
* emit message retry, failure and dead-letter events ([9c22c1a](https://github.com/loipv/nats-go/commit/9c22c1a24b203cea57435f878b1e74e1bda1049c))
* export Message, WrapPublish and Attempt for integrations ([4d89c7a](https://github.com/loipv/nats-go/commit/4d89c7a942c980ef8e307d4f83a1415468ae5264))
* lean on nats.go APIs and add WithJSPublishOpts ([fd0fdc7](https://github.com/loipv/nats-go/commit/fd0fdc7aecefbbaf013759d72979e4ad07f8c56f))
* make Logger an access log ([0720ec3](https://github.com/loipv/nats-go/commit/0720ec33bcd8ac84c51f28d3f4382c87f308423a))
* per-handler idempotency options with default scopes ([9f8faaa](https://github.com/loipv/nats-go/commit/9f8faaa1233eb7e59890efd2505fb91113020fed))
* provision modes Upsert, CreateOnly and Verify for declared streams and consumers ([21c67fd](https://github.com/loipv/nats-go/commit/21c67fd2c4add19e7fddb427120778eaa299c66f))
* recover handler panics in workers and expose Msg.Attempt ([bf09984](https://github.com/loipv/nats-go/commit/bf09984617229b1009e05db7efa881e52846ca6b))
* retry core, reply and ordered handlers in process ([233b4b8](https://github.com/loipv/nats-go/commit/233b4b82bee2937f3d5509bb7fc1021d6df22f5e))
* scaffold module with errors and codec ([3760957](https://github.com/loipv/nats-go/commit/37609574568d9293e3e0a54261e8cde0348577e2))
* scope idempotency per handler; replace Idempotent with WithIdempotency ([9c67002](https://github.com/loipv/nats-go/commit/9c67002684727c089ed0db793d9f87160a108e33))
* send a handler error to the requester only when it is meant for them ([803acdb](https://github.com/loipv/nats-go/commit/803acdb18e3fe20d57dbc925034631e502bb6f68))
* supervise JetStream consumers and restart with backoff ([97115ad](https://github.com/loipv/nats-go/commit/97115ad46fc93fd9d126fb1537c6f1cfc912761b))


### Bug Fixes

* ack and skip Idempotent duplicates in every worker ([394c34a](https://github.com/loipv/nats-go/commit/394c34aa177e701a1a821af0cf65efd506eee8d6))
* announce the micro-service example only once it is up ([815369f](https://github.com/loipv/nats-go/commit/815369f8dbb6fe10b8e7fb4ae99b866dd199fd6a))
* backoff overflow, retry stop check before zero backoff, AckWait refreshed on restart, no error on already-acked duplicate, first non-nil BatchError in a joined error; document LastError ([1f32efb](https://github.com/loipv/nats-go/commit/1f32efb52057e2a3723500aea82ab1e4aae05424))
* bound cancelled-Start teardown, read publish conns under lock, DISCONNECTED health after failed Start, set baseCtx under lock, apply nats options once ([2e07309](https://github.com/loipv/nats-go/commit/2e073092d535ff957aea2c301a05e0fb7242a189))
* bound shutdown, two-phase start, ordered consumer safety, reconnect forever by default ([25c723c](https://github.com/loipv/nats-go/commit/25c723cea305e356df908e1b98dabb5d858e0e9e))
* Conn and JetStream hide a failed attempt's closed connection, like Health ([7dc9bdc](https://github.com/loipv/nats-go/commit/7dc9bdc1a0b7ffb631f07937b085d07bb44a9ad1))
* drain buffered endpoint requests on shutdown ([45bb4e3](https://github.com/loipv/nats-go/commit/45bb4e3cb419e578ce9df602538b2113f9adff40))
* empty JetStream option values set no header, batch send and ack errors name their message, atomic batch rejects a middleware calling next twice ([80572c1](https://github.com/loipv/nats-go/commit/80572c104d52b17cab0f5f2955be523bbfe932d8))
* guard the example's dead-letter signal and give Shutdown its own timeout ([fef0e7c](https://github.com/loipv/nats-go/commit/fef0e7cdb017d608cb73763d4121c80d50f919a2))
* hide panic text from requesters, skip replyless endpoint requests, unique endpoint health names ([5f28f81](https://github.com/loipv/nats-go/commit/5f28f8168280fb9a5212f74ec1f0480b147e2bb1))
* keep acks that arrived before ctx expired in PublishJSBatch ([7955b00](https://github.com/loipv/nats-go/commit/7955b003e500d5a9866db78fdc47ce718fd48a13))
* keep ClosedCB when NoCallbacksAfterClientClose is set ([e24f452](https://github.com/loipv/nats-go/commit/e24f45249f54d0482b91d82b19e0e107041a9f96))
* keep the connection in an atomic so publishing never takes the App lock; capture handler base contexts per Start attempt; Health hides a failed attempt's closed connection; empty JS option value clears the header; PublishJSBatch rejects next called twice; document KVStore during drain ([3e7ce4f](https://github.com/loipv/nats-go/commit/3e7ce4f5b5cc915891cf3f6ffb0a1e4a624b0b2f))
* keep the connection set after a failed Start for lock-free publishers ([3503b83](https://github.com/loipv/nats-go/commit/3503b8314aefd2d9598a6b084c3dfd099cd2b3a6))
* KVStore returns ErrNotRunning after Shutdown, wraps bucket errors, rounds huge TTLs without overflow ([7ec8a77](https://github.com/loipv/nats-go/commit/7ec8a77941f8cde577bd0e27918e61e61024979f))
* make cloudevents Handle one-way and stop answering the PubAck inbox ([9699333](https://github.com/loipv/nats-go/commit/96993337c53d135916fc5480db19f7d5a1ad60f3))
* make Shutdown during Start wait for or cancel the Start ([4e2e0cd](https://github.com/loipv/nats-go/commit/4e2e0cd6ee4064c45f8e54f820064648e41dc69d))
* name endpoints in OnHandlerStopped and join hook errors only when present ([e1296c4](https://github.com/loipv/nats-go/commit/e1296c42f3a151530a771fbfefa64f260395c97d))
* no consume restart or warning after Shutdown; nak fetched batch on stop; omit unknown DLQ sequence; floor keepAlive interval; rerun-safe jetstream example ([af5725e](https://github.com/loipv/nats-go/commit/af5725ef2f6a38f9a88e77efc917397b589fc6f0))
* no default idempotency scope for endpoints without a queue group; document store keys and ErrInProgress outside JetStream ([ca29aba](https://github.com/loipv/nats-go/commit/ca29aba7ba91b76017abb3c1d6f43dbcda32610f))
* no false drift for BackOff AckWait, StreamSource.Domain and empty pointers; test Verify recovery ([a0e8620](https://github.com/loipv/nats-go/commit/a0e8620df41483abf9e5cf7d05b1925e43b426e9))
* no warning when a consumer lookup is cut short by stop; correct ManualAck batch and endpoint drain docs; retry-dlq reports only this run ([93ec912](https://github.com/loipv/nats-go/commit/93ec9128692da00f9a79c0f4e9a3fbd39be05837))
* purge KV claim markers with a TTL and round KV TTLs up to whole seconds ([4e8ce77](https://github.com/loipv/nats-go/commit/4e8ce77e2e08fbbb24ac2fddc2739b0d6fa3744c))
* read Conn and JetStream once in atomicbatch.Publish to avoid a nil panic ([a364908](https://github.com/loipv/nats-go/commit/a364908775f1a54284a094e97d51e345a5ca4d77))
* reject a core DLQ subject matched by the handler's own subject ([4b6f1b3](https://github.com/loipv/nats-go/commit/4b6f1b398e8d450fc32d54d3e719ef3746da5248))
* reject duplicate endpoint names; tighten idempotency registration edge cases ([f4b0515](https://github.com/loipv/nats-go/commit/f4b0515e206629d8bc99f80a3c33c5badd8836c7))
* release idempotency claim on panic and log to the app logger ([64e9fb9](https://github.com/loipv/nats-go/commit/64e9fb9a564178b2a975f51ff9b58d8fc07feb6a))
* send reply response outside the retried handler chain ([1171cbf](https://github.com/loipv/nats-go/commit/1171cbfaec7ea162281be16a7eaa354488d13bfd))
* service stop waits for requests even when micro Stop fails, otel error.type looks past fmt wrappers, clearer WithDLQ subject error; document DLQ processors ([00a26e3](https://github.com/loipv/nats-go/commit/00a26e3d1470d3e6a779cdbe0b3f3260b6fffee7))
* Shutdown aborts only the Start it waited on ([e07fdfc](https://github.com/loipv/nats-go/commit/e07fdfc386fdb5667b9b438d9c4d7099e2cd219e))
* stop sending a JetStream batch once its context is done ([62c5003](https://github.com/loipv/nats-go/commit/62c50030bc56c6f63e2c85bd0d29c35622b0b90b))
* time out unanswered async JetStream publishes after 30s ([a02627b](https://github.com/loipv/nats-go/commit/a02627b5dbbf32c0489ac5e6c94e8431b78fbec1))
* treat a missing stream as a deleted consumer in the supervisor ([b9425da](https://github.com/loipv/nats-go/commit/b9425da88ef2af33e53a0edac861cc787a215960))
* treat a typed-nil BatchError as success instead of crashing ([f179ed7](https://github.com/loipv/nats-go/commit/f179ed79a5470f907e04237a71bb545da5d03555))
* validate DLQ subjects and option values, keep joined batch errors ([83454d6](https://github.com/loipv/nats-go/commit/83454d63116ee0e2a34caadab79d966b9129c821))
* wait in concurrent Shutdown and do not block Health during Start ([031a1ff](https://github.com/loipv/nats-go/commit/031a1ff1ac11432ff12370e34408d5cfc108a072))


### Code Refactoring

* move atomicbatch.Publish to App.PublishJSAtomicBatch ([ce0dd71](https://github.com/loipv/nats-go/commit/ce0dd71f7cd8d8af6fe0008227154e87954d2ce2))
* rename publish batch, health and service types ([2347cb9](https://github.com/loipv/nats-go/commit/2347cb9fd9de8f92a98b6ee6fc482f69556d2260))
