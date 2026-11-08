# Review decisions

This records the disposition of all 100 proposals, 50 feedback notes and 50 scope cautions from the November 2026 review. The proposals are analytical suggestions, not customer demand. Repeated ideas share one implementation. **A decision is not a claim that every suggested feature was built.**

Concrete correctness and usability findings are fixed or documented in the linked changes. Broader integration proposals remain uncommitted candidates with explicit acceptance criteria. Cloud-account, GPU, multi-tenant authorization and new control-plane support are not implied by configuration examples.

The maintained roadmap is [TODO](https://github.com/hussein-awala/spark-on-k8s/blob/main/TODO.md). Use [development checks](development.md) and [support boundaries](user-guide/support.md) before promoting a candidate to supported functionality.

## 01 · Python practitioner

### 01.1 · A copyable Python project packaging recipe with executor import checks

**Not adopted in this change.** The existing ETL path and first-run route are maintained; additional Python packaging/parity starters need a concrete application and runtime acceptance fixture.

**Required evidence before implementation:** From a clean checkout and a prepared cluster, an engineer can replace the helper with a two-module project and complete the documented import check within 30 minutes; a deliberately missing executor dependency produces a failing check.

### 01.2 · A pandas-to-Spark migration walkthrough with output parity

**Not adopted in this change.** The existing ETL path and first-run route are maintained; additional Python packaging/parity starters need a concrete application and runtime acceptance fixture.

**Required evidence before implementation:** The documented command proves matching clean orders, quarantine reasons, and revenue on the fixture; changing one date or deduplication rule makes the parity comparison fail with a useful row difference.

**Feedback: Put the existing verified ETL path before placeholder submission.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/start-here.md).

**Scope caution accepted: Avoid turning submit into implicit environment deployment.** Automatically uploading the current virtual environment or installing arbitrary packages at startup would blur an understandable image boundary and create slow, hard-to-reproduce failures. A small, checked packaging recipe serves this persona with less ongoing maintenance.

## 02 · JVM platform architect

### 02.1 · Check an explicit JVM build/runtime compatibility descriptor

**Not adopted in this change.** JVM compatibility is qualified explicitly; another descriptor/starter should be driven by a real Java/Scala application and connector set.

**Required evidence before implementation:** A matching fixture passes; Scala 2.12 declared against a 2.13 runtime fails before any Kubernetes call; missing runtime metadata produces an explicit unknown result. Existing jobs remain usable without this optional check.

### 02.2 · Add one reproducible JVM dependency-packaging starter

**Not adopted in this change.** JVM compatibility is qualified explicitly; another descriptor/starter should be driven by a real Java/Scala application and connector set.

**Required evidence before implementation:** The image completes a distributed check while runtime artifact-repository access is unavailable. Artifact inspection confirms the helper dependency is present and Spark implementation classes are excluded. Rebuilding from identical declared inputs yields the same dependency inventory.

**Feedback: Keep JVM compatibility claims tied to the tested workload.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/spark_compatibility.md).

**Scope caution accepted: Avoid a separate Java/Scala submission SDK.** Image-local jars, class entrypoints, declarative jobs, and the CLI already provide a language-neutral deployment boundary. Another SDK adds release obligations without directly solving artifact reproducibility. Validate adoption friction before expanding that surface.

## 03 · Kubernetes SRE

### 03.1 · Include a verified cluster target in diagnostic receipts

**Not adopted in this change.** Production deployment inputs are documented now; cluster fleet context and maintenance presets need a selected operational environment.

**Required evidence before implementation:** Run the same namespace/service-account check against two test contexts and confirm saved reports remain distinguishable without shell history. Cover in-cluster and custom-client paths, missing context metadata, endpoint redaction, and JSON compatibility. Never export credentials or kubeconfig contents.

### 03.2 · Provide an explicit maintenance and disruption profile for the API chart

**Not adopted in this change.** Production deployment inputs are documented now; cluster fleet context and maintenance presets need a selected operational environment.

**Required evidence before implementation:** A chart rendering matrix covers defaults and both maintenance profiles. A documented drain/upgrade exercise measures readiness transitions, completed requests, interruption duration, and reconnect behavior. The history example states its measured outage instead of promising zero downtime.

**Feedback: Make production release identity and resource assumptions visible.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/chart-deployments.md).

**Scope caution accepted: Avoid building a central fleet scheduler.** Cluster federation, capacity balancing, and cross-cluster failover multiply credentials, state, and recovery obligations. Target-aware reports and operational deployment examples offer useful SRE improvements while keeping each existing deployment independently understandable.

## 04 · Airflow DAG author

### 04.1 · Fail safely when retry attachment cannot be verified

**Implemented.** Uncertain Airflow lookup now propagates instead of resubmitting; 404 alone establishes absence. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/airflow/operators.py).

### 04.2 · Template package coordinates and pod attribution consistently

**Implemented.** Packages and pod labels/annotations participate in Airflow templating, with both-version tests. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/tests/airflow/test_operators.py).

**Feedback: Explain lifecycle policies with a scheduled-task example.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/airflow.md).

**Scope caution accepted: Avoid a second retry and scheduling engine inside the operator.** Keep scheduling, backfills, task concurrency, and retry timing in Airflow. Improve attachment decisions and expose Spark lifecycle facts without adding hidden resubmission loops that make hundreds of scheduled tasks harder to reason about. Application-level idempotent output handling remains necessary.

## 05 · Airflow administrator

### 05.1 · Add an Airflow component preflight using the actual Kubernetes connection

**Not adopted in this change.** Permissions are clarified; component preflight and a multi-backend registry add deployment contracts that need a real Airflow installation.

**Required evidence before implementation:** In supported Airflow environments, a missing triggerer kubeconfig and a denied pod-read permission produce distinct actionable findings within 30 seconds; healthy worker and triggerer checks pass without creating persistent Kubernetes objects or exposing connection secrets.

### 05.2 · Allow an administrator-defined registry of task UI backends

**Not adopted in this change.** Permissions are clarified; component preflight and a multi-backend registry add deployment contracts that need a real Airflow installation.

**Required evidence before implementation:** Two tasks using two registered clusters display their respective applications even when namespace and pod names coincide; unknown backend identifiers fail closed, and writes stay disabled for the read-only backend.

**Feedback: Document submission privileges separately from task-tab permissions.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/airflow.md).

**Scope caution accepted: Avoid inventing a parallel Airflow identity and secrets system.** Continue using KubernetesHook, AsyncKubernetesHook, existing Airflow connections, and Airflow authorization. A bespoke connection type or credential store would increase upgrade and rotation work without addressing the observed component-readiness or single-backend gaps. UI permissions must not be marketed as isolation for arbitrary DAG code.

## 06 · Analytics SQL engineer

### 06.1 · Locate failed SQL statements by file and line

**Partially implemented.** SQL source basename, statement and line are preserved; failure logging avoids full query text. Structured execution events/timing and a completed/unstarted summary remain candidates. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/sql/runner.py).

### 06.2 · Typed run-date parameters for repeatable SQL jobs

**Not adopted in this change.** Source-aware failures are delivered first. Typed SQL parameters require Spark-version-specific semantics and a separate injection-safe API design.

**Required evidence before implementation:** Run one unchanged SQL file against two dates and verify only the intended partitions are addressed. An apostrophe-containing string remains one literal; unresolved placeholders and invalid dates fail locally before Kubernetes resources are created.

**Feedback: Make query previews a deliberate production choice.** Query text is no longer logged by the wrapper, and result-row exposure is documented. A quiet preview mode is not added because skipping a Spark action could change execution semantics; it requires a separate tested design. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/sql.md).

**Scope caution accepted: Avoid automatic resume from the failed statement.** Earlier statements may already have committed writes, while temporary views belong to the prior session. Skipping earlier statements or replaying them blindly can both change results. Keep recovery explicit until a workload supplies a verifiable idempotency contract. A statement summary helps an analyst decide; it does not establish safe retry boundaries.

## 07 · Notebook scientist

### 07.1 · Publish a multi-cell notebook session lifecycle recipe

**Not adopted in this change.** Generated-data onboarding is corrected; session lifecycle/transfer recipes need a chosen notebook and server configuration.

**Required evidence before implementation:** Execute the example cells in order and rerun setup twice: temporary views survive between analysis cells, each replaced session is closed once, final cleanup is safe to repeat, and another independent client remains usable. A new user can complete it using only a supplied profile and credentials.

### 07.2 · Extend the Connect check with an opt-in notebook data-transfer check

**Not adopted in this change.** Generated-data onboarding is corrected; session lifecycle/transfer recipes need a chosen notebook and server configuration.

**Required evidence before implementation:** The notebook mode passes against the existing integration server, reports a distinct sanitized conversion failure when conversion is deliberately broken, respects the same total timeout, and exposes no tokens or data paths. The default check retains its present behavior.

**Feedback: Make the first notebook example independent of a private warehouse.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/spark_connect.md).

**Scope caution accepted: Avoid turning notebook convenience into automatic infrastructure ownership.** Do not launch shared servers, request Kubernetes access, or upload arbitrary local notebook directories as a convenience default. Endpoint provisioning and storage authorization are platform responsibilities; keep improvements centered on a supplied profile, bounded synthetic checks, and explicit session ownership.

## 08 · ML feature engineer

### 08.1 · Add a reproducible batch-inference starter with an application-owned manifest

**Not adopted in this change.** ML adaptation limitations are documented. GPU/inference examples need hardware, a model artifact and an owner for recurring executor tests.

**Required evidence before implementation:** Two executions against the same fixture and model produce identical sorted predictions and identity fields; changing the expected model digest fails before outputs are published; changing the input identifier is visible in the manifest.

### 08.2 · Document and validate a CPU-driver, accelerator-executor inference recipe

**Not adopted in this change.** ML adaptation limitations are documented. GPU/inference examples need hardware, a model artifact and an owner for recurring executor tests.

**Required evidence before implementation:** Offline fixtures flag each documented contradiction and accept the reference profile. Before declaring runtime support, an opt-in accelerator-cluster smoke test verifies an executor uses the device while the driver requests none.

**Feedback: Mark the latest-event ETL example as unsuitable for historical feature reconstruction.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/starters.md).

**Scope caution accepted: Avoid turning submission tooling into a feature store or model lifecycle platform.** Registry promotion, feature serving, experiment tracking, and training orchestration create unrelated state and support obligations. Prefer small reproducible applications and explicit integration contracts; build further ML features only after actual adoption evidence.

## 09 · Streaming engineer

### 09.1 · Ship a checkpoint-preserving streaming restart starter

**Not adopted in this change.** Termination semantics are clarified. Checkpoint/freshness recipes need a real source, sink and recovery contract.

**Required evidence before implementation:** Process a deterministic sequence of input files, stop and redeploy with the same checkpoint, add more files, and assert the expected output set without rereading committed input in this specific example. Repeat after abrupt driver loss. An operator following the recipe can identify the checkpoint and confirm there is only one active driver before resubmission.

### 09.2 · Add an opt-in streaming progress and freshness integration recipe

**Not adopted in this change.** Termination semantics are clarified. Checkpoint/freshness recipes need a real source, sink and recovery contract.

**Required evidence before implementation:** A demonstration distinguishes normal processing, healthy idle input, delayed processing, terminated query with a still-live process, and missing telemetry. A stopped instrumentation feed becomes unknown/stale within the configured freshness threshold, without turning an idle stream into a failure.

**Feedback: Describe kill as a termination request, not a completed drain.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/automation.md).

**Scope caution accepted: Avoid automatic restarts inferred solely from pod phase.** A built-in streaming supervisor would add leader election, backoff, upgrade coordination, and checkpoint ownership obligations. Delegate continuous reconciliation to existing orchestration systems; first prove the recovery recipe and query-level observability.

## 10 · Iceberg lakehouse owner

### 10.1 · Provide a tested REST-catalog and object-storage starter profile

**Not adopted in this change.** The current single-writer Iceberg fixture remains the tested boundary; REST/object-storage and maintenance recipes require a qualified catalog/store.

**Required evidence before implementation:** An engineer supplies documented endpoints and identity references, then runs the same job through the new profile without editing Python. A separate session reads the expected three orders, a replay preserves their values, and the checked-in manifest contains no credential values.

### 10.2 · Add an explicit snapshot-retention maintenance recipe

**Not adopted in this change.** The current single-writer Iceberg fixture remains the tested boundary; REST/object-storage and maintenance recipes require a qualified catalog/store.

**Required evidence before implementation:** On a disposable fixture with several commits, maintenance preserves current rows and the configured protected snapshot while making an intentionally expired snapshot unavailable. The inspection step changes no snapshots; omitting required policy inputs prevents execution.

**Feedback: Keep snapshot observations distinct from commit ownership.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/starters.md).

**Scope caution accepted: Do not turn the submission library into an Iceberg control plane.** Catalog hosting, cross-engine writer locking, table governance, and automatic repair would add substantial persistent-state and correctness obligations. Prefer optional recipes and narrowly tested integrations with the catalog and orchestration systems users already operate. A successful Spark run should not be presented as an exactly-once or table-isolation guarantee.

## 11 · Delta lakehouse owner

### 11.1 · Add a narrowly qualified bring-your-own Delta batch recipe

**Not adopted in this change.** Map-merge adaptation is documented. Delta support needs a pinned connector/runtime test rather than a compatibility claim from configuration alone.

**Required evidence before implementation:** From a clean namespace, the documented commands build and submit the example; an independent reader verifies expected keys and values after two replays and reads the earlier version. Report local execution and Kubernetes execution separately.

### 11.2 · Provide a Delta stream restart qualification recipe

**Not adopted in this change.** Map-merge adaptation is documented. Delta support needs a pinned connector/runtime test rather than a compatibility claim from configuration alone.

**Required evidence before implementation:** Two submissions using the same checkpoint produce exactly the expected fixture keys and counts, preserve the first batch, and include the second. Capture query progress and final table verification so driver completion alone cannot pass the check.

**Feedback: Explain profile merge behavior when adapting the Iceberg starter.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/starters.md).

**Scope caution accepted: Avoid turning submission tooling into a Delta table-management service.** Do not add generic optimize, vacuum, protocol-upgrade, or table-repair buttons. Those operations require table-level policies and knowledge of other writers; examples and existing SDK submission preserve the user's established Delta tooling with substantially less support burden. Neither proposal requires owning table lifecycle.

## 12 · CDC ingestion engineer

### 12.1 · Add a Hudi CDC contract starter with ordering and tombstone fixtures

**Not adopted in this change.** CDC ordering/deletion contracts are clarified. Hudi fixtures need a selected source ordering model and recurring table-format tests.

**Required evidence before implementation:** Execute the fixture twice and in deliberately reordered batches; assert the same final visible rows, source versions, and retained deletion markers. Replay an old insert after deletion and verify it remains invisible. Reject the incompatible schema without changing the table. Run this on one supported dependency combination.

### 12.2 · Publish a reproducible CDC recovery drill that separates submission from data commit

**Not adopted in this change.** CDC ordering/deletion contracts are clarified. Hudi fixtures need a selected source ordering model and recurring table-format tests.

**Required evidence before implementation:** For each injected failure, follow the documented commands to recover the same manifest. Verify the checkpoint never skips an uncommitted interval, replay produces the expected table contents, and a changed manifest digest is refused. Demonstrate that missing driver history does not erase the durable recovery evidence.

**Feedback: Qualify replay safety by the source ordering contract.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/starters.md).

**Scope caution accepted: Avoid building a Hudi table-services or CDC connector manager.** Owning compaction, cleaning, database log capture, and schema-registry operations would substantially expand the project beyond submitting and observing Spark jobs. Start with one executable integration contract and recovery recipe; leave table lifecycle and source infrastructure with their existing operators.

## 13 · AWS EKS engineer

### 13.1 · Add an EKS workload-identity and S3 acceptance recipe

**Not adopted in this change.** Identity ownership is documented; EKS IAM/storage and executor-loss acceptance require an AWS test account and cost/cleanup boundaries.

**Required evidence before implementation:** In a disposable EKS namespace, the documented recipe completes a multi-partition S3 round trip with expected row count and no static keys in the job. Removing access to the test prefix makes the check fail visibly. A reviewer can identify the application service account and image dependencies from the saved profile.

### 13.2 · Provide a mixed-capacity executor-loss rehearsal

**Not adopted in this change.** Identity ownership is documented; EKS IAM/storage and executor-loss acceptance require an AWS test account and cost/cleanup boundaries.

**Required evidence before implementation:** The sample produces the same checksum before and after one executor deletion; the driver remains on its selected pool, replacement execution is visible, and completion stays within an operator-defined recovery budget. A separate deliberate driver-loss run documents the actual terminal behavior rather than implying transparent recovery.

**Feedback: Make deployment identity and application identity easier to distinguish.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/chart-deployments.md).

**Scope caution accepted: Avoid becoming an AWS infrastructure and credential manager.** Keep IAM provisioning, registry authentication infrastructure, node pools, and interruption handling with existing platform tooling. Versioned examples and acceptance jobs provide AWS adoption value without creating a second cloud control plane or storing credentials in the application service.

## 14 · GCP GKE engineer

### 14.1 · Publish a keyless GKE-to-GCS acceptance recipe

**Not adopted in this change.** Scaling and identity boundaries are documented; GKE storage/elasticity acceptance requires a cloud project and an explicit test owner.

**Required evidence before implementation:** In a designated GKE acceptance environment, the documented CLI job reads and writes a known dataset from multiple executor partitions with no JSON key mounted and no runtime dependency downloads; checksum and row count match. Removing the bucket grant causes the acceptance job to fail instead of reporting success.

### 14.2 · Add a GKE Standard elasticity rehearsal profile

**Not adopted in this change.** Scaling and identity boundaries are documented; GKE storage/elasticity acceptance requires a cloud project and an explicit test owner.

**Required evidence before implementation:** With a deliberately capacity-constrained test pool, the second burst produces pending executors, node growth makes them runnable, the job stays within its executor maximum, and idle executors are released. Publish observed timings and node scale-down outcome, including blockers, rather than promising a fixed provisioning latency.

**Feedback: Explain the three independent scaling controls together.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/chart-deployments.md).

**Scope caution accepted: Avoid a GCP control-plane subsystem.** Do not grow cluster provisioning, IAM reconciliation, bucket creation, or network configuration into this client. Versioned examples and acceptance jobs exploit the existing submission surface while leaving infrastructure lifecycle with the user's established Google Cloud tooling. They also avoid implying that this project can certify every private-network or organization-policy configuration.

## 15 · Azure AKS engineer

### 15.1 · Publish one tested AKS workload-identity-to-ADLS reference job

**Not adopted in this change.** Identity boundaries are documented; AKS/ADLS and Key Vault rotation require an Azure subscription and credential-rotation fixture.

**Required evidence before implementation:** On a prepared AKS namespace, the documented SDK/CLI job writes and rereads a known partitioned dataset using at least two executor pods, without account keys or client secrets. A deliberately incorrect application service account fails the negative test. Capture reproducible setup and cleanup instructions.

### 15.2 · Add a Key Vault CSI mounted-secret recipe with an explicit rotation contract

**Not adopted in this change.** Identity boundaries are documented; AKS/ADLS and Key Vault rotation require an Azure subscription and credential-rotation fixture.

**Required evidence before implementation:** A prepared AKS test shows both pod roles reading the mounted test value without printing it. After rotation, the documented reread/restart path observes the replacement; missing-provider and denied-access cases fail with actionable evidence. Submission plans and logs contain no secret values.

**Feedback: Make the identity boundary visible before Azure users edit Helm values.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/chart-deployments.md).

**Scope caution accepted: Avoid turning the project into an Azure resource and credential manager.** Creating cloud identities, role assignments, Key Vault synchronization, and networking would duplicate platform tooling and greatly expand the trust boundary. Start with maintained examples and explicit prerequisites; keep production Azure lifecycle ownership with the platform team. Build more only after concrete integration failures justify it.

## 16 · OpenShift administrator

### 16.1 · Publish and verify an arbitrary-UID API deployment recipe

**Not adopted in this change.** Namespace-scoped deployment guidance is delivered. Arbitrary-UID and disconnected promotion need actual OpenShift admission and registry environments.

**Required evidence before implementation:** The documented API profile starts with two different non-root UIDs, serves health and readiness, and records a run on a suitably provisioned claim without permission errors; the verification requires no privileged pod or elevated SCC.

### 16.2 · Provide a disconnected promotion recipe for one complete starter

**Not adopted in this change.** Namespace-scoped deployment guidance is delivered. Arbitrary-UID and disconnected promotion need actual OpenShift admission and registry environments.

**Required evidence before implementation:** A second operator follows the recipe in a namespace whose egress permits only the private registry and required cluster services, starts the API, and completes the lakehouse starter with the recorded image digests and no public artifact requests.

**Feedback: Make namespace-scoped installation the first restricted-cluster example.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/chart-deployments.md).

**Scope caution accepted: Do not solve admission friction by granting anyuid or privileged access.** Such a shortcut defeats the target operator’s deployment constraints and hides image or volume assumptions. Prefer narrowly documented runtime permissions and verified writable paths; keep administrator policy changes outside this library.

## 17 · Newcomer learner

### 17.1 · A resource-budgeted laptop first-run path

**Not adopted in this change.** First-run routes use existing tested computation. A new laptop installer or automatic failure lab would create platform ownership beyond the client.

**Required evidence before implementation:** On the documented clean local cluster configuration, a reviewer completes the guide with one driver and one executor, observes the expected result, and reruns it after cleanup without editing source files. A small newcomer trial targets completion within 30 minutes after prerequisites and image downloads are ready.

### 17.2 · A guided image-failure and recovery lesson

**Not adopted in this change.** First-run routes use existing tested computation. A new laptop installer or automatic failure lab would create platform ownership beyond the client.

**Required evidence before implementation:** A learner can identify the missing-image condition from evidence, explain why doctor success does not certify image availability, recover without rebuilding the package, and independently identify which pod runs application code.

**Feedback: Make scoped permissions the first teaching example.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/start-here.md).

**Scope caution accepted: Avoid a bundled laptop platform installer.** Automatically installing Kubernetes, Airflow, the API, storage and a registry would create a large support surface and consume the learner's limited resources. A core-client lab using ordinary existing tools makes each component understandable and keeps the optional integrations optional.

## 18 · FinOps analyst

### 18.1 · Preserve a small, exportable cost-attribution record

**Not adopted in this change.** The resource budget slice is delivered. Cost attribution policy still requires agreement on retained labels, privacy and an external cost system.

**Required evidence before implementation:** Record two teams sharing a namespace, delete their driver pods, and export every retained run with the correct attribution and unique deployment/run identity; a missing-label fixture must remain explicitly unattributed. Disallowed labels must never enter storage or output.

### 18.2 · Add an offline resource-envelope budget check

**Partially implemented.** CPU/memory request envelope and explicit budget checks reject unknown or excessive maxima. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/resource_plan.py).

**Feedback: Keep operational observation times out of spend and utilization claims.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/run-history.md).

**Scope caution accepted: Avoid building a cloud-pricing and chargeback platform.** Cloud discounts, shared-node allocation, storage, network charges and billing corrections exceed the recorder's evidence and would create substantial maintenance. Prioritize stable attribution joins and transparent resource envelopes; let an existing organizational cost system compute money and allocations. Do not automatically terminate jobs based on these estimates.

## 19 · Batch scheduling specialist

### 19.1 · Expose an aggregate workload capacity envelope in offline plans

**Implemented.** One shared conservative capacity summary; not a scheduler feasibility guarantee. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/resource_plan.py).

### 19.2 · Add a paired driver/executor scheduling audit for existing profiles

**Not adopted in this change.** Capacity requests are summarized. Scheduling audits cannot establish quota, placement or autoscaler feasibility offline.

**Required evidence before implementation:** An intentionally mismatched driver/executor scheduler fails an explicit parity assertion; a documented higher-priority driver passes an allowed difference; an inaccessible executor template remains unknown. Existing jobs without assertions continue to submit unchanged.

**Feedback: Describe remote admission limits as per-job bounds, not shared capacity control.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/remote-submission.md).

**Scope caution accepted: Avoid building an internal fair-share or gang scheduler.** An embedded queue introduces durable state, cancellation, resource reservation, fairness, and multi-instance coordination obligations. Prefer transparent capacity summaries and verified integration contracts with an existing scheduler; neither the current shared operator token nor metadata history should become an implicit scheduler database.

## 20 · Multi-tenant platform architect

### 20.1 · Publish a repeatable deployment recipe for separate trust boundaries

**Not adopted in this change.** Separate trust-boundary deployment guidance is explicit. Per-application identity binding is not sufficient to claim route-wide tenant isolation.

**Required evidence before implementation:** In the two-department fixture, each credential can submit and inspect its own job; every tested cross-department read or management operation fails without returning workload data. Rendered manifests contain no wildcard ClusterRoleBinding for either API. A documented onboarding exercise requires only substituting department names and existing gateway/secret references.

### 20.2 · Bind approved applications to their permitted workload identities

**Not adopted in this change.** Separate trust-boundary deployment guidance is explicit. Per-application identity binding is not sufficient to claim route-wide tenant isolation.

**Required evidence before implementation:** A matrix with two applications and two workload identities admits exactly the two intended pairings and rejects both swapped pairings for plan and submit. Rejection occurs before any Kubernetes write. Legacy-policy conversion preserves its original four combinations and clearly identifies that behavior.

**Feedback: Keep the operator boundary visible beside namespace allowlists.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/remote-submission.md).

**Scope caution accepted: Avoid labeling a token-per-team extension as tenant isolation.** Multiple tokens alone would leave the shared backend credentials and management surfaces unchanged. Prefer the supported deployment boundary until principal-aware authorization, workload identities and cross-boundary access tests exist throughout the API.

## 21 · Identity engineer

### 21.1 · Preserve verified gateway actor identity in submission audit events

**Not adopted in this change.** Authorization matrix and gateway boundary are documented. Actor audit and renewable credentials need verified gateway identity/expiry contracts.

**Required evidence before implementation:** A gateway fixture produces attributable allow and deny events that correlate to returned request IDs and accepted run receipts. Forged headers, invalid signatures, wrong audiences and expired assertions cannot obtain attributed execution when the adapter is enabled. Existing shared-token mode remains explicitly unattributed.

### 21.2 · Add a renewable credential provider to the remote SDK

**Not adopted in this change.** Authorization matrix and gateway boundary are documented. Actor audit and renewable credentials need verified gateway identity/expiry contracts.

**Required evidence before implementation:** One client instance uses token A, then token B after a provider/file update. Expired or malformed credentials cause zero network requests. A submission receiving 401 or losing its response is sent exactly once, and neither logs nor exception messages expose either token.

**Feedback: Publish one explicit authorization matrix across deployment modes.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/operations.md).

**Scope caution accepted: Avoid implementing an identity provider inside spark-on-k8s.** User directories, password recovery, session management and provider-specific group synchronization would create a permanent security maintenance burden. Prefer a narrow gateway contract and established identity infrastructure; do not advertise tenant isolation merely because a verified subject becomes available.

## 22 · Application security engineer

### 22.1 · Strip proxy credentials independently of remote submission mode

**Implemented.** Proxy credential allowlist applies independently of remote-submission mode. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/api/webserver/__init__.py).

### 22.2 · Define and enforce browser origins for log WebSockets

**Implemented.** Browser origins checked before log collection; explicit external-origin configuration and non-browser semantics documented. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/security-boundaries.md).

**Feedback: Publish the security boundary alongside the reporting policy.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/SECURITY.md).

**Scope caution accepted: Do not put the operator bearer in browser storage or socket URLs.** Solving browser authentication by embedding the shared token in bootstrap data, localStorage, or query strings would expose a powerful reusable credential to more browser code and logs. Preserve the gateway-mediated design and improve its explicit origin and header contracts instead. This does not require expanding the project into an identity platform.

## 23 · Supply-chain engineer

### 23.1 · Build release containers from a frozen API dependency set

**Implemented.** Container consumes the release wheel and hash-checked frozen API dependencies using a pinned base. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/docker/Dockerfile).

### 23.2 · Publish an artifact inventory that includes the bundled frontend

**Implemented bounded scope.** Release manifest records package/dependency inputs and hashes; OCI image build requests provenance/SBOM. This is not a vulnerability dashboard. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/scripts/release_artifacts.py).

**Feedback: Make the release-to-tested-commit relationship explicit.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/scripts/release_eligibility.py).

**Scope caution accepted: Avoid an all-purpose security dashboard or blanket vulnerability gate.** Start with verifiable release inputs and usable artifact evidence. A custom dashboard adds maintenance, while blocking every advisory across all optional and build dependencies can make releases unpredictable without establishing actual exposure. Define ownership, affected install profiles, and exception handling before adopting a broader gate.

## 24 · Compliance auditor

### 24.1 · Attach policy provenance to submission decision receipts

**Not adopted in this change.** Reproducible inputs and metadata limitations are documented. Compliance receipts/exports require a consumer schema, retention policy and trusted actor source.

**Required evidence before implementation:** An accepted request, denied request, and simulated partial Kubernetes failure each produce correlatable response/event records. Changing the loaded policy changes its digest; formatting-only changes do not. Sentinel secrets never appear in events. Shared-token requests cannot acquire a claimed human identity from client headers.

### 24.2 · Export a bounded run-evidence snapshot with an explicit coverage manifest

**Not adopted in this change.** Reproducible inputs and metadata limitations are documented. Compliance receipts/exports require a consumer schema, retention policy and trusted actor source.

**Required evidence before implementation:** Concurrent collector updates and pruning during export cannot yield a mixed snapshot. The manifest count matches the artifact; the checksum verifies; namespace filtering remains enforced. Oversized exports return an explicit error. Reviewers can identify collection freshness and retention limitations from the artifact alone.

**Feedback: Provide a reproducible operator-deployment example.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/chart-deployments.md).

**Scope caution accepted: Do not market operational history as a compliance audit trail.** Polling misses runs, retention removes records, and shared operator authorization does not identify individual users. Preserve the documented limitations and integrate with external identity and evidence retention systems when needed; avoid unsupported legal compliance claims or a large embedded governance suite.

## 25 · Privacy engineer

### 25.1 · Add an explicit, minimized log-sharing export

**Implemented.** Local bounded excerpt preparation with explicit copy/download scope and stable reading view; no extra server retention. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/frontend/src/LogExcerpt.tsx).

### 25.2 · Offer a minimal history metadata profile before persistence

**Not adopted in this change.** Log minimization is delivered. A separate minimal-history profile needs a documented consumer impact and migration story.

**Required evidence before implementation:** Seed names, image paths, and application IDs with unique sensitive sentinels. Under minimal mode, no sentinel reaches inserted rows or API responses, including after an update and migration. Status filtering, missing-pod tracking, pagination, and aggregate metrics remain usable.

**Feedback: Qualify privacy guarantees by storage surface and field origin.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/run-history.md).

**Scope caution accepted: Avoid collecting more sensitive data to power privacy features.** Do not add central raw-log archives, SQL capture, environment snapshots, or third-party scanning uploads solely to support redaction or troubleshooting. Preserve the current metadata boundary and operate on explicitly selected local excerpts; otherwise a privacy improvement creates a larger retention and access problem.

## 26 · Observability engineer

### 26.1 · Ship a small recorder-health dashboard and alert recipe

**Not adopted in this change.** Metric semantics are clarified at HELP and in documentation. Dashboard/failure taxonomy expansion needs operator use cases and a stable bounded vocabulary.

**Required evidence before implementation:** Against fixture time series, healthy collection stays green; age -1 alerts only after startup grace; stale age and failed scrapes produce different alerts; history-disabled deployments do not produce recorder-failure alerts. A documented example connects without embedding a token in URLs or dashboard files.

### 26.2 · Expose bounded collector failure reasons and collection timing

**Not adopted in this change.** Metric semantics are clarified at HELP and in documentation. Dashboard/failure taxonomy expansion needs operator use cases and a stable bounded vocabulary.

**Required evidence before implementation:** Inject representative timeout, denied-access, repeated-cursor, and storage errors and verify stable categories and duration measurements. Assert that arbitrary exception strings, application IDs, and pod names never create labels, and that worker takeover does not double count committed attempts.

**Feedback: Make retained-count semantics explicit at the metric itself.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/api/run_history.py).

**Scope caution accepted: Avoid per-application Prometheus labels or a competing Spark telemetry store.** Application IDs, pod UIDs, SQL, and exception strings introduce unbounded cardinality and privacy exposure. Keep aggregate operational metrics bounded; use existing run identifiers for explicit drill-down and external Spark telemetry for execution detail.

## 27 · On-call incident responder

### 27.1 · Show when diagnostic evidence happened and refresh successful snapshots

**Not adopted in this change.** Timestamp and cause-specific guidance are implemented. New command-generation or evidence collection UI should preserve the existing read-only boundary.

**Required evidence before implementation:** Fixtures containing an old scheduling warning, a current waiting state, a previous OOM and missing event times remain distinguishable in JSON and the panel. Refreshing a successful snapshot visibly advances collection time. In a small internal triage exercise, responders can identify evidence age without a separate Kubernetes query.

### 27.2 · Turn diagnostic next steps into scoped, read-only investigation recipes

**Not adopted in this change.** Timestamp and cause-specific guidance are implemented. New command-generation or evidence collection UI should preserve the existing read-only boundary.

**Required evidence before implementation:** Generated recipes consistently include the selected context, namespace and container, contain only allowed inspection operations, and quote arguments safely. In a staged crash-loop exercise, a responder retrieves the intended container's previous attempt within two minutes without editing resource identifiers.

**Feedback: Make incomplete-report guidance specific to the missing evidence.** Incomplete guidance distinguishes bounds, ownership, authorization, missing resources and transient failures. Diagnostics record collection time. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/diagnostics.py).

**Scope caution accepted: Avoid automatic restart or resubmit buttons derived from diagnostic classifications.** An observed crash, OOM or scheduling warning does not establish that replay is safe or that configuration changes are correct. Keep diagnosis as evidence-led investigation; application owners must decide recovery using checkpoint, idempotency and downstream-write knowledge the snapshot does not contain. This protects the useful narrow scope of the existing diagnostics and metadata history.

## 28 · Disaster-recovery engineer

### 28.1 · Add a verified, portable history backup command

**Not adopted in this change.** A consistent SQLite backup recipe and recovery coverage are documented. Automated restore/PVC drills require an explicitly selected deployment and backup destination.

**Required evidence before implementation:** While snapshots are being committed, create a backup and restore it into an isolated directory. Integrity validation passes and its rows form a transactionally consistent snapshot. Interrupting backup leaves no published successful artifact; using the live database as destination is rejected. A documented restore produces queryable records after restart.

### 28.2 · Provide a rehearsable PVC replacement and recovery playbook

**Not adopted in this change.** A consistent SQLite backup recipe and recovery coverage are documented. Automated restore/PVC drills require an explicitly selected deployment and backup destination.

**Required evidence before implementation:** A maintainer follows the recipe on a disposable cluster, replaces the original claim, retrieves a retained completed run, observes fresh collection, and rolls back successfully. The resulting report distinguishes recoverable backup age from runs never observed or already pruned and records measured downtime.

**Feedback: Describe recovery coverage separately from persistent storage.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/operations.md).

**Scope caution accepted: Do not turn recorder durability into an HA database project.** Retain the single-pod SQLite boundary. Shared-network-file replication or a new distributed database would add substantial migration and support obligations to an optional metadata recorder. Establish verified backups and rehearsed recovery first; require demonstrated demand before considering a different persistence architecture.

## 29 · Network engineer

### 29.1 · Make internal service DNS use a configurable cluster domain

**Not adopted in this change.** Gateway acceptance is documented. Custom DNS/CONNECT proxy support changes routing and credentials and needs a concrete deployment fixture.

**Required evidence before implementation:** With clusterDomain set to corp.internal, render the chart and assert its History Server host ends in svc.corp.internal; capture a mocked Spark UI request and assert the same suffix. Default-domain outputs remain unchanged, and malformed domains fail configuration validation.

### 29.2 · Offer explicit CONNECT-proxy transport for remote submissions

**Not adopted in this change.** Gateway acceptance is documented. Custom DNS/CONNECT proxy support changes routing and credentials and needs a concrete deployment fixture.

**Required evidence before implementation:** A local test proxy records CONNECT to the configured API authority; a mock HTTPS API receives the bearer token only within the TLS tunnel. Invalid destination certificates fail, stalled tunnels time out, redirects remain refused, and setting HTTP_PROXY alone leaves direct-mode behavior unchanged.

**Feedback: Turn gateway requirements into a verifiable deployment contract.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/security-boundaries.md).

**Scope caution accepted: Do not become a service-mesh controller.** Keep mesh certificate issuance, sidecar lifecycle, ingress-controller installation, and vendor-specific authorization policy with the platform owner. Small configuration hooks and verifiable examples preserve compatibility without promising a maintained matrix of mesh products. Do not solve private-network friction by disabling TLS verification or broadly allowing all egress.

## 30 · Storage performance engineer

### 30.1 · Expose a safe storage inventory in offline application plans

**Not adopted in this change.** Storage ownership and demo limits are documented. Inventory/classification expansion needs validated CSI/PVC examples; no automatic PVC cleanup.

**Required evidence before implementation:** Fixtures covering an embedded executor template, SDK volume settings, driver template, explicit Spark overrides, and an external template produce the expected inventory; external information remains explicitly unknown. Secret sentinel strings never appear, and offline planning makes zero Kubernetes calls.

### 30.2 · Add storage-specific diagnoses from existing pod and event evidence

**Not adopted in this change.** Storage ownership and demo limits are documented. Inventory/classification expansion needs validated CSI/PVC examples; no automatic PVC cleanup.

**Required evidence before implementation:** Representative event/status fixtures yield the correct storage code and actionable remediation; ambiguous evictions remain unclassified as storage. Existing diagnosis fixtures retain their results, collection request counts stay unchanged, and incomplete evidence remains visible.

**Feedback: Keep the shared-PVC starter visibly scoped as a functional demo.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/examples/starters/storage.yaml).

**Scope caution accepted: Avoid automatic PVC garbage collection based only on application labels.** Shared dataset volumes and reusable scratch volumes can outlive one application. The current generic volume inputs do not establish ownership or retention intent. A cleanup controller would add substantial storage-specific lifecycle responsibility and risk deleting valuable data; require explicit ownership and retention contracts before considering it.

## 31 · Spark performance engineer

### 31.1 · Explain the effective resource envelope in offline plans

**Partially implemented.** Same shared resource summary as reviews18/19; value-provenance/conflict annotations remain unimplemented. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/resource_plan.py).

### 31.2 · Add an on-demand Spark performance evidence report

**Not adopted in this change.** Capacity requests are delivered. On-demand Spark performance collection needs event-log authorization and data-volume limits distinct from pod diagnostics.

**Required evidence before implementation:** Against small known workloads and REST fixtures, the report identifies the expected slow stage and matches the source quantities. Attempt selection, missing metrics, timeouts, and truncation produce explicit partial reports rather than misleading zero values.

**Feedback: Keep infrastructure health and workload efficiency visibly separate.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/operations.md).

**Scope caution accepted: Avoid universal tuning presets or automatic memory increases.** Heap, overhead, partitioning, skew, and concurrency interact with workload and data shape. A preset labeled optimized would hide that uncertainty. Prefer explicit evidence, reproducible profile comparisons, and changes users can inspect; do not infer optimal settings from pod success or one OOM event.

## 32 · Spark Connect operator

### 32.1 · Bounded fleet canary checks with latency reporting

**Not adopted in this change.** Cleanup semantics are fixed. Multi-endpoint canaries and draining recipes need a selected shared-server service contract.

**Required evidence before implementation:** For five fixture endpoints with one stalled endpoint, emit five attributable results within the configured overall deadline, never exceed the concurrency cap, preserve healthy results, and expose no token values. A scheduled staging trial must demonstrate the documented maximum probe rate.

### 32.2 · Staging qualification recipe for session expiry and endpoint maintenance

**Not adopted in this change.** Cleanup semantics are fixed. Multi-endpoint canaries and draining recipes need a selected shared-server service contract.

**Required evidence before implementation:** Against a disposable staging service, record client termination and new-session recovery times, verify the configured cleanup objective using the operator’s server evidence, and mark unavailable server evidence as unverified rather than passing. Confirm the client harness itself exits within its deadline.

**Feedback: Describe session cleanup as an attempted release, with explicit failure semantics.** Cleanup no longer masks an application failure; cleanup-only failure remains visible. Tests cover both paths, and the first notebook example uses generated data. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/connect.py).

**Scope caution accepted: Avoid turning the companion into a full Connect service platform.** Server pools, session routing, admission queues, tenant identity, and autoscaling introduce a new operational product. Keep these with the existing service owner and improve observable integration boundaries first; client profiles and shared tokens cannot establish capacity control or tenant isolation.

## 33 · CLI automation engineer

### 33.1 · Persist the submission receipt before entering a long wait

**Implemented.** Atomic no-overwrite receipt file is published before waiting and survives observation failure. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/cli/receipts.py).

### 33.2 · Extend machine-readable contracts to explicit cleanup commands

**Not adopted in this change.** Early receipts and guarded cancellation are delivered. General structured cleanup results remain separate from accepted cancellation and garbage-collection observation.

**Required evidence before implementation:** A cleanup shell fixture must parse one JSON document for success, missing application, and API failure; verify consistent exit codes, opt-in already-absent success, and preserved --force behavior. Neither command should require parsing human text.

**Feedback: Publish an explicit command-by-command automation contract matrix.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/automation.md).

**Scope caution accepted: Avoid maintaining a separate workflow engine or CI-provider SDK.** Stable receipts, explicit action results, and small shell examples can serve several CI systems through existing commands. Provider-specific orchestration, automatic retries, and implicit cancellation policies would add maintenance and ambiguous ownership without evidence here that the shared CLI is insufficient.

## 34 · Python SDK maintainer

### 34.1 · Publish a typed public SDK boundary with compatibility fixtures

**Not adopted in this change.** Supported imports/sharing semantics are documented. A new facade or complete exception taxonomy needs a backward-compatible migration proposal.

**Required evidence before implementation:** A small downstream fixture type-checks successful local, remote, and SQL usage; misspelled keywords and reading a plan-only field from a submission result fail statically. The same fixture imports from an installed wheel without optional Airflow or Connect packages.

### 34.2 · Make SDK failure categories programmatic and backward compatible

**Not adopted in this change.** Supported imports/sharing semantics are documented. A new facade or complete exception taxonomy needs a backward-compatible migration proposal.

**Required evidence before implementation:** Consumer fixtures branch by category without parsing messages; existing exception catches still work; a local wait timeout includes the submitted identity; a remote transport failure reports uncertainty without inventing a run identity.

**Feedback: Clarify client sharing before consumers assume reentrant submission.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/automation.md).

**Scope caution accepted: Avoid mirroring the entire SDK with an async implementation.** An async twin would double public signatures, error guarantees, and maintenance before concrete integration demand is established. First publish blocking and cancellation semantics, stabilize value/error contracts, and document that cancelling a waiting caller does not cancel its Spark application.

## 35 · GitOps practitioner

### 35.1 · Validate the whole environment matrix and explain promotion differences

**Implemented bounded scope.** Base plus every named profile can be validated offline in one command. A semantic promotion diff remains a separate candidate. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/cli/job.py).

### 35.2 · Bind submission to an approved source receipt

**Not adopted in this change.** All-profile validation is delivered. Provenance receipts and semantic promotion diffs need canonicalization and secret-handling design before claiming approval integrity.

**Required evidence before implementation:** An unchanged checkout and template pass receipt verification. Changing one template byte, the selected profile, or the package version rejects submission before resource creation. The ordinary unguarded workflow remains available.

**Feedback: Make the production example select its cluster explicitly.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/jobs.md).

**Scope caution accepted: Avoid a new GitOps controller for transient Spark resources.** Do not turn illustrative plans into continuously reconciled manifests. That would introduce workload ownership, duplicate-run, deletion, and reconciliation semantics beyond this SDK. Keep configuration review and promotion integration small, and leave trigger ownership with existing orchestration tools. The documentation already correctly explains why preview resources are not directly applicable.

## 36 · Infrastructure integrator

### 36.1 · Make Helm values a validated configuration contract

**Implemented bounded scope.** Chart schema validates maintained chart controls and render tests cover valid/invalid inputs; arbitrary Kubernetes maps remain escape hatches. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/chart/values.schema.json).

### 36.2 · Separate service-account and RBAC ownership for existing clusters

**Implemented bounded scope.** External service-account/RBAC ownership is represented explicitly and documented; no new infrastructure provider. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/chart-deployments.md).

**Feedback: Avoid mutable image defaults in reproducible chart upgrades.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/chart-deployments.md).

**Scope caution accepted: Do not build a dedicated Terraform provider yet.** A maintained Helm contract and a short integration example cover installation ownership without adding provider releases, state migrations, credential handling, and another public API. Revisit only if concrete lifecycle requirements cannot be expressed through existing Kubernetes and Helm resources.

## 37 · Spark Operator migrant

### 37.1 · Add a conservative SparkApplication migration assessor and importer

**Not adopted in this change.** Client versus controller ownership is clarified. A Spark Operator importer requires real manifests and must report every unsupported semantic; no silent conversion.

**Required evidence before implementation:** For a small published fixture corpus, every source leaf has an explicit accounting status; supported fixtures produce validated jobs and expected plans; a fixture containing an unsupported lifecycle setting cannot receive a migration-ready result.

### 37.2 · Provide a bounded parity report for side-by-side migration trials

**Not adopted in this change.** Client versus controller ownership is clarified. A Spark Operator importer requires real manifests and must report every unsupported semantic; no silent conversion.

**Required evidence before implementation:** Reference fixtures detect an omitted volume, a service-account change, and a resource change while ignoring generated identifiers. Redacted values produce unverifiable results, never false equality. A maintainer can complete the paired-run checklist without changing an existing controller-managed workload.

**Feedback: State the lifecycle ownership change before presenting portable YAML.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/automation.md).

**Scope caution accepted: Avoid building a second SparkApplication reconciler.** Controller compatibility would expand lifecycle, upgrade, and CRD obligations substantially. Start with explicit migration assessment and comparison; preserve the ability to retain an existing operator for workloads whose lifecycle requirements do not fit direct submission.

## 38 · Orchestration integrator

### 38.1 · Expose a framework-neutral run outcome helper

**Not adopted in this change.** Shared receipt and lifecycle contracts are documented. A Dagster/Prefect adapter needs an adopter and interruption-recovery tests instead of copied Airflow operators.

**Required evidence before implementation:** A shared fixture suite exercises successful, failed, missing, unknown, and timed-out runs. The helper and CLI agree on workload classification; a sample wrapper cannot report a failed or unknown workload as success and does not delete a job after observation timeout.

### 38.2 · Publish restart-aware Dagster and Prefect integration recipes

**Not adopted in this change.** Shared receipt and lifecycle contracts are documented. A Dagster/Prefect adapter needs an adopter and interruption-recovery tests instead of copied Airflow operators.

**Required evidence before implementation:** Restart each example after handle persistence and verify that it observes the original pod without creating another. A terminal successful run remains successful on retry; API unavailability does not trigger resubmission. Document and exercise the uncertain-submission branch.

**Feedback: Make lifecycle policy differences visible to adapter authors.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/airflow.md).

**Scope caution accepted: Avoid cloning the Airflow operator into two new providers.** Large dedicated adapters would multiply submission, cleanup, UI-link, and outcome-policy maintenance before demand is established. Start with shared SDK primitives and narrow runnable recipes; promote them into optional packages only when actual framework-specific requirements justify ownership.

## 39 · Cloud-native workflow author

### 39.1 · Ship thin Argo and Kubeflow submit-and-observe examples

**Not adopted in this change.** Early receipts support workflow handoff. A maintained Argo/Kubeflow example needs an actual target workflow and cache/retry policy.

**Required evidence before implementation:** Run a tiny successful and failing job through both examples. Downstream work starts only after success; failure is visible; retrying the observer preserves the original application ID; the receipt and declared data URI reach the downstream task.

### 39.2 · Persist a run receipt before a long observation step begins

**Implemented.** Shared early receipt mechanism supports interrupted workflow workers; it cannot recover an unknown submission response. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/cli/receipts.py).

**Feedback: Make pipeline side effects and completion semantics explicit.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/automation.md).

**Scope caution accepted: Avoid introducing an orchestrator-specific controller.** Argo and Kubeflow already own DAG execution and retries. Another controller would add lifecycle and compatibility obligations before thin CLI components establish adoption. Keep scheduling in the workflow engine and Spark submission/observation in the existing shared implementation.

## 40 · Lineage architect

### 40.1 · Add a tested OpenLineage recipe using existing job profiles

**Not adopted in this change.** Operational history is explicitly not lineage. OpenLineage and retained task correlation need a chosen endpoint, event semantics and privacy policy.

**Required evidence before implementation:** The fixture produces captured upstream events containing the expected input/output datasets, stable logical job identity, and supplied parent identity; a second execution has a distinct run identity. The uninstrumented starter remains usable.

### 40.2 · Retain opt-in orchestration correlation in recorded runs

**Not adopted in this change.** Operational history is explicitly not lineage. OpenLineage and retained task correlation need a chosen endpoint, event semantics and privacy policy.

**Required evidence before implementation:** Record an operator-created pod, remove it, and confirm its DAG/run/task/mapping/submission-attempt association remains available. Disabled collection retains none of those fields; invalid context is omitted; unrelated annotations never enter storage.

**Feedback: Keep operational observations distinct from lineage completion.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/run-history.md).

**Scope caution accepted: Avoid building a parallel lineage engine from pod polling.** Pod metadata cannot reveal reliable dataset reads, writes, or commit semantics. Delegate those facts to upstream instrumentation and existing catalogs; use this project for configuration and correlation. A second event collector, catalog, or lineage graph would expand maintenance while offering weaker evidence.

## 41 · Data quality engineer

### 41.1 · Add explicit rejection budgets before publishing ETL outputs

**Not adopted in this change.** Quarantine precedence and SQL assertion guidance are documented. Reject budgets/quality-check APIs need explicit data correctness and rollback contracts.

**Required evidence before implementation:** Against pre-existing output fixtures, an all-invalid batch and an over-budget batch fail without changing clean_orders or daily_revenue; the configured boundary passes; empty input has a documented outcome; the current accepted fixture retains its verified results.

### 41.2 · Offer named SQL quality checks with strict result semantics

**Not adopted in this change.** Quarantine precedence and SQL assertion guidance are documented. Reject budgets/quality-check APIs need explicit data correctness and rollback contracts.

**Required evidence before implementation:** One passing check completes; false, null, zero-row, multi-row, and SQL-error cases fail with the named check; a normal SELECT that returns false remains unchanged outside check mode; summaries never contain sampled dataset rows.

**Feedback: Make the quarantine explanation complete enough to diagnose upstream faults.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/starters.md).

**Scope caution accepted: Do not label Kubernetes success as certified data quality.** The UI accurately describes recorded status as the last observed phase at frontend/src/Runs.tsx:107. Preserve that boundary: missing quality evidence means unknown, not passed. Avoid adding a built-in quality platform when explicit application checks and small reusable conventions address the immediate need.

## 42 · Distributed-systems reliability engineer

### 42.1 · Add a bounded submission-key and receipt reconciliation contract

**Not adopted in this change.** UID cancellation and partial identity are implemented. A durable idempotency ledger needs atomic reservation, fingerprints, reconciliation and retention design.

**Required evidence before implementation:** Inject a lost HTTP response after driver creation, restart the API, and issue ten concurrent replays: all resolve to one driver UID. A changed document conflicts. A replay after driver deletion cannot create another driver within the published retention window.

### 42.2 · Make cancellation target an immutable run identity

**Partially implemented.** UID-bearing handles and guarded cancellation reject replaced/wrong-role drivers. Bounded disappearance observation and an already-gone result are not implemented; accepted deletion is distinct from completed cleanup. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/utils/app_manager.py).

**Feedback: Expose partial-submission identity before declaring submission failure.** The client records confirmed driver identity immediately after creation and exposes submission phase; CLI can persist a partial receipt. An ambiguous create response remains unknown and is never blindly retried. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/spark_on_k8s/client.py).

**Scope caution accepted: Do not add blanket transport retries or an exactly-once marketing claim.** Transport uncertainty is not proof of failed submission. Repeated POSTs currently generate different run identities, and even a future durable submission key would only deduplicate job creation, not application side effects or externally triggered reruns.

## 43 · Quality and chaos engineer

### 43.1 · Exercise public startup deadlines against a deliberately unschedulable driver

**Implemented.** Client and both Airflow profiles exercise unschedulable-driver observation/startup deadlines on disposable Kubernetes. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/tests/integration/airflow/run_kubernetes.py).

### 43.2 · Add one packaged browser-to-Kubernetes log failure contract

**Not adopted in this change.** Real client/Airflow fault tests are expanded without multiplying all combinations. Browser-to-real-cluster streaming remains a separate fixture with gateway setup.

**Required evidence before implementation:** The browser receives the real sentinel through the packaged server without routeWebSocket interception; driver deletion yields the specified terminal/error state within a bounded interval; reopening cannot display data from the closed connection. Failure artifacts include browser trace, server logs, and cluster diagnostics.

**Feedback: Persist scenario outcomes before advancing to the next case.** Client scenario outcomes are persisted after each attempt, including failure category, while preserving the failing process exit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/tests/integration/spark/run_kubernetes.py).

**Scope caution accepted: Avoid multiplying every fault across every supported version.** The existing runtime matrix already exercises three Spark releases. Initially run deterministic lifecycle faults on one representative stack; expand only when version-dependent evidence justifies it. Random pod/node destruction and blanket retries would make failures harder to interpret while consuming the maintainers’ CI budget.

## 44 · OSS maintainer

### 44.1 · Require release eligibility evidence for the tagged commit

**Implemented.** Exact-tag workflow eligibility gate, nonpublishing validation and artifact-version evidence before publication. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/scripts/release_eligibility.py).

### 44.2 · Define a small supported surface and a practical deprecation rule

**Documented.** Documented public boundaries, compatibility expectations and support scope; no indefinite enterprise SLA. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/support.md).

**Feedback: Make the roadmap express choices rather than open-ended promises.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/TODO.md).

**Scope caution accepted: Avoid automatic feature parity across every integration.** TODO asks both the CLI and Airflow operator to support all Python-client features. Treat parity as a deliberate choice: some capabilities belong only in the SDK or existing configuration escape hatches. Requiring every new capability in every surface multiplies documentation, compatibility, and review obligations for a small maintenance team. Build an adapter only when a concrete workflow justifies it.

## 45 · Technical writer

### 45.1 · Add a start-here route map with prerequisite checkpoints

**Documented.** A start-here route selector links canonical existing paths rather than duplicating installation instructions. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/start-here.md).

### 45.2 · Turn diagnostics documentation into an evidence-first troubleshooting path

**Not adopted in this change.** Canonical navigation is delivered; operational guidance links existing diagnostics. No second tutorial framework is introduced.

**Required evidence before implementation:** Using four scripted scenarios, an unfamiliar reader selects the intended next inspection step in at least three cases and correctly explains that empty or incomplete diagnostics do not prove health in both relevant scenarios.

**Feedback: Reduce duplicated installation and getting-started instructions.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/start-here.md).

**Scope caution accepted: Avoid a second tutorial framework or a broad documentation rewrite.** The repository already has tested examples and precise technical guides. A small navigation layer, focused troubleshooting guidance, and fewer duplicated commands offer a more maintainable learning improvement than a new example suite, documentation portal, or speculative beginner curriculum. Validate discovery friction before expanding scope.

## 46 · Accessibility specialist

### 46.1 · Preserve keyboard orientation when embedded views change

**Implemented.** Embedded navigation moves focus deliberately; workload quick links include identity. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/frontend/src/SparkConsole.tsx).

### 46.2 · Offer a stable reading snapshot for streaming logs

**Implemented.** Reading can freeze a bounded log snapshot independently of continuing collection. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/frontend/src/Logs.tsx).

**Feedback: Keep the semantic table, but make repeated quick links identifiable.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/frontend/src/Applications.tsx).

**Scope caution accepted: Do not announce the entire live log stream.** Adding an assertive live region to the changing log text would create continuous speech and compete with navigation and error messages. Prefer stable user-controlled reading and sparse state summaries; do not infer accessibility conformance from automated role checks alone.

## 47 · Product designer

### 47.1 · Make snapshot freshness visible where operators inspect and act

**Implemented.** Shared views show successful-fetch age and the intentional pause during detail inspection. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/frontend/src/SnapshotAge.tsx).

### 47.2 · Give diagnostics a compact triage summary with expandable evidence

**Not adopted in this change.** Severity overview, collapsible evidence and prioritization remain separate candidates. Freshness is delivered under47.1.

**Required evidence before implementation:** In paired usability tasks using the same twelve-finding report, target a 25% reduction in median time to locate the highest-severity finding and its next step, without reducing correct interpretation of incomplete evidence. Verify keyboard access to every finding and evidence section.

**Feedback: Keep the existing confirmation safeguards, but always show the target identity.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/frontend/src/Applications.tsx).

**Scope caution accepted: Avoid presenting retained snapshots as definitive reliability or duration analytics.** The recorder can miss entire short-lived runs and unobserved transitions, and disappearance does not establish an outcome (docs/user-guide/run-history.md:56–60). Attractive success-rate or exact-duration charts would imply a completeness the data does not provide. Favor explicit observation semantics until an appropriate event source and interpretation are established.

## 48 · Enterprise evaluator

### 48.1 · Publish one reproducible deployment recipe per release

**Foundation only.** Release evidence ties package artifacts, image inputs and eligibility to a commit; deployment guide records the selected chart/image combination. Real target-cluster qualification remains required. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/releases.md).

### 48.2 · Provide a rehearsable upgrade and recovery exercise for the single-pod deployment

**Not adopted in this change.** Release/deployment evidence is delivered. A destructive upgrade/restore exercise needs a selected supported deployment and explicit data/recovery fixture.

**Required evidence before implementation:** An engineer who did not write the runbook completes the drill on staging, restores the chosen checkpoint's run UIDs, and records downtime and recovery time without undocumented steps. The team can make an explicit acceptance decision against its own service target.

**Feedback: Make the support boundary as precise as the deployment boundaries.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/support.md).

**Scope caution accepted: Do not build an enterprise control plane to satisfy an evaluation checklist.** Adding tenant identity, HA databases, proprietary support workflows, and policy administration at once would expand the ownership burden for maintainers and the small teams evaluating the project. First make the existing bounded deployment reproducible, recoverable, and explicit about support; reject adoption scenarios whose requirements exceed those boundaries.

## 49 · Developer advocate

### 49.1 · Add a project-fit landing page with outcome-based entry paths

**Documented.** Start-here explains project fit and routes to existing examples. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/start-here.md).

### 49.2 · Offer a first-contribution pathway with bounded review contracts

**Partially documented.** Contributor guidance, scoped roadmap and repository skills describe concrete entry points and checks. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/development.md).

**Feedback: Replace stale roadmap promises with scoped, current opportunities.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/TODO.md).

**Scope caution accepted: Avoid an integration-logo collection without maintained user journeys.** Adding badges, superficial scheduler adapters or many nearly identical demos would increase the perceived support surface without proving adoption value. Prefer a few verifiable outcomes using the existing SDK, CLI, shared UI and starters, and label unverified ecosystem combinations explicitly.

## 50 · Skeptical minimalist CTO

### 50.1 · Ship one reproducible release bundle across package, chart and API image

**Foundation only.** Release artifact evidence and exact-commit gate establish reproducible inputs. A full enterprise-certified bundle is not promised. [Implementation or contract](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/releases.md).

### 50.2 · Provide one tested external-gateway recipe for operator access

**Not adopted in this change.** Release reproducibility is delivered. One actual external gateway needs adoption-driven HTTP/WebSocket/identity qualification before becoming a supported deployment recipe.

**Required evidence before implementation:** The example passes authenticated browser and Airflow requests, streams logs, rejects unauthenticated and forged-identity requests, rejects invalid-CSRF writes, and never exposes the operator token to browser configuration. Direct backend access is blocked in the deployment fixture.

**Feedback: Make compatibility retirement criteria part of the product scope.** The clarified behavior and ownership boundaries are captured in the linked implementation or canonical guide; existing support limits remain explicit. [Response](https://github.com/hussein-awala/spark-on-k8s/blob/main/docs/user-guide/support.md).

**Scope caution accepted: Refuse a multi-tenant, highly available Spark control plane.** Do not grow operator admission into tenant IAM, the metadata recorder into a distributed database service, or the client into a scheduler. The documented trust-boundary and single-pod limits are coherent product boundaries. Integration requests should first use existing gateways, orchestrators and observability systems; revisit only with concrete users and dedicated maintainers. Those responsibilities would change the project's operating model, not merely add features.
