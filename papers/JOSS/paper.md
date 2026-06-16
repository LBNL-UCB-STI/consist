---
title: 'Consist: Provenance-Aware Caching and Queryable Provenance for Scientific Workflows'
tags:
  - Python
  - provenance
  - caching
  - reproducibility
  - scientific workflows
  - transportation modeling
authors:
  - name: Zachary A. Needell
    affiliation: 1
    corresponding: true
affiliations:
  - name: Lawrence Berkeley National Laboratory, United States
    index: 1
    ror: 02jbv0t02
date: 16 June 2026
bibliography: paper.bib
---

# Summary

Computational scientific workflows produce outputs that are functions of three things: the code used to produce them, the configuration settings that parameterize their behavior, and the set of input objects they consume. Each of those inputs may itself be the output of an earlier computational step, creating dependency chains that can extend across model runs. In practice, these chains are rarely explicit: researchers frequently cannot trace which exact code, configuration, and upstream inputs produced a given result, or which downstream outputs are affected by an upstream change.

Consist is a Python library that makes this lineage explicit, persistent, and queryable. It wraps ordinary Python computations in a provenance layer that records which code, configuration, and input artifacts produced each output, computing a deterministic identity signature. Each step's identity incorporates the identities of its inputs, including the signatures of the runs that produced them. As a result, any output can be traced back through its full chain of upstream dependencies to the original inputs and assumptions at the system boundary. A consequence of this identity-based lineage is correct, automatic caching: upstream changes invalidate only the downstream steps that genuinely depend on them, and unchanged steps are safely reused without re-execution.

Provenance is persisted in a local DuckDB database [@raasveldt2019duckdb] that also serves as an analytical engine: users can query across run outputs---whether ingested or remaining distributed on disk---so that cross-scenario comparison, lineage tracing, and result recovery are available through standard SQL. The entire stack is self-contained with no external services, reducing barriers to provenance tracking in research settings where simplicity of deployment matters.


# Statement of Need

Many scientific domains rely on workflows where multiple models interact iteratively: climate projections couple atmosphere and ocean models, energy system analyses link capacity expansion with dispatch simulation, and epidemiological studies chain population dynamics with intervention models. In transportation planning---the domain that motivated Consist---land-use models produce population and employment forecasts; vehicle ownership models determine household fleets; activity-based demand models generate travel behavior; and network assignment models evaluate congestion and accessibility. These models run iteratively across multi-year horizons under multiple policy scenarios, with congestion feeding back into demand and demand feeding back into land use.

These workflows create four recurring problems. First, **provenance ambiguity**: when a result changes between runs, it is often unclear which upstream input, configuration parameter, or code change caused the difference. Second, **redundant recomputation**: extending a converged 10-iteration equilibrium run to 15 iterations should not require re-executing the first 10, yet without explicit dependency tracking and state management it typically does. Third, **scattered outputs**: results accumulate as directories of files with ad hoc naming, making systematic comparison across scenarios a manual exercise. Fourth, **implicit wiring**: dependencies between steps are encoded through file paths, global variables, or framework conventions rather than in inspectable, queryable structure. Consist addresses these problems by treating each workflow step as a provenance-aware computation with explicit identity and lineage.

# State of the Field

Existing tools address fragments of this problem. The closest analogue in scientific computing is Snakemake [@molder2021snakemake], which provides directed acyclic graph (DAG)-based execution with file-content-based recomputation. However, Snakemake requires expressing workflows in its own rule-based domain-specific language (DSL) rather than wrapping existing Python code, and its focus is correct re-execution rather than a persistent, queryable provenance store. Workflow engines such as Nextflow [@ditommaso2017nextflow] similarly emphasize orchestration, requiring specific execution models without providing post-hoc queryable provenance. Earlier Python-native provenance tools such as Sumatra [@davison2012sumatra] and Recipy [@recipy] track runs with minimal code changes but lack identity-based caching, multi-step lineage propagation, and queryable output stores. DVC [@iterative2020dvc] versions data files and pipelines but targets data-asset management rather than run-level identity and experiment history, while MLflow [@chen2020mlflow] offers rich experiment tracking oriented toward machine-learning training loops rather than multi-step simulations with heterogeneous configuration ecosystems. OpenLineage [@openlineage2026] standardizes lineage event emission but does not itself persist provenance or use lineage for execution decisions; Consist can optionally emit OpenLineage events while natively handling provenance storage, querying, and identity-based cache resolution.

Apache Hamilton [@hamilton2024] is the closest existing tool: it builds function-level DAGs with caching and Merkle-style identity propagation. However, Hamilton requires its steps to be pure Python functions, a major barrier for scientific workflows that rely on containers and subprocesses. It also lacks support for the complex configuration structures common in simulation pipelines and provides no analytical engine to make provenance metadata and artifact contents queryable.

Consist fills this gap by combining non-invasive provenance tracking, deterministic identity-based caching with lineage propagation, and a queryable analytical surface for post-hoc comparison—without requiring a workflow DSL or external services.

# Software Design

## The Identity Model

Consist's core mechanism is a deterministic three-part signature computed for each computational step: `signature = SHA256(code_hash || config_hash || input_hash)`. The code hash captures relevant implementation state (configurable from full-repository Git state, callable-scoped source code, or container manifest). The config hash captures a canonicalized dictionary of model settings. The input hash captures the identity of declared input artifacts, and when those artifacts were themselves produced by Consist, their identity incorporates the producing run's signature, inducing a Merkle-style DAG over the workflow. A change to any upstream step propagates invalidation only through genuinely dependent downstream computations (\autoref{fig:diagram}).

![Figure 1. Identity propagation across workflow steps. Each step's signature is computed from its code identity, configuration, and input identity. When Step B consumes an artifact produced by Step A, Signature A is incorporated into Step B's input identity (solid arrow), propagating cache invalidation through the workflow. The artifact's bytes are consumed separately at runtime (dashed arrow) and need not be materialized when a cache hit makes re-execution unnecessary.\label{fig:diagram}](diagram.pdf)

## Why This Is Hard in Practice

Real workflows complicate this clean conceptual model.

**Structured configurations:** In complex simulation ecosystems, "configuration" simultaneously plays several roles: parameters that define experimental identity and must trigger recomputation when changed; metadata useful for later filtering and grouping but irrelevant to execution semantics; and large external file trees (YAML, CSV, XML) that collectively define model behavior but are impractical to store as queryable structured metadata. Consist separates these explicitly into identity-bearing configuration (structured parameters that are both hashed for cache identity and stored as queryable metadata), facets (indexed for analytical queries but not part of cache identity), and identity inputs (files or directories whose content is hashed for cache identity but not stored as queryable metadata).

**Functions with side effects:** Domain models write files, call subprocesses, and interact with external tools that cannot be refactored into pure functions. Consist tracks such processes through their declared inputs, configuration, and outputs without requiring domain code to become aware of the provenance engine. This motivates a design contrast from pure-function frameworks---Consist artifacts live on disk rather than in memory, and Consist loads or attaches them to steps only as required. This better accommodates outputs too large to hold in memory, which would otherwise demand manual serialization logic at every step boundary. It also enables a distinction between "hydration" (recovering a cached result's metadata and provenance) and "materialization" (ensuring its bytes exist on disk), so a cache hit can resolve cheaply without reading or duplicating files at all. In a multi-step workflow where only a downstream parameter has changed, the entire upstream chain can be validated and reused without copying or even reading the intermediate files. This correctness, however, depends on inputs and code changes being faithfully declared; undeclared dependencies (e.g., untracked files, environment state, or non-deterministic computation) can produce stale cache hits.

**Post-run analysis:** Consist persists provenance in both JSON snapshots and a DuckDB database for analytical querying. Selected tabular outputs can be ingested alongside provenance metadata, while large or infrequently queried outputs remain file-backed. Users can query across both through a unified analytical surface with provenance columns (run ID, scenario, year, iteration) attached for grouping and filtering. Consequently, agent-level trip records and population tables---millions of rows stored as distributed Parquet files---can be queried through a single SQL surface, enabling analyses such as regional vehicle miles traveled trends by home geography across policy scenarios without loading full datasets into memory.

## Graduated Formalization

At a minimum, Consist requires users to declare the inputs, outputs, and identity-bearing configuration of each step---typically a few additional lines per function call. This declaration makes dependency structure explicit and creates a legible record of what each step consumes and produces. Beyond this minimum, users can optionally add facets for analytical filtering, register configuration adapters that canonicalize complex domain configuration (current adapters exist for ActivitySim [@activitysim2026] and BEAM [@laarabi2024beam]), and define typed schemas for important artifacts. Each layer requires additional upfront effort but delivers proportionally richer capabilities, from basic cache reuse through queryable provenance to memory-efficient typed analytical views over large simulation outputs.

# Research Impact Statement

Consist was developed to support PILATES, a workflow runtime used in the BEAM CORE modeling ecosystem at Lawrence Berkeley National Lab [@spurlock2024beamcore]. PILATES coordinates UrbanSim [@waddell2002urbansim] (land use), ATLAS [@jin2024atlas] (vehicle ownership), ActivitySim [@activitysim2026] (travel demand), and BEAM [@laarabi2024beam] (network assignment) across multi-year, multi-iteration scenario lifecycles. PILATES is currently used in DOE-funded scenario analyses of vehicle technology change, curb utilization, and supply-chain optimization.

During PILATES integration, adding Consist required making model-step input and output bindings explicit. That process surfaced a latent cross-model dependency error: ActivitySim's population-source input was bound to a valid but wrong-year UrbanSim artifact, using the current-year datastore rather than the forecast-year population snapshot produced after ATLAS updates. The issue was difficult to detect from individual files alone because each artifact was internally valid; the lineage graph made the semantic mismatch between upstream artifact roles visible.

Consist's caching also delivers practical savings: when ActivitySim or BEAM configurations vary across scenarios, unchanged upstream UrbanSim and ATLAS steps are identified by their signatures and reused rather than re-executed, typically saving several HPC node-hours per cache hit. The repository includes five example notebooks demonstrating these mechanics in self-contained workflows, covering cache behavior, scenario comparison, parameter sweeps, iterative extension, and cross-run analysis.

# Availability

Consist is available on PyPI (`pip install consist`) and developed openly at https://github.com/LBNL-UCB-STI/consist under a BSD 3-Clause license. Documentation including a quickstart, usage guide, concept guides, API documentation, and the example notebook gallery is hosted at https://lbnl-ucb-sti.github.io/consist/latest/. The repository includes an automated test suite run via continuous integration, contribution guidelines (CONTRIBUTING.md), and a code of conduct. Consist requires Python 3.11+ and depends on DuckDB, SQLModel, pandas, and PyArrow, with optional dependencies for spatial formats, xarray-based matrix views, and data ingestion via dlt.

# AI Usage Disclosure

Anthropic Claude Opus 4.5-4.7 and OpenAI ChatGPT 5.2-5.5 used across software development (code generation, refactoring, test scaffolding), documentation drafting, and paper editing. The authors reviewed, edited, and validated all AI-assisted text and code; AI-assisted software components were validated through automated tests and real-world use in the PILATES integration for funded analysis work. All architectural decisions, technical claims, and domain-specific content reflect the authors' direct experience developing and deploying Consist.

# Acknowledgements

This paper and the work described were sponsored by the U.S. Department of Energy
(DOE) Vehicle Technologies Office (VTO) under the Systems and Modeling for
Accelerated Research in Transportation (SMART) Mobility Laboratory Consortium, an
initiative of the Energy Efficient Mobility Systems
(EEMS) Program. Lawrence Berkeley National Laboratory operates under DOE Contract
No.DE-AC02-05CH11231. The views expressed in the article do not necessarily represent
the views of the DOE or the U.S. Government. The U.S. Government retains a
nonexclusive, paid-up, irrevocable, worldwide license to publish or reproduce the published
form of this work, or allow others to do so, for U.S. Government purposes.

# References







