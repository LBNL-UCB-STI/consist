---
hide:
  - navigation
  - title
  - toc
---

<section id="home-page" class="home-hero">
  <div class="home-hero__inner">
    <p class="home-hero__eyebrow">Consist documentation</p>
    <picture class="home-hero__logo">
      <source media="(prefers-color-scheme: dark)" srcset="assets/logo-dark.png">
      <source media="(prefers-color-scheme: light)" srcset="assets/logo.png">
      <img src="assets/logo.png" alt="Consist">
    </picture>
    <h1>Know exactly which code, config, and data produced every result.</h1>
    <p class="tagline">Consist records what your simulation runs consumed and produced, reuses prior outputs when nothing changed, and lets you trace any figure back to the inputs that made it. Built for scientific workflows where reproducibility is not optional.</p>
    <p class="tagline">Reference documentation for recording workflow runs, recovering outputs, and analyzing scenario lineage with DuckDB-backed metadata.</p>
    <div class="home-actions">
      <a class="md-button md-button--primary" href="getting-started/quickstart/">Quickstart</a>
      <a class="md-button" href="usage-guide/">Usage guide</a>
      <a class="md-button" href="api/">API reference</a>
    </div>
    <dl class="home-summary">
      <div class="home-summary__item">
        <dt>Recorded state</dt>
        <dd>Code, config, inputs, outputs, and scenario metadata</dd>
      </div>
      <div class="home-summary__item">
        <dt>Cached execution</dt>
        <dd>Reuse unchanged runs and hydrate prior outputs on demand</dd>
      </div>
      <div class="home-summary__item">
        <dt>Queryable history</dt>
        <dd>Search past runs, recorded files, and scenario metadata in DuckDB</dd>
      </div>
    </dl>
  </div>
</section>

<section class="home-docs">
  <section class="home-section">
    <h2>Start here</h2>
    <p>Read these pages first if you are new to Consist.</p>
    <ol class="home-simple-list">
      <li><a href="getting-started/installation/">Installation</a></li>
      <li><a href="getting-started/quickstart/">Quickstart</a></li>
      <li><a href="getting-started/first-workflow/">First Workflow</a></li>
      <li><a href="concepts/overview/">Core Concepts</a></li>
      <li><a href="usage-guide/">Usage Guide</a></li>
    </ol>
  </section>

  <section class="home-section">
    <h2>What Consist records</h2>
    <p>For each run, Consist records the execution context and artifact metadata needed to reproduce or inspect a result.</p>
    <ul class="home-simple-list">
      <li>the function or external tool that ran</li>
      <li>config and identity inputs that affect the run signature</li>
      <li>input and output artifacts, including file hashes and lineage</li>
      <li>scenario, stage, phase, and facet metadata for later querying</li>
    </ul>
    <p>On a cache hit, Consist returns previous output artifact metadata instead of re-running the step. Load, hydrate, or materialize bytes only when the next tool needs them.</p>
    <p>This matters when a reviewer asks which scenario produced a published figure, when a colleague needs to reproduce last quarter's result after the config has drifted, or when a parameter sweep wastes hours re-running preprocessing that didn't change.</p>
  </section>

  <section class="home-section">
    <h2>Common reference needs</h2>
    <ul class="home-link-list">
      <li><a href="usage-guide/"><strong>Usage Guide</strong><span>Choose between <code>run</code>, <code>trace</code>, and <code>scenario</code>.</span></a></li>
      <li><a href="concepts/caching-and-hydration/"><strong>Caching & Hydration</strong><span>Understand cache hits, misses, hydration, and materialization.</span></a></li>
      <li><a href="getting-started/first-workflow/"><strong>First Workflow</strong><span>Pass artifacts between workflow steps.</span></a></li>
      <li><a href="guides/historical-recovery/"><strong>Historical Recovery</strong><span>Restore historical outputs or stage inputs.</span></a></li>
      <li><a href="containers-guide/"><strong>Container Guide</strong><span>Run Docker, Singularity, or Apptainer tools.</span></a></li>
      <li><a href="building-domain-tracker/"><strong>Domain Tracker</strong><span>Wrap Consist behind domain-specific verbs for shared codebases.</span></a></li>
      <li><a href="concepts/data-materialization/"><strong>Data Materialization</strong><span>Ingest data for SQL analysis.</span></a></li>
      <li><a href="schema-export/"><strong>Schema Export</strong><span>Generate SQLModel schemas from captured files.</span></a></li>
      <li><a href="cli-reference/"><strong>CLI Reference</strong><span>Inspect runs from the terminal.</span></a></li>
      <li><a href="db-maintenance/"><strong>DB Maintenance</strong><span>Repair or compact the provenance database.</span></a></li>
      <li><a href="troubleshooting/"><strong>Troubleshooting</strong><span>Debug a cache miss or missing output.</span></a></li>
      <li><a href="api/"><strong>API Reference</strong><span>Look up Python signatures.</span></a></li>
    </ul>
  </section>

  <section class="home-section">
    <h2>Reference paths</h2>
    <ul class="home-link-list home-link-list--compact">
      <li><a href="concepts/overview/"><strong>Concepts</strong><span>Mental model and behavior.</span></a></li>
      <li><a href="usage-guide/"><strong>Guides</strong><span>Workflow patterns and integrations.</span></a></li>
      <li><a href="cli-reference/"><strong>Operations</strong><span>CLI, maintenance, and troubleshooting.</span></a></li>
      <li><a href="api/"><strong>API Reference</strong><span>Generated Python API pages.</span></a></li>
      <li><a href="glossary/"><strong>Glossary</strong><span>Short definitions for Consist terms.</span></a></li>
    </ul>
  </section>
</section>
