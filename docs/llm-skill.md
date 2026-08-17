# Consist LLM Skill

Consist includes an LLM-provider-agnostic skill for agents that help users
instrument downstream scientific workflows. The skill packages project-specific
guidance for bootstrapping Consist, wrapping legacy file-writing steps,
debugging cache and provenance behavior, inspecting runs, and querying recorded
artifacts.

The canonical skill lives in the repository at
[`skills/consist/`](https://github.com/LBNL-UCB-STI/consist/tree/main/skills/consist).

## When to use it

Use the skill when asking an agent to:

- add Consist to a new project or pipeline;
- choose between `run(...)`, `trace(...)`, and `scenario(...)`;
- wrap external tools, containers, or legacy code that writes files;
- declare outputs, output sets, schema profiles, and cache identity;
- inspect or analyze recorded runs and artifacts;
- debug cache misses, hydration, mount resolution, or scenario wiring.

## Install

Download or clone the Consist repository and copy the full `skills/consist/`
folder into your agent tool's skill directory. Preserve the folder layout;
`SKILL.md` depends on the files under `references/`.

For Codex, the default install shape is:

```bash
git clone https://github.com/LBNL-UCB-STI/consist.git
mkdir -p "${CODEX_HOME:-$HOME/.codex}/skills"
cp -R consist/skills/consist "${CODEX_HOME:-$HOME/.codex}/skills/"
```

For active development from a local Consist checkout, use a symlink instead so
skill edits stay in sync:

```bash
mkdir -p "${CODEX_HOME:-$HOME/.codex}/skills"
ln -s "$(pwd)/skills/consist" "${CODEX_HOME:-$HOME/.codex}/skills/consist"
```

## Maintain

Keep canonical guidance in `skills/consist/SKILL.md` and
`skills/consist/references/`. Provider-specific files such as
`skills/consist/agents/openai.yaml` are optional UI metadata.

After editing the skill, validate it with your agent platform's skill validator.
With Codex's `skill-creator` tooling, the command shape is:

```bash
python path/to/skill-creator/scripts/quick_validate.py path/to/skills/consist
```
