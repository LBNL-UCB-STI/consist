# Consist LLM Skill

`skills/consist/` is an LLM-provider-agnostic skill for agents working with
Consist in downstream projects. It helps agents bootstrap Consist in a new
project, integrate it into an existing workflow, debug cache or provenance
behavior, and inspect recorded runs and artifacts.

To use it, install or copy the `skills/consist/` folder into your agent tool's
skill directory, or point the tool directly at `skills/consist/SKILL.md`. Start
from `SKILL.md`, then load only the task-relevant files under
`references/`.

## Install From GitHub

If you are reading this on GitHub, download or clone the Consist repository and
copy the entire `skills/consist/` folder into your agent tool's skill directory.
Preserve the folder layout; do not copy `SKILL.md` by itself.

For Codex, the default install shape is:

```bash
git clone https://github.com/LBNL-UCB-STI/consist.git
mkdir -p "${CODEX_HOME:-$HOME/.codex}/skills"
cp -R consist/skills/consist "${CODEX_HOME:-$HOME/.codex}/skills/"
```

For active development from a local clone, you can symlink instead of copying so
edits stay in sync:

```bash
mkdir -p "${CODEX_HOME:-$HOME/.codex}/skills"
ln -s "$(pwd)/skills/consist" "${CODEX_HOME:-$HOME/.codex}/skills/consist"
```

`skills/consist/agents/openai.yaml` is optional OpenAI UI metadata. Keep
canonical guidance in `SKILL.md` and `references/`, not in provider-specific
metadata files.
