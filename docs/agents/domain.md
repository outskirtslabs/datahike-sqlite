# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repository root.
- **`docs/adr/`** — if it exists, read ADRs that touch the area you're about to work in.

If either location doesn't exist, **proceed silently**. Don't flag its absence or suggest creating it upfront. Skill(domain-modeling) creates domain documentation lazily when terms or decisions are resolved.

## File structure

This repository uses a single-context layout:

/
├── CONTEXT.md
├── docs/adr/
└── src/

## Use the glossary's vocabulary

When output names a domain concept—in an issue title, refactor proposal, hypothesis, or test name—use the term defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the needed concept isn't in the glossary, reconsider whether the language belongs to the project or note the gap for Skill(domain-modeling).

## Flag ADR conflicts

If output contradicts an existing ADR, surface it explicitly rather than silently overriding it.
