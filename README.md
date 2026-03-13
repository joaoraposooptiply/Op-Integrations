# Op-Integrations

Optiply Singer taps, targets, and ETL notebooks.

## Structure
```
taps/          # Singer taps (extract from source systems) — each has its own repo
targets/       # Singer targets (load to Optiply) — each has its own repo
etl/           # ETL notebooks (transform between tap → target)
docs/          # Integration documentation and data mappings
shared/        # Shared utilities
```

## Stack
- **SDK:** `hotglue_singer_sdk`
- **Spec:** Singer (open source ETL)
- **Language:** Python 3.9+

## Integration Flow
```
Source System → Tap (extract) → Snapshot (cache) → ETL (transform) → Target (load) → Optiply
```

## Integrations

| Integration | Tap | Target | ETL | Docs |
|---|---|---|---|---|
| Extend Commerce (Lxir) | [tap-extend](https://github.com/joaoraposooptiply/tap-extend) | [target-extend](https://github.com/joaoraposooptiply/target-extend) | [etl/extend](etl/extend/) | [docs/extend](docs/extend/) |
