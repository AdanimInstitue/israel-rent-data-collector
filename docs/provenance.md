# Provenance

The public bundle is written under `data/public_bundle/` and includes:

- `locality_crosswalk.csv`
- `source_inventory.csv`
- `manifest.json`

`manifest.json` records only bundle-relative paths and rejects absolute or escaping paths during validation.

`source_inventory.csv` is generated from `src/rent_collector/source_registry.py` and records:

- source identity
- access method
- public status
- attribution requirements
- redistribution note
