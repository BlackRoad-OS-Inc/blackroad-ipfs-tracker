# blackroad-ipfs-tracker

Production-grade IPFS content tracker with SQLite persistence, gateway availability checks, and manifest export.

## Features

- Track IPFS CIDs with rich metadata (name, type, tags, description)
- Pin/unpin via IPFS HTTP API or CLI daemon
- Verify availability across multiple public gateways
- Full-text search across tracked content
- Bulk import from JSON manifests
- Export manifest for backup/migration
- SQLite persistence in `~/.blackroad/ipfs_tracker.db`

## Usage

```bash
# Add a CID
python ipfs_content_tracker.py add QmYwAP... --name "my-doc.pdf" --tags "archive,2024" --pin

# List all tracked content
python ipfs_content_tracker.py list

# Verify availability
python ipfs_content_tracker.py verify <content-id>

# Search
python ipfs_content_tracker.py search "quarterly report"

# Export manifest
python ipfs_content_tracker.py export --output manifest.json

# Bulk import
python ipfs_content_tracker.py import manifest.json

# Statistics
python ipfs_content_tracker.py stats
```

## Testing

```bash
pip install pytest
pytest tests/ -v
```

## Architecture

- **`ipfs_content_tracker.py`** — Core library + CLI (400+ lines)
- **SQLite tables**: `content`, `pin_events`, `availability_checks`
- **Gateway support**: ipfs.io, cloudflare-ipfs, pinata, dweb.link
- **IPFS integration**: Checks local daemon at `http://127.0.0.1:5001` or `ipfs` CLI
