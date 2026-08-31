#!/usr/bin/env python3
"""Generate the website's ADR index (adrs/index.json) from docs/decisions/.

Each entry: {"id": "0016", "file": "ADR-0016-....md", "title": "..."} — id from
the filename, title from the first heading with the "ADR-XXXX — " prefix
stripped. Sorted by id so the site lists ADRs in build order automatically.
"""
import json, re, sys
from pathlib import Path

src, dst = Path(sys.argv[1]), Path(sys.argv[2])
entries = []
for f in sorted(src.glob("ADR-*.md")):
    m = re.match(r"ADR-([0-9]+[a-z]?)-", f.name)
    if not m:
        continue
    title = f.name
    for line in f.read_text().splitlines():
        if line.startswith("# "):
            title = re.sub(r"^#\s*ADR-[0-9]+[a-z]?\s*[—-]\s*", "", line).strip()
            break
    entries.append({"id": m.group(1), "file": f.name, "title": title})
dst.parent.mkdir(parents=True, exist_ok=True)
dst.write_text(json.dumps(entries, indent=1))
print(f"{dst}: {len(entries)} ADRs")
