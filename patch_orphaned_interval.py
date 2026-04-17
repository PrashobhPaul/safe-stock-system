#!/usr/bin/env python3
"""
patch_orphaned_interval.py — Fixes the splash-screen hang in ProfitPilot index.html

Root cause: An orphaned `refreshTmr = setInterval(...)` block runs at parse time
before REFRESH_MS, isOpen, loadData, renderAll are declared, causing a ReferenceError
that kills the entire script. init() never runs → splash never hides.

Usage:
    python patch_orphaned_interval.py index.html
"""
import sys
from pathlib import Path

def main():
    if len(sys.argv) != 2:
        print("Usage: python patch_orphaned_interval.py index.html")
        return 1

    path = Path(sys.argv[1])
    if not path.exists():
        print(f"✗ {path} not found")
        return 1

    original = path.read_text(encoding="utf-8")
    lines = original.splitlines(keepends=True)
    output = []
    fixed = False

    i = 0
    while i < len(lines):
        line = lines[i]
        # Detect the orphaned setInterval (top-level, before const declarations)
        if (not fixed
            and 'refreshTmr = setInterval(async () => {' in line
            and i + 3 < len(lines)
            and 'REFRESH_MS' in lines[i + 3]):
            # Verify it's the orphaned one: previous non-blank line should be a comment
            prev = i - 1
            while prev >= 0 and lines[prev].strip() == '':
                prev -= 1
            if prev >= 0 and lines[prev].strip().startswith('//'):
                # Skip the 4-line block + any trailing blank lines
                i += 4
                while i < len(lines) and lines[i].strip() == '':
                    i += 1
                fixed = True
                print("✓ Removed orphaned setInterval block")
                continue

        output.append(line)
        i += 1

    if not fixed:
        print("⚠ Orphaned setInterval block not found — file may already be fixed")
        return 0

    # Backup
    backup = path.with_suffix('.html.bak2')
    if not backup.exists():
        backup.write_text(original, encoding="utf-8")
        print(f"✓ Backup → {backup}")

    path.write_text(''.join(output), encoding="utf-8")
    print(f"✓ Fixed {path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
