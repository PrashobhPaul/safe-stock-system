"""
generate_icons.py — StockSage India
=====================================
Run once to generate all PWA icon sizes.
Requires: pip install Pillow cairosvg

Usage:
    python generate_icons.py

Output: icons/ folder with all required PNG sizes
"""

import os
import struct
import zlib

# Create icons directory
os.makedirs("icons", exist_ok=True)

# We'll generate icons programmatically using pure Python (no external deps)
# Creates a dark green background with "SS" monogram

def create_png(size, bg=(6, 10, 6), accent=(0, 230, 118)):
    """Create a simple PNG icon with StockSage branding using pure Python."""
    import struct, zlib

    width = height = size
    
    def write_chunk(chunk_type, data):
        chunk = chunk_type + data
        return (struct.pack('>I', len(data)) + chunk +
                struct.pack('>I', zlib.crc32(chunk) & 0xffffffff))

    # PNG signature
    png = b'\x89PNG\r\n\x1a\n'
    
    # IHDR
    ihdr = struct.pack('>IIBBBBB', width, height, 8, 2, 0, 0, 0)
    png += write_chunk(b'IHDR', ihdr)
    
    # Create pixel data
    pixels = []
    cx, cy = width // 2, height // 2
    r = size // 2
    
    for y in range(height):
        row = b'\x00'  # filter byte
        for x in range(width):
            # Circle mask with padding
            dx, dy = x - cx, y - cy
            dist = (dx*dx + dy*dy) ** 0.5
            pad = size * 0.08
            
            if dist <= r - pad:
                # Inside circle
                # Draw a simple upward arrow / chart line
                # Background
                pr, pg, pb = bg[0] + 8, bg[1] + 12, bg[2] + 8  # slightly lighter bg
                
                # Accent stripe at top (like a chart going up)
                line_y = int(size * 0.35)
                line_y2 = int(size * 0.55)
                line_x1 = int(size * 0.25)
                line_x2 = int(size * 0.75)
                
                # Draw rising line
                if line_x1 <= x <= line_x2:
                    progress = (x - line_x1) / (line_x2 - line_x1)
                    target_y = int(line_y + (line_y2 - line_y) * (1 - progress))
                    if abs(y - target_y) <= max(1, size // 40):
                        pr, pg, pb = accent[0], accent[1], accent[2]
                    elif y > target_y and y < target_y + max(2, size // 30):
                        # Glow below line
                        pr = min(255, int(pr + accent[0] * 0.3))
                        pg = min(255, int(pg + accent[1] * 0.3))
                        pb = min(255, int(pb + accent[2] * 0.3))
                
                # Bottom "S" hint - accent dot
                center_dist = ((x-cx)**2 + (y-cy)**2) ** 0.5
                if center_dist < size * 0.08:
                    pr, pg, pb = accent[0], accent[1], accent[2]
                    
                row += bytes([pr, pg, pb])
            elif dist <= r:
                # Border
                row += bytes([accent[0], accent[1], accent[2]])
            else:
                # Outside — transparent area (use bg for PNG)
                row += bytes([bg[0], bg[1], bg[2]])
        
        pixels.append(row)
    
    # IDAT
    raw = b''.join(pixels)
    compressed = zlib.compress(raw, 9)
    png += write_chunk(b'IDAT', compressed)
    
    # IEND
    png += write_chunk(b'IEND', b'')
    
    return png


SIZES = [72, 96, 128, 144, 152, 192, 384, 512]

print("Generating StockSage PWA icons...")
for size in SIZES:
    data = create_png(size)
    path = f"icons/icon-{size}.png"
    with open(path, 'wb') as f:
        f.write(data)
    print(f"  ✓ {path} ({size}×{size})")

print(f"\n✅ Generated {len(SIZES)} icons in icons/")
print("\nNext steps:")
print("  git add icons/")
print("  git commit -m 'Add PWA icons'")
print("  git push")
