import json
import sys

if len(sys.argv) != 3:
    print("Usage: python gen_catalog.py <discover_file> <output_catalog_file>")
    sys.exit(1)

discover_path = sys.argv[1]
output_catalog_path = sys.argv[2]

discover = json.load(open(discover_path))
catalog_file = open(output_catalog_path, "w")
catalog = {
    "streams": [
        {"stream": s, "sync_mode": "full_refresh", "destination_sync_mode": "append"}
        for s in discover["catalog"]["streams"]
    ]
}
json.dump(catalog, catalog_file, indent=2)
catalog_file.close()
