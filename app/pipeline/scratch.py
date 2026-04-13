import json

with open('fixtures/silver_entity_discovery_benchmark.json', 'r') as f:
    data = json.load(f)

print(len(data))
