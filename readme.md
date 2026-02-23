# DB Federator Local Test Pack

- Install deps: `python -m pip install -r requirements.txt`
- Run flow: `source .venv/bin/activate && python main.py`
- Run flow (plain/no color): `python main.py --no-color`
- Run API server: `python main.py --mode server --host 127.0.0.1 --port 8080`
- API query (rich mapping): `curl -s -X POST http://127.0.0.1:8080/query -H 'Content-Type: application/json' -d '{"query_items":[{"input_id":"exact","device_id":"device_a","circle_id":"circle_1","cluster_id":"cluster_red"}]}'`
- Run benchmark: `python benchmark.py --runs 50`
- Run benchmark (plain/no color): `python benchmark.py --runs 50 --no-color`
- CTO pack: `PRESENTATION.md`
- Architecture: `docs/HLD.md`, `docs/LLD.md`
- Log sequence: `docs/LOG_FLOW.md`
- Demo runbook: `docs/DEMO_15MIN.md`
- Work log: `KNOWLEDGE.md`
