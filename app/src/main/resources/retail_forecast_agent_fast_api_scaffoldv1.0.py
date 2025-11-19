# Project: retail-forecast-agent

This repository scaffold contains a ready-to-run FastAPI server, JSON schemas, Pydantic models, validators, and a simple orchestrator stub that shows how to wire an LLM -> orchestrator -> forecasting engine flow. Replace forecasting stubs with your real model-serving endpoints.

---

# Folder structure

```
retail-forecast-agent/
├── Dockerfile
├── README.md
├── requirements.txt
├── .env.example
├── pyproject.toml
├── src/
│   └── app/
│       ├── __init__.py
│       ├── main.py                # FastAPI app entry
│       ├── config.py              # configuration loader
│       ├── schemas.py             # pydantic models + JSON schemas
│       ├── validators.py          # jsonschema validation helpers
│       ├── orchestrator.py        # parse command, route to handler
│       ├── forecast_runner.py     # placeholder/adapter to actual forecasting engine
│       ├── llm_client.py          # small wrapper to call LLM if needed
│       └── utils.py               # helper utils (IDs, timestamps)
└── tests/
    └── test_api.py
```

---

# requirements.txt

```
fastapi==0.95.2
uvicorn[standard]==0.22.0
pydantic==1.10.12
jsonschema==4.19.0
python-dotenv==1.0.0
httpx==0.24.1
pytest==7.4.2
```

---

# Dockerfile

```
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src ./src
ENV PYTHONPATH=/app/src
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--loop", "uvloop", "--ws", "auto"]
```

---

# .env.example

```
LLM_API_URL=https://api.openai.example/v1/chat/completions
LLM_API_KEY=YOUR_KEY_HERE
FORECAST_ENGINE_URL=http://forecast-engine.local/predict
SERVICE_ENV=dev
```

---

# src/app/main.py

```python
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError
import uvicorn
from .schemas import CommandIn, CommandResponse
from .validators import validate_against_schema, load_json_schema
from .orchestrator import handle_command

app = FastAPI(title="Retail Forecasting Orchestrator",
              description="Receives LLM JSON commands, validates them, and routes to forecasting engine",
              version="0.1.0")

# Load JSON schemas to memory (simple caching)
SCHEMAS = {
    'run_forecast': load_json_schema('run_forecast'),
    'simulate_forecast': load_json_schema('simulate_forecast'),
    'explain_forecast': load_json_schema('explain_forecast'),
    'compare_forecast': load_json_schema('compare_forecast'),
    'detect_anomalies': load_json_schema('detect_anomalies'),
    'generate_sql': load_json_schema('generate_sql'),
    'request_clarification': load_json_schema('request_clarification'),
}


@app.post('/command', response_model=CommandResponse)
async def post_command(cmd: dict):
    """Primary endpoint: accepts LLM JSON command as raw JSON dict.

    It validates the `action` and payload against the matching JSON schema, then calls the orchestrator.
    """
    if 'action' not in cmd:
        raise HTTPException(status_code=400, detail={"error": "missing 'action' field"})
    action = cmd['action']
    schema = SCHEMAS.get(action)
    if not schema:
        raise HTTPException(status_code=400, detail={"error": f"unsupported action: {action}"})

    # validate payload
    valid, errors = validate_against_schema(cmd, schema)
    if not valid:
        raise HTTPException(status_code=400, detail={"validation_errors": errors})

    # dispatch to orchestrator
    result = await handle_command(cmd)
    return JSONResponse(status_code=200, content=result)


@app.get('/health')
async def health():
    return {"status": "ok"}


if __name__ == '__main__':
    uvicorn.run('app.main:app', host='0.0.0.0', port=8000, reload=True)
```

---

# src/app/schemas.py

```python
from pydantic import BaseModel, Field
from typing import Any, Dict, List, Optional

# Incoming command wrapper (loose) - we accept arbitrary dict but validate server-side
class CommandIn(BaseModel):
    action: str
    payload: Dict[str, Any] = Field(default_factory=dict)


class CommandResponse(BaseModel):
    status: str
    action: str
    result: Optional[Dict[str, Any]] = None
    message: Optional[str] = None
```

---

# src/app/validators.py

```python
import json
from jsonschema import validate, ValidationError
from pathlib import Path
from typing import Tuple, Any, List

SCHEMA_DIR = Path(__file__).parent / 'json_schemas'


def load_json_schema(name: str) -> dict:
    """Load schema by name from json_schemas folder."""
    p = SCHEMA_DIR / f"{name}.json"
    if not p.exists():
        raise FileNotFoundError(f"Schema not found: {p}")
    return json.loads(p.read_text(encoding='utf8'))


def validate_against_schema(instance: dict, schema: dict) -> Tuple[bool, List[str]]:
    errors = []
    try:
        validate(instance=instance, schema=schema)
        return True, []
    except ValidationError as e:
        # produce a friendly error list
        errors.append(str(e.message))
        return False, errors
```

---

# src/app/json_schemas/run_forecast.json

```json
{
  "type": "object",
  "required": ["action", "sku", "store", "horizon"],
  "properties": {
    "action": { "const": "run_forecast" },
    "sku": { "type": "string" },
    "store": { "type": "string" },
    "horizon": { "type": "integer", "minimum": 1 },
    "model": { "type": "string" },
    "frequency": { "type": "string", "enum": ["daily", "weekly", "monthly"] }
  }
}
```

---

# src/app/json_schemas/simulate_forecast.json

```json
{
  "type": "object",
  "required": ["action", "sku", "store", "horizon", "adjustments"],
  "properties": {
    "action": { "const": "simulate_forecast" },
    "sku": { "type": "string" },
    "store": { "type": "string" },
    "horizon": { "type": "integer", "minimum": 1 },
    "adjustments": {
      "type": "object",
      "properties": {
        "price_change_pct": { "type": "number" },
        "promo_flag": { "type": "boolean" },
        "discount_pct": { "type": "number" },
        "event_type": { "type": "string" }
      },
      "additionalProperties": false
    }
  }
}
```

---

# src/app/json_schemas/explain_forecast.json

```json
{
  "type": "object",
  "required": ["action", "forecast_id"],
  "properties": {
    "action": { "const": "explain_forecast" },
    "forecast_id": { "type": "string" }
  }
}
```

---

# src/app/json_schemas/compare_forecast.json

```json
{
  "type": "object",
  "required": ["action", "items"],
  "properties": {
    "action": { "const": "compare_forecast" },
    "items": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["forecast_id"],
        "properties": {
          "forecast_id": { "type": "string" },
          "label": { "type": "string" }
        }
      }
    }
  }
}
```

---

# src/app/json_schemas/detect_anomalies.json

```json
{
  "type": "object",
  "required": ["action", "sku", "store", "start_date", "end_date"],
  "properties": {
    "action": { "const": "detect_anomalies" },
    "sku": { "type": "string" },
    "store": { "type": "string" },
    "start_date": { "type": "string", "format": "date" },
    "end_date": { "type": "string", "format": "date" },
    "threshold": { "type": "number" }
  }
}
```

---

# src/app/json_schemas/generate_sql.json

```json
{
  "type": "object",
  "required": ["action", "intent"],
  "properties": {
    "action": { "const": "generate_sql" },
    "intent": { "type": "string" }
  }
}
```

---

# src/app/json_schemas/request_clarification.json

```json
{
  "type": "object",
  "required": ["action", "missing"],
  "properties": {
    "action": { "const": "request_clarification" },
    "missing": {
      "type": "array",
      "items": { "type": "string" }
    }
  }
}
```

---

# src/app/orchestrator.py

```python
import asyncio
from .forecast_runner import run_forecast_sync, simulate_forecast_sync, explain_forecast_sync
from typing import Dict, Any
from .utils import new_id


async def handle_command(cmd: Dict[str, Any]) -> Dict[str, Any]:
    action = cmd['action']
    # short dispatcher - in production use a more robust router
    if action == 'run_forecast':
        # call adapter (sync) in threadpool
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, run_forecast_sync, cmd)
        return {
            'status': 'ok',
            'action': action,
            'result': result
        }
    if action == 'simulate_forecast':
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, simulate_forecast_sync, cmd)
        return {'status': 'ok', 'action': action, 'result': result}
    if action == 'explain_forecast':
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, explain_forecast_sync, cmd)
        return {'status': 'ok', 'action': action, 'result': result}

    # for unsupported actions, return an error-like payload
    return {'status': 'error', 'action': action, 'message': 'action not implemented'}
```

---

# src/app/forecast_runner.py

```python
"""
Lightweight adapter/stub that simulates calling a forecasting engine.
Replace the HTTP calls with your real model-serving endpoints.
"""
from typing import Dict, Any
from .utils import now_iso


def run_forecast_sync(cmd: Dict[str, Any]) -> Dict[str, Any]:
    # This stub returns a fake forecast array and a generated forecast_id
    sku = cmd['sku']
    store = cmd['store']
    horizon = cmd['horizon']
    forecast_id = f"fc_{sku}_{store}_{now_iso()}"
    # create fake daily predictions
    predictions = [{'day': i + 1, 'value': 100 + i} for i in range(horizon)]
    return {
        'forecast_id': forecast_id,
        'sku': sku,
        'store': store,
        'horizon': horizon,
        'predictions': predictions,
        'meta': {'model': cmd.get('model', 'short_term_v1')}
    }


def simulate_forecast_sync(cmd: Dict[str, Any]) -> Dict[str, Any]:
    # Apply adjustments to the fake forecast
    base = run_forecast_sync(cmd)
    adj = cmd.get('adjustments', {})
    # naive simulation: price drop increases demand linearly
    price_pct = adj.get('price_change_pct', 0)
    multiplier = 1.0 + ( -price_pct / 100.0 ) * 0.2  # naive elasticity
    for p in base['predictions']:
        p['value'] = round(p['value'] * multiplier, 2)
    base['simulation_adjustments'] = adj
    return base


def explain_forecast_sync(cmd: Dict[str, Any]) -> Dict[str, Any]:
    # Return a simple explanation object. In production attach SHAP or feature importances.
    forecast_id = cmd.get('forecast_id')
    return {
        'forecast_id': forecast_id,
        'explanation': [
            {'reason': 'recent price increase', 'impact': -0.12},
            {'reason': 'no active promotion this month', 'impact': -0.08},
            {'reason': 'seasonal decline (post-peak)', 'impact': -0.05}
        ]
    }
```

---

# src/app/utils.py

```python
from datetime import datetime


def now_iso() -> str:
    return datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')


def new_id(prefix: str = 'id') -> str:
    return f"{prefix}_{now_iso()}"
```

---

# tests/test_api.py

```python
from fastapi.testclient import TestClient
from app.main import app

client = TestClient(app)


def test_health():
    r = client.get('/health')
    assert r.status_code == 200
    assert r.json()['status'] == 'ok'


def test_run_forecast_missing_fields():
    # missing sku/store/horizon
    r = client.post('/command', json={'action': 'run_forecast'})
    assert r.status_code == 400


def test_run_forecast_full():
    payload = {"action": "run_forecast", "sku": "SKU1", "store": "S1", "horizon": 7}
    r = client.post('/command', json=payload)
    assert r.status_code == 200
    data = r.json()
    assert data['action'] == 'run_forecast'
    assert data['status'] == 'ok'
```

---

# README.md (short)

```
# Retail Forecast Agent - FastAPI scaffold

## Quickstart

1. Create a venv and install requirements:

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Run the server locally:

   ```bash
   uvicorn app.main:app --reload --port 8000
   ```

3. Health check:

   ```bash
   curl http://localhost:8000/health
   ```

4. Example run_forecast request:

   ```bash
   curl -X POST http://localhost:8000/command -H "Content-Type: application/json" \
     -d '{"action":"run_forecast","sku":"SKU123","store":"STORE1","horizon":14}'
   ```

## Notes
- JSON schemas live in `src/app/json_schemas/` and are used for validation.
- Replace `forecast_runner.py` stubs with real HTTP calls to your model server or in-process model.
- Add authentication / RBAC around endpoints before enabling in production.
```

---

# Delivery notes

- This scaffold is intentionally minimal but complete and runnable.
- Replace stubs with model endpoints and add logging, metrics, and auth.
- If you want, I can now:
  - Convert forecast_runner stubs to real HTTP calls to an example TorchServe/Triton endpoint,
  - Add OpenAI / other LLM client example for generating commands,
  - Add CI config (GitHub Actions) and Makefile.


# End of scaffold
