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

---

# Added: /chat FastAPI endpoint (LLM -> command -> execute)

Add the following to `src/app/main.py` or a new module `src/app/chat.py` and import/register router in main.

```python
# src/app/chat.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from .llm_client import llm_to_command
from .validators import load_json_schema, validate_against_schema
from .orchestrator import handle_command

router = APIRouter()

# Load schemas used to validate LLM output
RUN_SCHEMA = load_json_schema('run_forecast')
SIM_SCHEMA = load_json_schema('simulate_forecast')
EXPLAIN_SCHEMA = load_json_schema('explain_forecast')
CLARIFY_SCHEMA = load_json_schema('request_clarification')

class ChatIn(BaseModel):
    user_text: str

@router.post('/chat')
async def chat_endpoint(payload: ChatIn):
    # 1. Ask LLM to convert to JSON command
    try:
        llm_resp = await llm_to_command(payload.user_text)
    except Exception as e:
        raise HTTPException(status_code=502, detail={"error": "LLM call failed", "reason": str(e)})

    # 2. Parse LLM output (we expect a JSON string/object)
    if isinstance(llm_resp, str):
        try:
            cmd = json.loads(llm_resp)
        except Exception:
            raise HTTPException(status_code=400, detail={"error": "LLM returned non-JSON"})
    elif isinstance(llm_resp, dict):
        cmd = llm_resp
    else:
        raise HTTPException(status_code=400, detail={"error": "Unexpected LLM response type"})

    # 3. Validate against schema for safety
    action = cmd.get('action')
    if not action:
        raise HTTPException(status_code=400, detail={"error": "missing 'action' in LLM output"})

    schema_map = {
        'run_forecast': RUN_SCHEMA,
        'simulate_forecast': SIM_SCHEMA,
        'explain_forecast': EXPLAIN_SCHEMA,
        'request_clarification': CLARIFY_SCHEMA
    }
    schema = schema_map.get(action)
    if not schema:
        raise HTTPException(status_code=400, detail={"error": f"unsupported action from LLM: {action}"})

    valid, errors = validate_against_schema(cmd, schema)
    if not valid:
        raise HTTPException(status_code=400, detail={"error": "LLM JSON failed schema validation", "errors": errors})

    # 4. Execute command using orchestrator
    result = await handle_command(cmd)

    # 5. Return both the command and result for transparency
    return {"command": cmd, "result": result}
```

Remember to register router in `main.py`:

```python
from .chat import router as chat_router
app.include_router(chat_router)
```

---

# Added: End-to-end workflow diagram (Mermaid)

Insert this into your README or docs to render a visual workflow.

```mermaid
flowchart LR
  U[User / Analyst]
  LLM[LLM (Orchestration Prompt)]
  API[FastAPI Orchestrator (/chat & /command)]
  VALID[JSON Schema Validation]
  ORCH[Orchestrator Router]
  MODEL[Model Server (Triton / TorchServe)]
  DB[Feature Store / Timeseries DB]
  UI[Dashboard (Vue)]
  OUT[Forecast Output / Explanation]

  U -->|Natural language| LLM
  LLM -->|JSON command| API
  API --> VALID
  VALID --> ORCH
  ORCH --> MODEL
  MODEL --> DB
  MODEL --> OUT
  ORCH --> UI
  U --> UI
  UI -->|action| API
  OUT --> UI
```

---

# Added: Vue 3 Single File Component dashboard (basic)

Create `src/frontend/src/components/ForecastDashboard.vue` (Vue 3 + Composition API). This is a minimal interactive dashboard to request forecasts and display results.

```vue
<template>
  <div class="p-4 max-w-4xl mx-auto">
    <h1 class="text-2xl font-semibold mb-4">Retail Forecast Dashboard</h1>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-4 mb-4">
      <input v-model="sku" placeholder="SKU" class="input" />
      <input v-model="store" placeholder="Store" class="input" />
      <input v-model.number="horizon" type="number" placeholder="Horizon (days)" class="input" />
      <select v-model="model" class="input">
        <option value="short_term_v1">short_term_v1</option>
        <option value="tft_v1">tft_v1</option>
      </select>
    </div>

    <div class="flex gap-2 mb-4">
      <button @click="runForecast" class="btn">Run Forecast</button>
      <button @click="simulatePriceDrop" class="btn-ghost">Simulate -10% Price</button>
    </div>

    <div v-if="loading">Loading…</div>

    <div v-if="result" class="mt-4">
      <h2 class="text-xl font-medium">Forecast (ID: {{ result.result.result.forecast_id }})</h2>
      <ul class="mt-2">
        <li v-for="p in result.result.result.predictions" :key="p.day">
          Day {{ p.day }}: {{ p.value }}
        </li>
      </ul>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'

const sku = ref('SKU123')
const store = ref('STORE1')
const horizon = ref(14)
const model = ref('short_term_v1')
const loading = ref(false)
const result = ref(null)

async function runForecast() {
  loading.value = true
  result.value = null
  const userText = `Give me a ${horizon.value}-day forecast for ${sku.value} at ${store.value} using model ${model.value}`
  try {
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_text: userText })
    })
    result.value = await res.json()
  } catch (e) {
    alert('Error: ' + e.message)
  } finally {
    loading.value = false
  }
}

async function simulatePriceDrop() {
  loading.value = true
  result.value = null
  const userText = `Simulate a 10% price drop for ${sku.value} at ${store.value} for next ${horizon.value} days.`
  try {
    const res = await fetch('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ user_text: userText })
    })
    result.value = await res.json()
  } catch (e) {
    alert('Error: ' + e.message)
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.input { padding: 0.5rem; border: 1px solid #ddd; border-radius: 6px; }
.btn { background: #2563eb; color: white; padding: 0.5rem 1rem; border-radius: 6px; }
.btn-ghost { background: transparent; color: #333; padding: 0.5rem 1rem; border-radius: 6px; border: 1px solid #ddd; }
</style>
```

Notes:
- This component expects your FastAPI to serve under `/api` or configure a reverse proxy. In dev you can use Vite with proxy to `http://localhost:8000`.
- For production, add authentication and CSRF protections.

---

# Next steps I can do for you (pick any):
- Wire the Vue app into a Vite project with proxy config and Dockerfile.
- Add Chart.js rendering for forecast time series.
- Add auth (JWT) to FastAPI and Vue.
- Provide a docker-compose that brings up Triton + API + Vite frontend.



# --- Added: Model Server Integration (Triton / TorchServe) ---

# Example Triton inference client inside forecast_runner.py
# Replace the existing stub functions with this implementation.

import httpx
import json

TRITON_SERVER_URL = "http://localhost:8001/v2/models/demand_forecast/infer"

async def call_triton(payload: dict):
    async with httpx.AsyncClient(timeout=30) as client:
        response = await client.post(TRITON_SERVER_URL, json=payload)
        response.raise_for_status()
        return response.json()

async def run_forecast(sku: str, store: str, horizon: int, model: str = None, frequency: str = None):
    payload = {
        "inputs": [
            {"name": "sku", "shape": [1], "datatype": "BYTES", "data": [sku]},
            {"name": "store", "shape": [1], "datatype": "BYTES", "data": [store]},
            {"name": "horizon", "shape": [1], "datatype": "INT32", "data": [horizon]}
        ]
    }
    output = await call_triton(payload)
    return output


# --- Added: OpenAI LLM Orchestrator Client ---

# In orchestrator_client.py (new file)

from openai import AsyncOpenAI
import os

client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))

SYSTEM_PROMPT = "You are a Demand Forecasting Orchestration Agent... (same as earlier)"

async def llm_to_command(user_text: str):
    response = await client.chat.completions.create(
        model="gpt-5.1",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_text}
        ],
        response_format={"type": "json_object"}
    )
    return response.choices[0].message.parsed
