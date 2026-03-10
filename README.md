# AI Business Analyser — Backend

Conversational AI for Instant Business Intelligence Dashboards.

---

## Setup & Run Instructions

### 1. Prerequisites

- Python 3.11 or higher
- A Google Gemini API key ([get one here](https://aistudio.google.com/app/apikey))

---

### 2. Clone and enter the project

```bash
cd "AI Business analyser"
```

---

### 3. Create a virtual environment

```bash
python -m venv venv
```

Activate it:

- **Windows:**
  ```bash
  venv\Scripts\activate
  ```
- **macOS / Linux:**
  ```bash
  source venv/bin/activate
  ```

---

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

---

### 5. Configure environment variables

Copy the example file and fill in your API key:

```bash
copy .env.example .env
```

Edit `.env`:

```
GEMINI_API_KEY=your_actual_gemini_api_key
GEMINI_MODEL=gemini-1.5-flash
DATABASE_PATH=data/database.db
MAX_CSV_SIZE_MB=50
```

---

### 6. Create the data directory

```bash
mkdir data
```

---

### 7. Start the FastAPI backend

```bash
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: `http://localhost:8000`

Interactive API docs: `http://localhost:8000/docs`

---

### 8. Start the Streamlit frontend

Open a **second terminal** (keep the backend running):

```bash
streamlit run frontend/app.py
```

The UI will open at: `http://localhost:8501`

---

## API Endpoints

| Method | Endpoint              | Description                                      |
|--------|-----------------------|--------------------------------------------------|
| GET    | `/health`             | Liveness probe                                   |
| POST   | `/upload-csv`         | Upload a CSV file (replaces active dataset)      |
| POST   | `/generate-dashboard` | Ask a NL question → chart + insights             |
| POST   | `/follow-up`          | Follow-up NL question (same pipeline)            |
| GET    | `/schema`             | Get the current dataset schema                   |

---

## Project Structure

```
AI Business analyser/
├── backend/
│   ├── main.py                # FastAPI app, all endpoints
│   ├── config.py              # Environment config
│   ├── database.py            # SQLite lifecycle and queries
│   ├── csv_loader.py          # CSV ingestion and schema extraction
│   ├── llm_service.py         # Gemini API wrapper + prompt templates
│   ├── query_executor.py      # SQL validation and safe execution
│   ├── chart_selector.py      # Plotly chart builder
│   └── langgraph_pipeline.py  # LangGraph workflow (7 nodes)
├── data/
│   └── database.db            # SQLite file (auto-created, gitignore this)
├── .env                       # Your secrets (never commit)
├── .env.example               # Template
├── requirements.txt
└── README.md
```

---

## LangGraph Pipeline

```
START
  └─► schema_analyzer
        └─► sql_generator
              └─► sql_validator
                    ├─► [error] error_node ──► END
                    └─► query_executor
                          ├─► [error]        error_node       ──► END
                          ├─► [empty result] empty_result_node──► END
                          └─► chart_selector
                                └─► insight_generator ──► END
```

---

## Security Notes

- Only `SELECT` statements are permitted — DDL/DML is blocked at the validator layer.
- SQL keywords like `DROP`, `DELETE`, `INSERT`, `UPDATE`, `ALTER` are rejected.
- Comment injection (`--`, `/* */`) is stripped before validation.
- All column references are checked against the active schema before execution.
- The Gemini API is called with `temperature=0.2` and JSON-only output to minimise hallucinations.
- File size is capped at `MAX_CSV_SIZE_MB` (default 50 MB).
