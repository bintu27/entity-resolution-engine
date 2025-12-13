import json
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from sqlalchemy import text

from entity_resolution_engine.cli.run_mapping import main as run_mapping
from entity_resolution_engine.db.connections import get_engine

app = FastAPI(title="Unified Entity Store API")
alpha_engine = get_engine("SOURCE_ALPHA_DB_URL", "postgresql://postgres:pass@localhost:5433/source_alpha_db")
beta_engine = get_engine("SOURCE_BETA_DB_URL", "postgresql://postgres:pass@localhost:5434/source_beta_db")
ues_engine = get_engine("UES_DB_URL", "postgresql://postgres:pass@localhost:5435/ues_db")

DB_REGISTRY: Dict[str, Dict[str, Any]] = {
    "alpha": {"label": "SourceAlpha", "engine": alpha_engine},
    "beta": {"label": "SourceBeta", "engine": beta_engine},
    "ues": {"label": "UnifiedEntityStore", "engine": ues_engine},
}

API_ENDPOINTS = [
    {"method": "GET", "path": "/health", "description": "Simple API health check"},
    {"method": "POST", "path": "/mapping/run", "description": "Trigger the entity mapping pipeline"},
    {"method": "GET", "path": "/ues/player/{ues_id}", "description": "Fetch a unified player record by UES ID"},
    {"method": "GET", "path": "/lookup/player/by-alpha/{alpha_id}", "description": "Lookup a player via SourceAlpha ID"},
    {"method": "GET", "path": "/lookup/player/by-beta/{beta_id}", "description": "Lookup a player via SourceBeta ID"},
    {"method": "GET", "path": "/ues/player/{ues_id}/lineage", "description": "Retrieve lineage for a UES player"},
    {"method": "GET", "path": "/db/{db}/tables", "description": "List tables + columns for a database"},
    {"method": "GET", "path": "/db/{db}/table/{table}", "description": "Preview rows from a database table"},
]


def _get_db_entry(db_key: str) -> Dict[str, Any]:
    entry = DB_REGISTRY.get(db_key.lower())
    if not entry:
        raise HTTPException(status_code=404, detail=f"Unknown database '{db_key}'")
    return entry


def _list_tables(engine) -> Dict[str, List[Dict[str, str]]]:
    query = text(
        """
        SELECT
            c.table_name,
            c.column_name,
            c.data_type,
            c.ordinal_position
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON c.table_name = t.table_name
         AND c.table_schema = t.table_schema
        WHERE c.table_schema = 'public'
          AND t.table_type = 'BASE TABLE'
        ORDER BY c.table_name, c.ordinal_position
        """
    )
    tables: Dict[str, List[Dict[str, str]]] = {}
    with engine.connect() as conn:
        results = conn.execute(query).mappings()
        for row in results:
            tables.setdefault(row["table_name"], []).append(
                {"name": row["column_name"], "type": row["data_type"]}
            )
    return tables


def _fetch_table_rows(engine, table_name: str, limit: int) -> Dict[str, Any]:
    if '"' in table_name or ";" in table_name:
        raise HTTPException(status_code=400, detail="Invalid table name")
    tables = _list_tables(engine)
    if table_name not in tables:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
    query = text(f'SELECT * FROM "{table_name}" LIMIT :limit')
    with engine.connect() as conn:
        result = conn.execute(query, {"limit": limit})
        rows = [dict(row._mapping) for row in result]
        columns = list(result.keys())
    return {"table": table_name, "columns": columns, "rows": rows}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    db_cards = [{"key": key, "label": entry["label"]} for key, entry in DB_REGISTRY.items()]
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="utf-8" />
        <title>Entity Resolution Dashboard</title>
        <style>
            body {{
                font-family: Arial, sans-serif;
                margin: 0;
                padding: 0;
                background: #f4f5fb;
                color: #1f2430;
            }}
            header {{
                background: #1b2559;
                color: #fff;
                padding: 24px;
            }}
            main {{
                padding: 24px;
            }}
            h1 {{
                margin: 0 0 8px;
            }}
            .panel {{
                background: #fff;
                border-radius: 8px;
                padding: 16px;
                margin-bottom: 24px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.08);
            }}
            table {{
                border-collapse: collapse;
                width: 100%;
            }}
            th, td {{
                border-bottom: 1px solid #e5e7ef;
                text-align: left;
                padding: 8px;
                font-size: 14px;
            }}
            th {{
                background: #f2f4ff;
                font-weight: bold;
            }}
            .tag {{
                display: inline-block;
                padding: 2px 8px;
                border-radius: 12px;
                font-size: 12px;
                font-weight: bold;
            }}
            .tag.GET {{ background: #e0f2ff; color: #056097; }}
            .tag.POST {{ background: #ffe3d3; color: #a23d00; }}
            .db-grid {{
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
                gap: 16px;
            }}
            .table-list button {{
                width: 100%;
                text-align: left;
                padding: 6px 8px;
                border: none;
                background: transparent;
                border-bottom: 1px solid #eee;
                cursor: pointer;
            }}
            .table-list button:hover {{
                background: #f7f9ff;
            }}
            pre {{
                background: #10172a;
                color: #f8fafc;
                padding: 12px;
                border-radius: 6px;
                overflow: auto;
            }}
            .data-view {{
                max-height: 260px;
                overflow: auto;
                border: 1px solid #e5e7ef;
                border-radius: 6px;
                margin-top: 8px;
            }}
            .columns {{
                font-size: 12px;
                color: #4b5563;
                margin-bottom: 8px;
            }}
        </style>
    </head>
    <body>
        <header>
            <h1>Entity Resolution Dashboard</h1>
            <p>Explore endpoints and peek into SourceAlpha, SourceBeta, and UnifiedEntityStore data.</p>
        </header>
        <main>
            <section class="panel">
                <h2>API Endpoints</h2>
                <table>
                    <thead>
                        <tr><th>Method</th><th>Path</th><th>Description</th></tr>
                    </thead>
                    <tbody>
                        {''.join(f"<tr><td><span class='tag {ep['method']}'>{ep['method']}</span></td><td><code>{ep['path']}</code></td><td>{ep['description']}</td></tr>" for ep in API_ENDPOINTS)}
                    </tbody>
                </table>
                <p>Need interactive docs? Visit <a href="/docs">/docs</a> or <a href="/redoc">/redoc</a>.</p>
            </section>
            <section class="panel">
                <h2>Database Explorer</h2>
                <div class="db-grid">
                    {''.join(f"<div class='db-card'><h3>{db['label']}</h3><div class='table-list' id='tables-{db['key']}'></div><div class='data-view' id='data-{db['key']}'><em>Select a table to preview rows.</em></div></div>" for db in db_cards)}
                </div>
            </section>
        </main>
        <script>
            const databases = {json.dumps(db_cards)};

            function renderTableList(dbKey, tables) {{
                const container = document.getElementById(`tables-${{dbKey}}`);
                if (!container) return;
                container.innerHTML = '';
                tables.forEach(({{
                    name,
                    columns
                }}) => {{
                    const btn = document.createElement('button');
                    btn.textContent = name + ' (' + columns.length + ' cols)';
                    btn.onclick = () => loadTable(dbKey, name);
                    container.appendChild(btn);
                }});
            }}

            function loadTable(dbKey, tableName) {{
                const viewer = document.getElementById(`data-${{dbKey}}`);
                viewer.innerHTML = '<em>Loading...</em>';
                fetch(`/db/${{dbKey}}/table/${{tableName}}?limit=10`)
                    .then(resp => resp.json())
                    .then(data => {{
                        const columns = data.columns || [];
                        const rows = data.rows || [];
                        let html = `<div class="columns"><strong>${{tableName}}</strong> — columns: ${{columns.join(', ')}}</div>`;
                        if (!rows.length) {{
                            html += '<em>No rows found.</em>';
                        }} else {{
                            html += '<table><thead><tr>' + columns.map(col => `<th>${{col}}</th>`).join('') + '</tr></thead>';
                            html += '<tbody>';
                            rows.forEach(row => {{
                                html += '<tr>' + columns.map(col => `<td>${{row[col] ?? ''}}</td>`).join('') + '</tr>';
                            }});
                            html += '</tbody></table>';
                        }}
                        viewer.innerHTML = html;
                    }})
                    .catch(err => {{
                        viewer.innerHTML = `<span style="color:red">Failed to load table: ${{err}}</span>`;
                    }});
            }}

            function init() {{
                databases.forEach(db => {{
                    fetch(`/db/${{db.key}}/tables`)
                        .then(resp => resp.json())
                        .then(data => {{
                            renderTableList(db.key, data.tables || []);
                        }})
                        .catch(() => {{
                            const container = document.getElementById(`tables-${{db.key}}`);
                            if (container) {{
                                container.innerHTML = '<span style="color:red">Unable to load tables.</span>';
                            }}
                        }});
                }});
            }}
            init();
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/mapping/run")
def trigger_mapping():
    run_mapping()
    return {"status": "mapping_complete"}


def _fetch_player(where_clause: str, params: Dict[str, Any]):
    query = text(f"SELECT * FROM ues_players WHERE {where_clause} LIMIT 1")
    with ues_engine.connect() as conn:
        result = conn.execute(query, params).mappings().first()
        if not result:
            return None
        return dict(result)


@app.get("/ues/player/{ues_id}")
def get_player(ues_id: str):
    player = _fetch_player("ues_player_id = :pid", {"pid": ues_id})
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    return player


@app.get("/lookup/player/by-alpha/{alpha_id}")
def lookup_by_alpha(alpha_id: str):
    query = text(
        "SELECT ues_entity_id FROM source_lineage WHERE source_system='ALPHA' AND source_id=:sid AND ues_entity_type='player'"
    )
    with ues_engine.connect() as conn:
        result = conn.execute(query, {"sid": alpha_id}).scalar()
    if not result:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return get_player(result)


@app.get("/lookup/player/by-beta/{beta_id}")
def lookup_by_beta(beta_id: str):
    query = text(
        "SELECT ues_entity_id FROM source_lineage WHERE source_system='BETA' AND source_id=:sid AND ues_entity_type='player'"
    )
    with ues_engine.connect() as conn:
        result = conn.execute(query, {"sid": beta_id}).scalar()
    if not result:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return get_player(result)


@app.get("/ues/player/{ues_id}/lineage")
def get_player_lineage(ues_id: str):
    player = _fetch_player("ues_player_id = :pid", {"pid": ues_id})
    if not player:
        raise HTTPException(status_code=404, detail="Player not found")
    lineage = player.get("lineage")
    if isinstance(lineage, str):
        try:
            lineage = json.loads(lineage)
        except json.JSONDecodeError:
            pass
    return {"lineage": lineage}


@app.get("/db/{db_key}/tables")
def list_tables(db_key: str):
    entry = _get_db_entry(db_key)
    tables = _list_tables(entry["engine"])
    return {
        "database": entry["label"],
        "tables": [{"name": name, "columns": cols} for name, cols in tables.items()],
    }


@app.get("/db/{db_key}/table/{table_name}")
def preview_table(db_key: str, table_name: str, limit: int = Query(10, ge=1, le=100)):
    entry = _get_db_entry(db_key)
    data = _fetch_table_rows(entry["engine"], table_name, limit)
    return data
