import psycopg2
import json
import webbrowser
import os
import re
from pathlib import Path
from datetime import datetime

# Database config
DB_CONFIG = {
    "dbname": "olist_db",
    "user": "postgres",
    "password": "olist",
    "host": "localhost",
    "port": "5432"
}

def get_schema_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Get all tables/views and their columns across our schemas
    cur.execute("""
        SELECT 
            table_schema, 
            table_name, 
            column_name, 
            data_type
        FROM information_schema.columns 
        WHERE table_schema IN ('bronze', 'silver', 'gold')
        ORDER BY table_schema, table_name, ordinal_position;
    """)
    columns_data = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return columns_data

def get_edges_from_sql():
    edges_data = []
    sql_text = ""
    
    if Path("build_layers.sql").exists():
        sql_text = Path("build_layers.sql").read_text()
    elif Path("init.sql").exists():
        sql_text = Path("init.sql").read_text()
        
    # Split the SQL by view creation to analyze each block
    view_blocks = re.split(r'CREATE (?:OR REPLACE )?VIEW ', sql_text, flags=re.IGNORECASE)[1:]
    
    for block in view_blocks:
        # Extract target schema and table
        match_target = re.match(r'\s*([a-zA-Z0-9_]+)\.([a-zA-Z0-9_]+)', block)
        if match_target:
            target_schema, target_table = match_target.groups()
            target_full = f"{target_schema}.{target_table}"
            
            # Find ALL schema.table references within the view definition
            matches = re.findall(r'\b(bronze|silver|gold)\.([a-zA-Z0-9_]+)\b', block)
            for src_schema, src_table in set(matches):
                if f"{src_schema}.{src_table}" != target_full:
                    edges_data.append((src_schema, src_table, target_schema, target_table))
                
    return edges_data

def get_drift_status():
    drifted_tables = []
    try:
        if Path("schema_registry.json").exists():
            with open("schema_registry.json", "r") as f:
                registry = json.load(f)
                for table, meta in registry.items():
                    if meta.get("version", 1) > 1:
                        drifted_tables.append(f"bronze.{table}")
    except Exception:
        pass
    return drifted_tables

def compute_downstream_drift(drifted_tables, edges_data):
    adj = {}
    for src_schema, src_table, tgt_schema, tgt_table in edges_data:
        src = f"{src_schema}.{src_table}"
        tgt = f"{tgt_schema}.{tgt_table}"
        if src not in adj:
            adj[src] = []
        adj[src].append(tgt)
        
    affected_nodes = set()
    affected_edges = set()
    
    queue = list(drifted_tables)
    while queue:
        curr = queue.pop(0)
        if curr in adj:
            for nxt in adj[curr]:
                affected_edges.add((curr, nxt))
                if nxt not in affected_nodes and nxt not in drifted_tables:
                    affected_nodes.add(nxt)
                    queue.append(nxt)
                    
    return affected_nodes, affected_edges

def get_last_drift_date():
    try:
        if Path("schema_registry.json").exists():
            # Use file modification time as proxy for last change
            mtime = os.path.getmtime("schema_registry.json")
            return datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass
    return datetime.now().strftime("%Y-%m-%d %H:%M")

def build_html(columns_data, edges_data, last_change, drifted_tables, affected_nodes, affected_edges):
    # Process Postgres nodes
    tables = {}
    total_columns = 0
    for schema, table, col, dtype in columns_data:
        full_name = f"{schema}.{table}"
        if full_name not in tables:
            tables[full_name] = {"schema": schema, "name": table, "columns": []}
        
        dtype = dtype.replace('character varying', 'varchar').replace('timestamp without time zone', 'timestamp')
        tables[full_name]["columns"].append({"name": col, "type": dtype})
        total_columns += 1

    nodes = []
    
    layer_colors = {
        "raw":    {"border": "#4a4a4a", "background": "#e8e8e8"},
        "bronze": {"border": "#cd7f32", "background": "#ffe6cc"},
        "silver": {"border": "#c0c0c0", "background": "#f0f0f0"},
        "gold":   {"border": "#ffd700", "background": "#fffacd"}
    }
    
    layer_levels = {
        "raw": 0,
        "bronze": 1,
        "silver": 2,
        "gold": 3
    }

    # 1. Add Raw CSV Nodes
    raw_files = [
        "olist_orders_dataset.csv",
        "olist_customers_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv"
    ]
    
    for filename in raw_files:
        color = layer_colors["raw"]
        label = f"RAW DATA\n{filename}"
        nodes.append({
            "id": filename,
            "label": label,
            "collapsed_label": label,
            "expanded_label": label,
            "shape": "box",
            "color": color,
            "level": layer_levels["raw"],
            "font": {"face": "monospace", "align": "center"}
        })

    # 2. Add Postgres Nodes
    for full_name, data in tables.items():
        col_text = "\n".join([f"  {c['name'].ljust(25)} {c['type']}" for c in data["columns"]])
        
        color = layer_colors.get(data["schema"], {"border": "#000", "background": "#fff"}).copy()
        
        if full_name in drifted_tables:
            drift_header = "🛑 DRIFT DETECTED\n"
            color["border"] = "#cc0000"
            color["highlight"] = {"border": "#cc0000"}
            font_color = "#cc0000"
        elif full_name in affected_nodes:
            drift_header = "⚠️ AFFECTED BY DRIFT\n"
            color["border"] = "#cc6600"
            color["highlight"] = {"border": "#cc6600"}
            font_color = "#cc6600"
        else:
            drift_header = ""
            font_color = "black"

        collapsed_label = f"{drift_header}{full_name}\n{len(data['columns'])} columns\nLast change: {last_change}\n▼ click to expand"
        expanded_label = f"{drift_header}{full_name}\n{len(data['columns'])} columns\nLast change: {last_change}\n▲ click to collapse\n{col_text}"
        
        level = layer_levels.get(data["schema"], 4)
        
        nodes.append({
            "id": full_name,
            "label": collapsed_label,
            "collapsed_label": collapsed_label,
            "expanded_label": expanded_label,
            "shape": "box",
            "color": color,
            "level": level,
            "font": {"face": "monospace", "align": "left", "color": font_color}
        })

    # 3. Add Edges
    edges = []
    
    # Raw -> Bronze edges
    raw_to_bronze_map = {
        "olist_orders_dataset.csv": "bronze.orders",
        "olist_customers_dataset.csv": "bronze.customers",
        "olist_order_items_dataset.csv": "bronze.items",
        "olist_order_payments_dataset.csv": "bronze.payments"
    }
    for raw_csv, bronze_table in raw_to_bronze_map.items():
        edge_color = {"color": "red", "opacity": 0.8} if bronze_table in drifted_tables else {"color": "#999", "opacity": 0.6}
        edges.append({
            "from": raw_csv,
            "to": bronze_table,
            "arrows": "to",
            "color": edge_color,
            "dashes": True,
            "width": 2 if bronze_table in drifted_tables else 1
        })
        
    # Postgres view edges
    for src_schema, src_table, tgt_schema, tgt_table in edges_data:
        src = f"{src_schema}.{src_table}"
        tgt = f"{tgt_schema}.{tgt_table}"
        
        if (src, tgt) in affected_edges:
            edge_color = {"color": "red"}
            edge_width = 3
        else:
            edge_color = {"color": "#666"}
            edge_width = 2
            
        edges.append({
            "from": src,
            "to": tgt,
            "arrows": "to",
            "color": edge_color,
            "width": edge_width
        })

    html_content = f"""<!DOCTYPE html>
<html>
<head>
    <title>Living Data Map</title>
    <script type="text/javascript" src="https://unpkg.com/vis-network/standalone/umd/vis-network.min.js"></script>
    <style type="text/css">
        body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 0; background: #fafafa; }}
        #mynetwork {{ width: 100vw; height: 100vh; border: none; }}
        #summary {{
            position: absolute;
            top: 20px;
            right: 20px;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            border: 1px solid #ddd;
            z-index: 1000;
        }}
        h3 {{ margin-top: 0; color: #333; }}
        p {{ margin: 8px 0; color: #555; font-size: 14px; }}
        strong {{ color: #111; }}
    </style>
</head>
<body>
    <div id="summary">
        <h3>Dark Data Dashboard</h3>
        <p><strong>Tables:</strong> {len(tables)}</p>
        <p><strong>Total Columns:</strong> {total_columns}</p>
        <p><strong>Last Update:</strong> {last_change}</p>
    </div>
    <div id="mynetwork"></div>

    <script type="text/javascript">
        var nodes = new vis.DataSet({json.dumps(nodes)});
        var edges = new vis.DataSet({json.dumps(edges)});

        var container = document.getElementById('mynetwork');
        var data = {{
            nodes: nodes,
            edges: edges
        }};
        var options = {{
            nodes: {{
                margin: 15,
                borderWidth: 2,
                shadow: true
            }},
            edges: {{
                smooth: {{ type: 'cubicBezier', forceDirection: 'vertical', roundness: 0.4 }},
                width: 2
            }},
            layout: {{
                hierarchical: {{
                    direction: 'UD',
                    sortMethod: 'directed',
                    nodeSpacing: 350,
                    levelSeparation: 250
                }}
            }},
            physics: false
        }};
        var network = new vis.Network(container, data, options);

        // Toggle expand/collapse on click
        network.on("click", function(params) {{
            if (params.nodes.length > 0) {{
                var nodeId = params.nodes[0];
                var node = nodes.get(nodeId);
                
                if (node.label === node.collapsed_label) {{
                    nodes.update({{id: nodeId, label: node.expanded_label}});
                }} else {{
                    nodes.update({{id: nodeId, label: node.collapsed_label}});
                }}
            }}
        }});
    </script>
</body>
</html>"""
    
    Path("docs").mkdir(parents=True, exist_ok=True)
    out_path = Path("docs/schema.html")
    with open(out_path, "w") as f:
        f.write(html_content)
    
    print(f"Generated {out_path}")
    
    # Automatically open in the default browser
    filepath = os.path.abspath(out_path)
    print(f"Opening file://{filepath} in your browser...")
    webbrowser.open(f"file://{filepath}")

if __name__ == "__main__":
    columns_data = get_schema_data()
    edges_data = get_edges_from_sql()
    last_change = get_last_drift_date()
    drifted_tables = get_drift_status()
    affected_nodes, affected_edges = compute_downstream_drift(drifted_tables, edges_data)
    build_html(columns_data, edges_data, last_change, drifted_tables, affected_nodes, affected_edges)
