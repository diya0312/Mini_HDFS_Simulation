import threading, time, json, os
from flask import Flask, request, jsonify, render_template_string
import requests

app = Flask(__name__)
LOCK = threading.RLock()
METADATA_FILE = "metadata.json"
HEARTBEAT_TIMEOUT = 12
REPLICA_FACTOR = 2
CHECKSUMS = {}


state = {"files": {}, "datanodes": {}}

# --------------------------- Metadata Helpers ---------------------------
def save_metadata():
    with LOCK:
        with open(METADATA_FILE, "w") as f:
            json.dump(state, f, indent=2)

def load_metadata():
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE) as f:
            state.update(json.load(f))
    return state

# --------------------------- Priority Sorting ---------------------------
def sort_datanodes_by_priority(alive_dns, client_ip=None):
    """
    Sort datanodes by simulated network proximity to the client.
    If client_ip is known, prioritize nodes with similar IP prefix.
    Otherwise, just return in deterministic order.
    """
    if not client_ip:
        return sorted(alive_dns)

    def score(dn):
        try:
            host = state["datanodes"][dn]["host"]
            dn_ip = host.split("//")[-1].split(":")[0]
            client_prefix = client_ip.split(".")[:2]
            dn_prefix = dn_ip.split(".")[:2]
            score_val = sum(1 for a, b in zip(client_prefix, dn_prefix) if a == b)
            return -score_val
        except Exception:
            return 0

    return sorted(alive_dns, key=score)

# --------------------------- Heartbeats ---------------------------
@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    payload = request.json
    dn_id = payload.get("dn_id")
    host = payload.get("host")
    ts = time.time()
    with LOCK:
        state["datanodes"].setdefault(dn_id, {})
        state["datanodes"][dn_id].update({"host": host, "last_seen": ts, "alive": True})
        save_metadata()
    return jsonify({"status": "ok"})

# --------------------------- Upload Metadata ---------------------------
@app.route("/upload_metadata", methods=["POST"])
def upload_metadata():
    body = request.json
    filename = body["filename"]
    num_chunks = int(body["num_chunks"])
    client_checksums = body.get("checksums", {})

    with LOCK:
        alive_dns = [dn for dn, info in state["datanodes"].items() if info.get("alive")]

    if not alive_dns:
        return jsonify({"error": "no_datanodes_available"}), 503

    client_ip = request.remote_addr
    prioritized_dns = sort_datanodes_by_priority(alive_dns, client_ip)

    chunks, chunks_info = [], {}
    for i in range(num_chunks):
        chunk_id = f"{filename}.chunk.{i}"
        selected = []
        for r in range(REPLICA_FACTOR):
            selected.append(prioritized_dns[(i + r) % len(prioritized_dns)])
        chunks.append(chunk_id)
        chunks_info[chunk_id] = selected
        if chunk_id in client_checksums:
            CHECKSUMS[chunk_id] = client_checksums[chunk_id]

    with LOCK:
        state["files"][filename] = {"chunks": chunks, "chunks_info": chunks_info}
        save_metadata()

    result = []
    with LOCK:
        for c in chunks:
            dns = state["files"][filename]["chunks_info"][c]
            hosts = [state["datanodes"][dn]["host"] for dn in dns]
            result.append({"chunk_id": c, "datanodes": dns, "dn_hosts": hosts})

    print(f"[NameNode] Prepared upload plan for {filename} ({len(alive_dns)} alive datanodes)")
    return jsonify({"chunks": result})

# --------------------------- DataNode Monitor ---------------------------
def monitor_datanodes():
    while True:
        now = time.time()
        changed = False
        with LOCK:
            for dn, info in list(state["datanodes"].items()):
                if now - info.get("last_seen", 0) > HEARTBEAT_TIMEOUT:
                    if info.get("alive"):
                        print(f"[NameNode] Marking {dn} as DEAD (no heartbeat for {now - info['last_seen']:.1f}s)")
                        state["datanodes"][dn]["alive"] = False
                        changed = True
                        threading.Thread(target=trigger_replication_for_dn, args=(dn,), daemon=True).start()
                else:
                    if not info.get("alive"):
                        print(f"[NameNode] Marking {dn} as ALIVE again (heartbeat received)")
                        state["datanodes"][dn]["alive"] = True
                        changed = True
        if changed:
            save_metadata()
        time.sleep(3)

# --------------------------- Replication Logic ---------------------------
def trigger_replication_for_dn(dead_dn):
    with LOCK:
        files_copy = dict(state["files"])
    for fname, finfo in files_copy.items():
        for chunk in finfo["chunks"]:
            with LOCK:
                replica_dns = list(finfo["chunks_info"].get(chunk, []))
                alive_replicas = [dn for dn in replica_dns if state["datanodes"].get(dn, {}).get("alive")]
                alive_nodes = [dn for dn, info in state["datanodes"].items() if info.get("alive")]

            if dead_dn in replica_dns and len(alive_replicas) < REPLICA_FACTOR:
                candidates = [d for d in alive_nodes if d not in alive_replicas]

                if not candidates and alive_replicas:
                    target_dn = alive_replicas[0]
                elif candidates:
                    target_dn = candidates[0]
                else:
                    continue

                source_dn = alive_replicas[0] if alive_replicas else None
                if not source_dn:
                    continue

                src_host = state["datanodes"][source_dn]["host"]
                tgt_host = state["datanodes"][target_dn]["host"]

                try:
                    r = requests.post(f"{src_host}/replicate_chunk",
                                      json={"chunk_id": chunk, "target_host": tgt_host},
                                      timeout=8)
                    if r.status_code == 200:
                        with LOCK:
                            if target_dn not in state["files"][fname]["chunks_info"][chunk]:
                                state["files"][fname]["chunks_info"][chunk].append(target_dn)
                            save_metadata()
                        print(f"[NameNode] Replicated {chunk} to {target_dn}")
                except Exception as e:
                    print("[NameNode] Replication error:", e)

# --------------------------- Register Stored Chunk ---------------------------
@app.route("/register_chunk", methods=["POST"])
def register_chunk():
    data = request.get_json()
    filename = data.get("filename")
    chunk_id = data.get("chunk_id")
    dn_id = data.get("dn_id")

    if not filename or not chunk_id or not dn_id:
        return jsonify({"error": "missing_parameters"}), 400

    with LOCK:
        if filename not in state["files"]:
            state["files"][filename] = {"chunks": [], "chunks_info": {}}
        if chunk_id not in state["files"][filename]["chunks_info"]:
            state["files"][filename]["chunks_info"][chunk_id] = []
        if dn_id not in state["files"][filename]["chunks_info"][chunk_id]:
            state["files"][filename]["chunks_info"][chunk_id].append(dn_id)
        save_metadata()

    print(f"[NameNode] Registered {chunk_id} from {dn_id} for {filename}")
    return jsonify({"status": "registered"})

# --------------------------- Chunk Map ---------------------------
@app.route("/get_chunk_map", methods=["GET"])
def get_chunk_map():
    filename = request.args.get("filename")
    with LOCK:
        if filename not in state["files"]:
            return jsonify({"error": "file_not_found"}), 404
        file_info = state["files"][filename]
        result = []
        for chunk_id in file_info["chunks"]:
            dns = file_info["chunks_info"][chunk_id]
            alive_dns = [dn for dn in dns if state["datanodes"].get(dn, {}).get("alive")]
            prioritized_dns = sort_datanodes_by_priority(alive_dns, request.remote_addr)
            dn_hosts = [state["datanodes"][dn]["host"] for dn in prioritized_dns]
            result.append({"chunk_id": chunk_id, "dn_hosts": dn_hosts})
    print(f"[NameNode] Sent chunk map for {filename} to client.")
    return jsonify({"chunks": result})

# --------------------------- Download Metadata ---------------------------
@app.route('/download_metadata', methods=['POST'])
def download_metadata():
    data = request.get_json()
    filename = data.get("filename")
    with LOCK:
        if filename not in state["files"]:
            return jsonify({"error": "File not found"}), 404
        file_info = state["files"][filename]
        chunks_info = file_info["chunks_info"]
    response = {"filename": filename, "chunks_info": chunks_info}
    return jsonify(response), 200

# --------------------------- Dashboard ---------------------------
@app.route('/')
def dashboard():
    with LOCK:
        files = dict(state.get("files", {}))
        datanodes = dict(state.get("datanodes", {}))
    now = time.time()
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>HDFS NameNode Dashboard</title>
        <meta http-equiv="refresh" content="5">
        <style>
            body { font-family: Arial, sans-serif; background-color: #f4f7fa; margin: 20px; }
            h1 { color: #2c3e50; }
            .section { background: #fff; padding: 15px; margin-top: 20px; border-radius: 10px;
                       box-shadow: 0 2px 6px rgba(0,0,0,0.1); }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
            th { background-color: #2980b9; color: white; }
            .alive { color: green; font-weight: bold; }
            .dead { color: red; font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>HDFS NameNode Dashboard</h1>
        <div class="section">
            <h2>Stored Files</h2>
            {% if files %}
            {% for fname, info in files.items() %}
                <h3>{{ fname }}</h3>
                <table>
                    <tr><th>Chunk ID</th><th>Replicas (Datanodes)</th></tr>
                    {% for cid, dns in info["chunks_info"].items() %}
                        <tr><td>{{ cid }}</td><td>{{ dns }}</td></tr>
                    {% endfor %}
                </table>
            {% endfor %}
            {% else %}
                <p>No files currently stored in HDFS.</p>
            {% endif %}
        </div>
        <div class="section">
            <h2>Datanode Status</h2>
            <table>
                <tr><th>ID</th><th>Host</th><th>Last Seen (s ago)</th><th>Status</th></tr>
                {% for dn, info in datanodes.items() %}
                    {% set diff = now - info.get("last_seen", 0) %}
                    {% if diff <= """ + str(HEARTBEAT_TIMEOUT) + """ %}
                        {% set status = "ALIVE" %}
                        {% set cls = "alive" %}
                    {% else %}
                        {% set status = "DEAD" %}
                        {% set cls = "dead" %}
                    {% endif %}
                    <tr>
                        <td>{{ dn }}</td>
                        <td>{{ info.get("host", "") }}</td>
                        <td>{{ "%.1f" % diff }}</td>
                        <td class="{{ cls }}">{{ status }}</td>
                    </tr>
                {% endfor %}
            </table>
        </div>
    </body>
    </html>
    """
    return render_template_string(html, files=files, datanodes=datanodes, now=now)

# --------------------------- List & Delete ---------------------------
@app.route("/list_files", methods=["GET"])
def list_files():
    with LOCK:
        files = list(state["files"].keys())
    result = {}
    for fname in files:
        info = state["files"][fname]
        result[fname] = info.get("chunks_info", {})
    return jsonify(result)

@app.route("/delete_file", methods=["POST"])
def delete_file():
    body = request.json
    filename = body.get("filename")
    if not filename or filename not in state["files"]:
        return jsonify({"error": "file_not_found"}), 404
    file_info = state["files"][filename]
    for chunk_id, dn_list in file_info["chunks_info"].items():
        for dn in list(dn_list):
            try:
                host = state["datanodes"][dn]["host"]
                requests.post(f"{host}/delete_chunk", json={"chunk_id": chunk_id}, timeout=5)
            except Exception as e:
                print(f"[NameNode] Warning: delete failed on {dn}: {e}")
    with LOCK:
        del state["files"][filename]
        save_metadata()
    print(f"[NameNode] Deleted file {filename} and its chunks from all datanodes.")
    return jsonify({"status": "deleted", "filename": filename})

# --------------------------- Verification ---------------------------
@app.route("/verify_file", methods=["GET"])
def verify_file():
    filename = request.args.get("filename")
    if not filename or filename not in state["files"]:
        return jsonify({"error": "file_not_found"}), 404
    file_info = state["files"][filename]
    status = {}
    for chunk_id, dn_list in file_info["chunks_info"].items():
        replicas_ok = []
        for dn in dn_list:
            try:
                host = state["datanodes"][dn]["host"]
                r = requests.get(f"{host}/verify_chunk", params={"chunk_id": chunk_id}, timeout=5)
                replicas_ok.append(r.status_code == 200)
            except Exception:
                replicas_ok.append(False)
        status[chunk_id] = replicas_ok
    return jsonify({"filename": filename, "status": status})

# --------------------------- Chunk Ops (for testing) ---------------------------
@app.route("/delete_chunk", methods=["POST"])
def delete_chunk():
    data = request.json
    chunk_id = data.get("chunk_id")
    if not chunk_id:
        return jsonify({"error": "missing_chunk_id"}), 400
    path = os.path.join("chunks", chunk_id)
    if os.path.exists(path):
        os.remove(path)
        print(f"[DataNode] Deleted {chunk_id}")
        return jsonify({"status": "deleted"})
    else:
        return jsonify({"status": "not_found"}), 404

@app.route("/verify_chunk", methods=["GET"])
def verify_chunk():
    chunk_id = request.args.get("chunk_id")
    if not chunk_id:
        return jsonify({"error": "missing_chunk_id"}), 400
    path = os.path.join("chunks", chunk_id)
    if os.path.exists(path):
        return jsonify({"status": "exists"})
    else:
        return jsonify({"status": "missing"}), 404

# --------------------------- Chunks Assigned to a DataNode ---------------------------
@app.route("/get_chunks_for_dn", methods=["GET"])
def get_chunks_for_dn():
    dn_id = request.args.get("dn_id")
    if not dn_id:
        return jsonify({"error": "missing_dn_id"}), 400

    with LOCK:
        chunks = []
        for fname, finfo in state["files"].items():
            for chunk_id, dns in finfo["chunks_info"].items():
                if dn_id in dns:
                    chunks.append(chunk_id)

    return jsonify({"chunks": chunks}), 200

@app.route("/replication_success", methods=["POST"])
def replication_success():
    data = request.get_json()
    chunk_id = data.get("chunk_id")
    from_dn = data.get("from_dn")
    to_dn = data.get("to_dn")
    print(f"[NameNode] Replication confirmed: {chunk_id} copied from {from_dn} → {to_dn}")
    return jsonify({"status": "ok"})

@app.route("/request_recovery", methods=["POST"])
def request_recovery():
    """
    When a DataNode reports a missing chunk, NameNode finds a healthy source
    and instructs it to replicate the chunk to the requesting DataNode.
    """
    try:
        payload = request.json
        chunk_id = payload.get("chunk_id")
        target_dn = payload.get("dn_id")

        if not chunk_id or not target_dn:
            return jsonify({"error": "missing_parameters"}), 400

        # Check which DNs currently have this chunk
        holders = CHUNK_MAPPING.get(chunk_id, [])
        if not holders:
            logger.warning(f"No replicas exist for chunk {chunk_id}; cannot recover.")
            return jsonify({"error": "no_source"}), 404

        # Pick a healthy source DN (simple round-robin or first healthy)
        healthy_sources = [dn for dn in holders if dn in ACTIVE_DN]
        if not healthy_sources:
            logger.warning(f"No healthy replicas found for {chunk_id}.")
            return jsonify({"error": "no_healthy_source"}), 404

        source_dn = healthy_sources[0]
        source_host = ACTIVE_DN[source_dn]
        target_host = ACTIVE_DN.get(target_dn)

        if not target_host:
            return jsonify({"error": "target_not_active"}), 404

        logger.info(f"Coordinating recovery for {chunk_id}: {source_dn} → {target_dn}")

        # Ask source DN to replicate to target
        try:
            requests.post(f"{source_host}/replicate_chunk",
                          json={"chunk_id": chunk_id, "target_host": target_host},
                          timeout=5)
        except Exception as e:
            logger.warning(f"Failed to instruct replication from {source_dn} to {target_dn} for {chunk_id}: {e}")
            return jsonify({"error": "replication_failed"}), 500

        return jsonify({"status": "recovery_started"}), 200

    except Exception as e:
        logger.exception("Error in /request_recovery: %s", e)
        return jsonify({"error": "internal"}), 500

# --------------------------- Main ---------------------------
if __name__ == "__main__":
    load_metadata()
    threading.Thread(target=monitor_datanodes, daemon=True).start()
    print("[NameNode] Listening on 0.0.0.0:5000")
    app.run(host="0.0.0.0", port=5000, threaded=True)


