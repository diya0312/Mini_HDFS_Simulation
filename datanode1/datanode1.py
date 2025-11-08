# datanode_demo.py
import argparse
import threading
import time
import requests
import os
import base64
import hashlib
from flask import Flask, request, jsonify
from pathlib import Path

# ----------------------------
# Flask app
# ----------------------------
app = Flask(__name__)

# ----------------------------
# Arguments
# ----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--id", required=True, help="Datanode ID, e.g. dn1")
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--namenode", default="http://10.144.198.253:5000")
parser.add_argument("--data_dir", default=None)
args = parser.parse_args()

DN_ID = args.id
PORT = args.port
NAMENODE = args.namenode.rstrip("/")
DATA_DIR = args.data_dir or f"./data_{DN_ID}"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

HEARTBEAT_INTERVAL = 10
RECOVERY_INTERVAL = 30
HEARTBEAT_RETRIES = 3

# ----------------------------
# Utility Functions
# ----------------------------
def get_local_ip():
    return "10.144.232.80"  # replace with proper detection if needed

def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def write_sha_file(path, sha):
    with open(path + ".sha256", "w") as f:
        f.write(sha)

def print_sha(message, sha):
    sha_short = sha[:12]  # first 12 characters for logging
    print(f"[{DN_ID}] {message} with checksum: {sha_short}…")

def demo_log(message):
    print(f"[{DN_ID}] DEMO: {message}")

# ----------------------------
# STORE CHUNK
# ----------------------------
@app.route("/store_chunk", methods=["POST"])
def store_chunk():
    payload = request.json
    chunk_id = payload.get("chunk_id")
    filename = payload.get("filename", chunk_id)
    b64data = payload.get("data")

    if not chunk_id or not b64data:
        return jsonify({"error": "bad_request"}), 400

    data = base64.b64decode(b64data)
    path = os.path.join(DATA_DIR, chunk_id)

    with open(path, "wb") as f:
        f.write(data)

    sha = compute_sha256(data)
    write_sha_file(path, sha)
    print_sha(f"Stored chunk {chunk_id}", sha)
    demo_log(f"Chunk {chunk_id} stored successfully, ready for replication.")

    # Notify NameNode
    try:
        requests.post(f"{NAMENODE}/register_chunk",
                      json={"chunk_id": chunk_id, "dn_id": DN_ID, "filename": filename},
                      timeout=3)
    except Exception as e:
        print(f"[{DN_ID}] Failed to register chunk {chunk_id}: {e}")

    return jsonify({"status": "stored", "sha256": sha})

# ----------------------------
# REPLICATE CHUNK
# ----------------------------
@app.route("/replicate_chunk", methods=["POST"])
def replicate_chunk():
    payload = request.json
    chunk_id = payload.get("chunk_id")
    target_host = payload.get("target_host")

    if not chunk_id or not target_host:
        return jsonify({"error": "bad_request"}), 400

    path = os.path.join(DATA_DIR, chunk_id)
    sha_path = path + ".sha256"

    if not os.path.exists(path):
        return jsonify({"error": "missing_chunk"}), 404

    with open(path, "rb") as f:
        data = f.read()

    b64data = base64.b64encode(data).decode()
    filename = chunk_id

    try:
        url = target_host.rstrip("/") + "/store_chunk"
        r = requests.post(url, json={"chunk_id": chunk_id, "data": b64data, "filename": filename}, timeout=10)
        if r.status_code == 200:
            remote_sha = r.json().get("sha256")
            local_sha = open(sha_path).read().strip() if os.path.exists(sha_path) else compute_sha256(data)
            if remote_sha == local_sha:
                print_sha(f"Replicated {chunk_id} successfully to {target_host}", local_sha)
                demo_log(f"Replication of {chunk_id} verified with checksum.")
                # Notify NameNode
                try:
                    requests.post(f"{NAMENODE}/replication_success",
                                  json={"chunk_id": chunk_id, "from_dn": DN_ID, "to_dn": target_host}, timeout=3)
                except:
                    pass
                return jsonify({"status": "replicated"})
            else:
                return jsonify({"error": "checksum_mismatch"}), 500
        else:
            return jsonify({"error": "target_failed", "detail": r.text}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ----------------------------
# GET CHUNK
# ----------------------------
@app.route("/get_chunk", methods=["GET"])
def get_chunk():
    chunk_id = request.args.get("chunk_id")
    if not chunk_id:
        return jsonify({"error": "missing_chunk_id"}), 400

    path = os.path.join(DATA_DIR, chunk_id)
    sha_path = path + ".sha256"

    if not os.path.exists(path):
        return jsonify({"error": "not_found"}), 404

    with open(path, "rb") as f:
        data = f.read()

    actual_sha = compute_sha256(data)
    stored_sha = open(sha_path).read().strip() if os.path.exists(sha_path) else ""

    if stored_sha and stored_sha != actual_sha:
        return jsonify({"error": "corrupted_chunk"}), 500

    print_sha(f"Retrieved chunk {chunk_id}", actual_sha)
    demo_log(f"Chunk {chunk_id} served to client successfully.")
    return jsonify({"data": base64.b64encode(data).decode(), "sha256": actual_sha})

# ----------------------------
# DELETE CHUNK
# ----------------------------
@app.route("/delete_chunk", methods=["POST"])
def delete_chunk():
    data = request.json
    chunk_id = data.get("chunk_id")
    if not chunk_id:
        return jsonify({"error": "missing_chunk_id"}), 400

    path = os.path.join(DATA_DIR, chunk_id)
    if os.path.exists(path):
        os.remove(path)
        if os.path.exists(path + ".sha256"):
            os.remove(path + ".sha256")
        print(f"[{DN_ID}] Deleted chunk {chunk_id} with checksum removed")
        demo_log(f"Chunk {chunk_id} deleted successfully.")
        return jsonify({"status": "deleted"})
    return jsonify({"status": "not_found"}), 404

# ----------------------------
# VERIFY CHUNK
# ----------------------------
@app.route("/verify_chunk", methods=["GET"])
def verify_chunk():
    chunk_id = request.args.get("chunk_id")
    if not chunk_id:
        return jsonify({"error": "missing_chunk_id"}), 400

    path = os.path.join(DATA_DIR, chunk_id)
    if not os.path.exists(path):
        return jsonify({"status": "missing"}), 404

    with open(path, "rb") as f:
        data = f.read()

    actual_sha = compute_sha256(data)
    stored_sha_path = path + ".sha256"
    stored_sha = open(stored_sha_path).read().strip() if os.path.exists(stored_sha_path) else ""

    status = "valid" if stored_sha == actual_sha else "corrupted"
    print_sha(f"Verification of chunk {chunk_id}: {status}", actual_sha)
    demo_log(f"Checksum verification for {chunk_id} complete.")
    return jsonify({"status": status})

# ----------------------------
# HEARTBEAT THREAD
# ----------------------------
def send_heartbeat():
    while True:
        host = f"http://{get_local_ip()}:{PORT}"
        for attempt in range(1, HEARTBEAT_RETRIES + 1):
            try:
                print(f"[{DN_ID}] DEMO: Sending heartbeat to {NAMENODE}/heartbeat)")
                requests.post(f"{NAMENODE}/heartbeat", json={"dn_id": DN_ID, "host": host}, timeout=2)
                break
            except Exception as e:
                print(f"[{DN_ID}] DEMO: Heartbeat attempt failed: {e}")
                time.sleep(1)
        time.sleep(HEARTBEAT_INTERVAL)

# ----------------------------
# RECOVERY THREAD
# ----------------------------
def recovery_thread():
    while True:
        try:
            r = requests.get(f"{NAMENODE}/get_chunks_for_dn", params={"dn_id": DN_ID}, timeout=5)
            if r.status_code == 200:
                for chunk_info in r.json().get("chunks", []):
                    chunk_id = chunk_info.get("chunk_id") if isinstance(chunk_info, dict) else chunk_info
                    source_dn = chunk_info.get("source_dn") if isinstance(chunk_info, dict) else None
                    path = os.path.join(DATA_DIR, chunk_id)
                    if not os.path.exists(path) and source_dn:
                        try:
                            print(f"[{DN_ID}] DEMO: Missing chunk {chunk_id}, fetching from {source_dn}")
                            r2 = requests.get(f"{source_dn.rstrip('/')}/get_chunk", params={"chunk_id": chunk_id}, timeout=5)
                            if r2.status_code == 200:
                                data = base64.b64decode(r2.json()["data"])
                                with open(path, "wb") as f:
                                    f.write(data)
                                sha = compute_sha256(data)
                                write_sha_file(path, sha)
                                print_sha(f"Recovered chunk {chunk_id} from {source_dn}", sha)
                                demo_log(f"Recovery of {chunk_id} complete.")
                        except Exception as e:
                            print(f"[{DN_ID}] DEMO: Failed to recover chunk {chunk_id} from {source_dn}: {e}")
        except Exception as e:
            print(f"[{DN_ID}] DEMO: Recovery check failed: {e}")
        time.sleep(RECOVERY_INTERVAL)

# ----------------------------
# MAIN
# ----------------------------
if __name__ == "__main__":
    threading.Thread(target=send_heartbeat, daemon=True).start()
    threading.Thread(target=recovery_thread, daemon=True).start()
    print(f"[DataNode {DN_ID}] Running on port {PORT} with data dir {DATA_DIR}")
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=True)

