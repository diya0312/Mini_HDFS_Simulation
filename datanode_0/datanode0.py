import argparse, threading, time, requests, os, base64, json
from flask import Flask, request, jsonify
from pathlib import Path
import socket
import hashlib
import traceback

app = Flask(__name__)
parser = argparse.ArgumentParser()
parser.add_argument("--id", required=True, help="datanode id, e.g. dn0")
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--namenode", default="http://10.144.198.253:5000")
parser.add_argument("--data_dir", default=None)
args = parser.parse_args()

DN_ID = args.id
PORT = args.port
NAMENODE = args.namenode.rstrip("/")
DATA_DIR = args.data_dir or f"./data_{DN_ID}"
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
HEARTBEAT_INTERVAL = 10.0


def log(msg, level="INFO"):
    """Simple unified logger"""
    print(f"[{level}] [{DN_ID}] {msg}")


def get_local_ip():
    return "10.144.157.51"


@app.route("/store_chunk", methods=["POST"])
def store_chunk():
    """
    Body: {"chunk_id": "...", "filename": "...", "data": "<base64>"}
    Stores the chunk locally and informs the NameNode.
    """
    payload = request.json
    chunk_id = payload.get("chunk_id")
    filename = payload.get("filename", "unknown_file")
    b64 = payload.get("data")

    if chunk_id is None or b64 is None:
        log("Missing chunk_id or data in store_chunk", "ERROR")
        return jsonify({"error": "bad_request"}), 400

    try:
        data = base64.b64decode(b64)
        path = os.path.join(DATA_DIR, chunk_id)

        # Write chunk
        with open(path, "wb") as f:
            f.write(data)

        # Compute and save checksum
        sha = hashlib.sha256(data).hexdigest()
        with open(path + ".sha256", "w") as hf:
            hf.write(sha)

        log(f"Stored chunk {chunk_id} ({len(data)} bytes) with checksum {sha[:12]}")

        # Notify NameNode
        try:
            requests.post(
                f"{NAMENODE}/register_chunk",
                json={"filename": filename, "chunk_id": chunk_id, "dn_id": DN_ID},
                timeout=3
            )
            log(f"Registered chunk {chunk_id} for {filename} with NameNode")
        except Exception as e:
            log(f"Failed to register chunk {chunk_id} -> NameNode: {e}", "ERROR")

        return jsonify({"status": "stored", "sha256": sha})

    except Exception as e:
        log(f"Error storing chunk {chunk_id}: {e}\n{traceback.format_exc()}", "ERROR")
        return jsonify({"error": "store_failed", "detail": str(e)}), 500


@app.route("/get_chunk", methods=["GET"])
def get_chunk():
    chunk_id = request.args.get("chunk_id")
    path = os.path.join(DATA_DIR, chunk_id)

    if not os.path.exists(path):
        log(f"Chunk {chunk_id} not found for GET", "WARN")
        return jsonify({"error": "not_found"}), 404

    try:
        with open(path, "rb") as f:
            data = f.read()

        current_hash = hashlib.sha256(data).hexdigest()

        # Data integrity check — compare with stored hash
        stored_hash_file = path + ".sha256"
        if os.path.exists(stored_hash_file):
            with open(stored_hash_file, "r") as hf:
                stored_hash = hf.read().strip()
            if stored_hash != current_hash:
                log(f"Checksum mismatch for {chunk_id}! Stored:{stored_hash[:12]} Curr:{current_hash[:12]}", "ERROR")
                return jsonify({"error": "corrupted_chunk"}), 500
        else:
            log(f"No checksum file for {chunk_id}, skipping verification", "WARN")

        b64data = base64.b64encode(data).decode("utf-8")
        log(f"Served chunk {chunk_id} (verified OK)")
        return jsonify({"data": b64data, "sha256": current_hash})

    except Exception as e:
        log(f"Failed to read/serve chunk {chunk_id}: {e}", "ERROR")
        return jsonify({"error": "read_failed", "detail": str(e)}), 500


@app.route("/replicate_chunk", methods=["POST"])
def replicate_chunk():
    """
    Request body: {"chunk_id":"...", "target_host":"http://ip:port"}
    This endpoint allows NameNode to instruct a source DN to forward chunk bytes to a target DN.
    """
    payload = request.json
    chunk_id = payload.get("chunk_id")
    target = payload.get("target_host")

    if not chunk_id or not target:
        log("Bad replication request — missing params", "ERROR")
        return jsonify({"error": "bad_request"}), 400

    path = os.path.join(DATA_DIR, chunk_id)
    if not os.path.exists(path):
        log(f"Replication failed — missing chunk {chunk_id}", "ERROR")
        return jsonify({"error": "missing"}), 404

    try:
        with open(path, "rb") as f:
            data = f.read()
        b64 = base64.b64encode(data).decode("utf-8")

        r = requests.post(
            f"{target.rstrip('/')}/store_chunk",
            json={"chunk_id": chunk_id, "data": b64},
            timeout=10
        )
        if r.status_code == 200:
            log(f"Replicated chunk {chunk_id} -> {target}")
            return jsonify({"status": "replicated"})
        else:
            log(f"Replication to {target} failed: {r.status_code} {r.text}", "ERROR")
            return jsonify({"error": "target_failed", "detail": r.text}), 500

    except Exception as e:
        log(f"Replication error for {chunk_id}: {e}", "ERROR")
        return jsonify({"error": str(e)}), 500


def send_heartbeat():
    while True:
        try:
            ip = get_local_ip()
            host = f"http://{ip}:{PORT}"
            requests.post(
                f"{NAMENODE}/heartbeat",
                json={"dn_id": DN_ID, "host": host},
                timeout=2
            )
            log(f"Heartbeat sent to NameNode ({host})")
        except Exception as e:
            log(f"Heartbeat failed: {e}", "WARN")
        time.sleep(HEARTBEAT_INTERVAL)


@app.route("/delete_chunk", methods=["POST"])
def delete_chunk():
    data = request.json
    chunk_id = data.get("chunk_id")
    if not chunk_id:
        log("Missing chunk_id in delete request", "ERROR")
        return jsonify({"error": "missing_chunk_id"}), 400

    path = os.path.join(DATA_DIR, chunk_id)
    try:
        if os.path.exists(path):
            os.remove(path)
            if os.path.exists(path + ".sha256"):
                os.remove(path + ".sha256")
            log(f"Deleted chunk {chunk_id}")
            return jsonify({"status": "deleted"})
        else:
            log(f"Chunk {chunk_id} not found during delete", "WARN")
            return jsonify({"status": "not_found"}), 404
    except Exception as e:
        log(f"Failed to delete {chunk_id}: {e}", "ERROR")
        return jsonify({"error": "delete_failed"}), 500


@app.route("/verify_chunk", methods=["GET"])
def verify_chunk():
    chunk_id = request.args.get("chunk_id")
    if not chunk_id:
        log("Missing chunk_id in verify request", "ERROR")
        return jsonify({"error": "missing_chunk_id"}), 400

    path = os.path.join(DATA_DIR, chunk_id)
    if not os.path.exists(path):
        log(f"Chunk {chunk_id} missing during verification", "WARN")
        return jsonify({"status": "missing"}), 404

    try:
        with open(path, "rb") as f:
            data = f.read()
        current_hash = hashlib.sha256(data).hexdigest()
        stored_hash_file = path + ".sha256"
        if os.path.exists(stored_hash_file):
            with open(stored_hash_file, "r") as hf:
                stored_hash = hf.read().strip()
            if stored_hash == current_hash:
                log(f"Chunk {chunk_id} verified OK (checksum match)")
                return jsonify({"status": "ok", "sha256": current_hash})
            else:
                log(f"Chunk {chunk_id} checksum mismatch!", "ERROR")
                return jsonify({"status": "corrupted"}), 500
        else:
            log(f"No checksum file for {chunk_id}", "WARN")
            return jsonify({"status": "unknown"}), 200
    except Exception as e:
        log(f"Error verifying chunk {chunk_id}: {e}", "ERROR")
        return jsonify({"error": "verify_failed"}), 500


if __name__ == "__main__":
    t = threading.Thread(target=send_heartbeat, daemon=True)
    t.start()
    log(f"Starting DataNode on 0.0.0.0:{PORT}, data dir {DATA_DIR}, NameNode at {NAMENODE}")
    app.run(host="0.0.0.0", port=PORT, threaded=True, debug=True)
