#client.py
import os, math, base64, requests, sys, hashlib, json
CHUNK_SIZE = 32 # 512 KB
NAMENODE = "http://10.144.198.253:5000"

def compute_checksums(filepath):
    # returns dict: chunk_id -> sha256
    filesize = os.path.getsize(filepath)
    n = math.ceil(filesize / CHUNK_SIZE)
    d = {}
    with open(filepath, "rb") as f:
        for i in range(n):
            data = f.read(CHUNK_SIZE)
            cid = f"{os.path.basename(filepath)}.chunk.{i}"
            d[cid] = hashlib.sha256(data).hexdigest()
    return d

def split_and_upload(filepath, namenode=NAMENODE):
    import math, base64, os, requests

    filename = os.path.basename(filepath)
    filesize = os.path.getsize(filepath)
    num_chunks = math.ceil(filesize / CHUNK_SIZE)
    checksums = compute_checksums(filepath)

    print("[Client] Requesting upload plan from NameNode...")
    r = requests.post(
        f"{namenode}/upload_metadata",
        json={"filename": filename, "num_chunks": num_chunks, "checksums": checksums}
    )
    if r.status_code != 200:
        print("NameNode error:", r.status_code, r.text)
        return

    plan = r.json()["chunks"]  # [{chunk_id, dn_hosts}, ...]
    with open(filepath, "rb") as f:
        for info in plan:
            chunk_id = info["chunk_id"]
            data = f.read(CHUNK_SIZE)
            b64 = base64.b64encode(data).decode("utf-8")

            # send chunk to each assigned DataNode
            for host in info["dn_hosts"]:
                try:
                    resp = requests.post(
                        host.rstrip("/") + "/store_chunk",
                        json={
                            "chunk_id": chunk_id,
                            "filename": filename,  
                            "data": b64
                        },
                        timeout=15
                    )
                    if resp.status_code == 200:
                        print(f"[Client] Uploaded {chunk_id} -> {host}")
                    else:
                        print("[Client] Upload failed:", resp.status_code, resp.text)
                except Exception as e:
                    print("[Client] Upload exception to", host, e)


def download_and_reconstruct(filename, out_path, namenode=NAMENODE):
    r = requests.get(f"{namenode}/get_chunk_map", params={"filename": filename})
    if r.status_code != 200:
        print("NameNode error:", r.status_code, r.text)
        return
    chunks = r.json()["chunks"]
    # sort by chunk index
    chunks_sorted = sorted(chunks, key=lambda c: int(c["chunk_id"].split(".")[-1]))
    with open(out_path, "wb") as out:
        for c in chunks_sorted:
            retrieved = False
            # try each host until success
            for host in c["dn_hosts"]:
                if not host:
                    continue
                try:
                    resp = requests.get(host.rstrip("/") + "/get_chunk", params={"chunk_id": c["chunk_id"]}, timeout=8)
                    if resp.status_code == 200:
                        data_b64 = resp.json()["data"]
                        data = base64.b64decode(data_b64)
                        # optional: verify checksum if NameNode provided one
                        expected = c.get("checksum")
                        if expected:
                            import hashlib
                            if hashlib.sha256(data).hexdigest() != expected:
                                print("[Client] WARNING: checksum mismatch for", c["chunk_id"], "from", host)
                        out.write(data)
                        print("[Client] Wrote", c["chunk_id"], "from", host)
                        retrieved = True
                        break
                except Exception:
                    continue
            if not retrieved:
                print("[Client] Failed to retrieve chunk", c["chunk_id"])
                return
    print("[Client] Reconstructed file saved to", out_path)
def delete_file(filename, namenode=NAMENODE):
    r = requests.post(f"{namenode}/delete_file", json={"filename": filename})
    if r.status_code == 200:
        print(f"[Client] Deleted file {filename} from HDFS.")
    else:
        print("Delete failed:", r.status_code, r.text)


def verify_file(filename, namenode=NAMENODE):
    r = requests.get(f"{namenode}/verify_file", params={"filename": filename})
    if r.status_code != 200:
        print("Verification failed:", r.status_code, r.text)
        return
    result = r.json()
    print(f"Verification for {filename}:")
    for chunk, replicas in result["status"].items():
        print(f"  {chunk}: {[ 'OK' if ok else 'MISSING' for ok in replicas ]}")


def pretty_list(namenode=NAMENODE):
    r = requests.get(f"{namenode}/list_files")
    if r.status_code != 200:
        print("Error fetching file list:", r.status_code, r.text)
        return
    data = r.json()
    print("=== Files in HDFS ===")
    for fname, info in data.items():
        print(f"📄 {fname}:")
        for chunk, dns in info.items():
            print(f"   {chunk} -> {dns}")


def list_files(namenode=NAMENODE):
    r = requests.get(f"{namenode}/list_files")
    print(r.json())

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 client.py upload <file> | download <filename> <outpath> | list | delete <filename> | verify <filename>")
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "upload":
        split_and_upload(sys.argv[2])
    elif cmd == "download":
        download_and_reconstruct(sys.argv[2], sys.argv[3])
    elif cmd == "list":
        pretty_list()
    elif cmd == "delete":
        delete_file(sys.argv[2])
    elif cmd == "verify":
        verify_file(sys.argv[2])
    else:
        print("Unknown command", cmd)

