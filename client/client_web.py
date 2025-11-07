#client_web.py
from flask import Flask, request, render_template_string, send_file, redirect, url_for
import os, tempfile
import client  # <-- import your existing client.py functions

app = Flask(__name__)
UPLOAD_FOLDER = tempfile.gettempdir()

# ---------------- Original TEMPLATE with ONE extra line for dashboard link ----------------
TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>HDFS Client Dashboard</title>
    <style>
        body { font-family: Arial, sans-serif; background: #f5f7fa; margin: 40px; }
        h1 { color: #2c3e50; }
        .box { background: white; border-radius: 10px; padding: 20px; margin-bottom: 30px;
               box-shadow: 0 2px 6px rgba(0,0,0,0.1); }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { border: 1px solid #ddd; padding: 8px; }
        th { background-color: #2980b9; color: white; }
        input[type=file] { margin-top: 10px; }
        button { padding: 6px 12px; background: #2980b9; color: white; border: none;
                 border-radius: 5px; cursor: pointer; }
        button:hover { background: #1f6696; }
        .msg { color: green; font-weight: bold; }
        a.btn { display:inline-block; padding:6px 12px; background:#2980b9; color:white;
                text-decoration:none; border-radius:5px; margin-bottom:15px; }
        a.btn:hover { background:#1f6696; }
    </style>
</head>
<body>
    <h1>🌐 HDFS Web Client</h1>

    <!-- ✅ Added: Dashboard link button -->
    <a href="/dashboard" class="btn">📊 View Dashboard</a>

    <div class="box">
        <h2>📤 Upload a File</h2>
        <form method="POST" enctype="multipart/form-data" action="/upload">
            <input type="file" name="file" required>
            <button type="submit">Upload</button>
        </form>
        {% if msg %}<p class="msg">{{ msg }}</p>{% endif %}
    </div>

    <div class="box">
        <h2>🗂 Stored Files</h2>
        {% if files %}
        <table>
            <tr><th>Filename</th><th>Actions</th></tr>
            {% for fname in files %}
                <tr>
                    <td>{{ fname }}</td>
                    <td>
                        <a href="/download/{{ fname }}"><button>Download</button></a>
                        <a href="/verify/{{ fname }}"><button>Verify</button></a>
                        <a href="/delete/{{ fname }}"><button>Delete</button></a>
                    </td>
                </tr>
            {% endfor %}
        </table>
        {% else %}
        <p>No files found in HDFS.</p>
        {% endif %}
    </div>
</body>
</html>
"""

# ---------------- Your Original Routes ----------------

@app.route("/", methods=["GET"])
def index():
    files = []
    msg = None
    try:
        resp = client.requests.get(f"{client.NAMENODE}/list_files")
        if resp.status_code == 200:
            data = resp.json()
            files = list(data.keys())
        else:
            msg = f"NameNode returned {resp.status_code}"
    except Exception as e:
        msg = f"Error connecting to NameNode: {e}"

    return render_template_string(TEMPLATE, files=files, msg=msg)

@app.route("/upload", methods=["POST"])
def upload():
    file = request.files["file"]
    if not file:
        return redirect(url_for("index"))

    path = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(path)

    try:
        client.split_and_upload(path)
        msg = f"Uploaded {file.filename} successfully!"
    except Exception as e:
        msg = f"Upload failed: {e}"

    # Refresh file list after upload
    resp = client.requests.get(f"{client.NAMENODE}/list_files")
    files = list(resp.json().keys()) if resp.status_code == 200 else []
    return render_template_string(TEMPLATE, files=files, msg=msg)

@app.route("/download/<fname>")
def download(fname):
    out_path = os.path.join(UPLOAD_FOLDER, "downloaded_" + fname)
    client.download_and_reconstruct(fname, out_path)
    return send_file(out_path, as_attachment=True)

@app.route("/verify/<fname>")
def verify(fname):
    client.requests.get(f"{client.NAMENODE}/verify_file", params={"filename": fname})
    return redirect(url_for("index"))
    
@app.route("/delete/<fname>")
def delete(fname):
    try:
        response = client.requests.post(f"{client.NAMENODE}/delete_file", json={"filename": fname})
        if response.status_code == 200:
            msg = f"Deleted {fname} successfully!"
        else:
            msg = f"Failed to delete {fname}: {response.text}"
    except Exception as e:
        msg = f"Error deleting {fname}: {str(e)}"
    # Refresh file list after delete
    try:
        data = client.requests.get(f"{client.NAMENODE}/list_files").json()
        files = list(data.keys()) if data else []
    except Exception:
        files = []
    return render_template_string(TEMPLATE, files=files, msg=msg)


# ---------------- ✅ Added: DASHBOARD / CHUNK VISUALIZATION ----------------
@app.route("/dashboard")
def dashboard():
    try:
        resp = client.requests.get(f"{client.NAMENODE}/list_files")
        if resp.status_code != 200:
            return f"<h2>Error from NameNode: {resp.status_code}</h2>"
        data = resp.json()
    except Exception as e:
        return f"<h2>Error connecting to NameNode: {e}</h2>"

    DASHBOARD_TEMPLATE = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>HDFS Dashboard</title>
        <style>
            body { font-family: Arial; background: #eef3f8; margin: 40px; }
            h1 { color: #2c3e50; }
            .file-box { background: white; border-radius: 10px; padding: 20px; margin-bottom: 30px;
                        box-shadow: 0 2px 6px rgba(0,0,0,0.1); }
            table { width: 100%; border-collapse: collapse; margin-top: 10px; }
            th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }
            th { background-color: #2980b9; color: white; }
            .node-box { display: inline-block; padding: 5px 10px; border-radius: 6px;
                        background: #dff0d8; margin-right: 5px; font-size: 14px; }
            .chunk { font-weight: bold; color: #2c3e50; }
            a.btn { display:inline-block; padding:6px 12px; background:#2980b9; color:white;
                    text-decoration:none; border-radius:5px; margin-bottom:15px; }
            a.btn:hover { background:#1f6696; }
        </style>
    </head>
    <body>
        <h1>📊 HDFS Chunk Visualization Dashboard</h1>
        <a href="/" class="btn">← Back to Client</a>

        {% for fname, chunks in data.items() %}
        <div class="file-box">
            <h2>📁 {{ fname }}</h2>
            <table>
                <tr><th>Chunk ID</th><th>Stored on DataNodes</th></tr>
                {% for chunk, nodes in chunks.items() %}
                    <tr>
                        <td class="chunk">{{ chunk }}</td>
                        <td>
                            {% for n in nodes %}
                                <span class="node-box">{{ n }}</span>
                            {% endfor %}
                        </td>
                    </tr>
                {% endfor %}
            </table>
        </div>
        {% endfor %}
    </body>
    </html>
    """
    return render_template_string(DASHBOARD_TEMPLATE, data=data)


# ---------------- End of additions ----------------

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)

