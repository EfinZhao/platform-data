import io
import os
import sys
import http.server
import html
import urllib.parse
import json
import pandas as pd
import shutil
import socketserver
import tempfile
import zipfile

PORT = 8000
DIRECTORY = "rt"

class SubdirectoryHandler(http.server.SimpleHTTPRequestHandler):
  def __init__(self, *args, **kwargs):
    super().__init__(*args, directory=DIRECTORY, **kwargs)

  def get_dir_size(self, start_path):
    total_size = 0
    try:
      for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
          fp = os.path.join(dirpath, f)
          if os.path.exists(fp):
            total_size += os.path.getsize(fp)
    except Exception:
      pass

    return total_size

  def format_size(self, size_bytes):
    if size_bytes == 0:
      return "0 B"

    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
      if size_bytes < 1024:
        return f"{size_bytes:.2f} {unit}"
      size_bytes /= 1024

    return f"{size_bytes:.2f} PB"

  def list_directory(self, path):
    try:
      list_ = os.listdir(path)
    except OSError:
      self.send_error(404, "No permission to list directory")
      return None
    list_.sort(key=lambda a: a.lower())

    displaypath = urllib.parse.unquote(self.path)
    displaypath = html.escape(displaypath, quote=False)

    enc = sys.getfilesystemencoding()

    r = []
    r.append('<!DOCTYPE HTML>')
    r.append(f'<html><head><title> Directory listing for {displaypath}</title></head>')
    r.append(f'<body>\n<h2>Directory listing for {displaypath}</h2>')
    r.append('<hr>\n<ul>')

    displaypath_unquoted = urllib.parse.unquote(self.path, errors="surrogatepass")
    if displaypath_unquoted.rstrip("/") not in ("", "/"):
      r.append('<li><a href="../">..</a></li>')

    for name in list_:
      fullname = os.path.join(path, name)
      displayname = name
      linkname    = name
      if os.path.isdir(fullname):
        displayname = name + "/"
        linkname = name + "/"
      r.append(f'<li><a href="{urllib.parse.quote(linkname)}">{html.escape(displayname)}</a></li>')
    r.append('</ul>\n<hr>\n</body>\n</html>\n')
    html_content = "\n".join(r)

    try:
      local_path        = self.translate_path(self.path)
      dir_size_bytes    = self.get_dir_size(local_path)
      dir_size          = self.format_size(dir_size_bytes)
      total, used, free      = shutil.disk_usage(local_path)
      total_space       = self.format_size(total)
      free_space        = self.format_size(free)

      banner_style = (
        "<style>"
        ".storage-bar {"
        "  background: #e9ecef; border-radius: 4px; height: 8px;"
        "  width: 100%; max-width: 400px; margin-top: 6px; overflow: hidden;"
        "}"
        ".storage-progress {"
        "  background: #28a745; height: 100%;"
        "}"
        ".download-btn {"
        "  display: inline-block; margin-top: 8px; padding: 6px 14px;"
        "  background: #007bff; color: #fff; text-decoration: none;"
        "  border-radius: 4px; font-family: sans-serif; font-size: 14px;"
        "}"
        ".download-btn:hover { background: #0056b3; }"
        "</style>"
      )

      current_path = self.path.split('?', 1)[0]
      if not current_path.endswith('/'):
        current_path += '/'
      download_href = current_path + "?download=zip"

      used_percent = (used / total) * 100 if total > 0 else 0

      banner_html = (
        f"{banner_style}"
        f"<a class='download-btn' href='{html.escape(download_href)}'>"
        f"Download directory as ZIP</a><br>"
        f"<strong>Directory Size:</strong> {dir_size}<br>"
        f"<strong>System Disk Space:</strong> Available: {free_space} / Total: {total_space}<br>"
        f"<div class='storage-bar'><div class='storage-progress' style='width: {used_percent:.1f}%'></div></div>"
      )

      if "<body>" in html_content:
        html_content = html_content.replace("<body>", f"<body>\n{banner_html}", 1)
      else:
        html_content = banner_html + html_content
    except Exception:
      pass

    encoded = html_content.encode(enc, "surrogateescape")

    f = io.BytesIO()
    f.write(encoded)
    f.seek(0)

    self.send_response(200)
    self.send_header("content-type", f"text/html; charset={enc}")
    self.send_header("Content-length", str(len(encoded)))
    self.end_headers()

    return f

  def handle_zip_download(self):
    parsed = urllib.parse.urlparse(self.path)
    local_path = self.translate_path(parsed.path)

    if not os.path.isdir(local_path):
        self.send_error(404, "Not a directory")
        return

    dir_name = os.path.basename(os.path.normpath(local_path)) or "root"

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".zip")
    os.close(tmp_fd)

    try:
      try:
        with zipfile.ZipFile(tmp_path, "w", zipfile.ZIP_DEFLATED) as zf:
          for root, dirs, files in os.walk(local_path):
            for file in files:
              file_path = os.path.join(root, file)
              arcname = os.path.join(dir_name, os.path.relpath(file_path, local_path))
              try:
                zf.write(file_path, arcname)
              except OSError:
                continue
      except Exception:
        self.send_error(500, "Failed to create ZIP archive")
        return

      zip_size = os.path.getsize(tmp_path)

      self.send_response(200)
      self.send_header("Content-type", "application/zip")
      self.send_header("Content-Disposition", f'attachment; filename="{dir_name}.zip"')
      self.send_header("Content-Length", str(zip_size))
      self.end_headers()

      chunk_size = 64 * 1024
      try:
        with open(tmp_path, "rb") as zf_read:
          while True:
            chunk = zf_read.read(chunk_size)
            if not chunk:
              break
            self.wfile.write(chunk)
      except (BrokenPipeError, ConnectionResetError):
        pass

    finally:
      try:
        os.remove(tmp_path)
      except OSError:
        pass

  def handle_parquet_view(self):
    parsed = urllib.parse.urlparse(self.path)
    local_path = self.translate_path(parsed.path)

    if not os.path.isfile(local_path):
      self.send_error(404, "File not found")
      return

    query = urllib.parse.parse_qs(parsed.query)
    full = query.get("full", ["0"])[0] == "1"
    try:
      limit = int(query.get("limit", ["1000"])[0])
      offset = int(query.get("offset", ["0"])[0])
    except ValueError:
      limit, offset = 1000, 0

    try:
      df = pd.read_parquet(local_path)
    except Exception as e:
      self.send_error(500, f"Failed to read parquet file: {e}")
      return

    total_rows = len(df)

    if not full:
      df_view = df.iloc[offset: offset + limit]
    else:
      df_view = df

    records_json = df_view.to_json(orient="records", date_format="iso")
    records = json.loads(records_json)

    payload = {
      "file": os.path.basename(local_path),
      "total_rows": len(records),
      "offset": offset if not full else 0,
      "limit": limit if not full else total_rows,
      "columns": list(df.columns.astype(str)),
      "data": records,
    }

    body = json.dumps(payload, indent=2).encode("utf-8")

    self.send_response(200)
    self.send_header("Content-type", "application/json; charset=utf-8")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)

  def do_GET(self):
    parsed = urllib.parse.urlparse(self.path)
    query = urllib.parse.parse_qs(parsed.query)
    
    if query.get("download") == ["zip"]:
      self.handle_zip_download()
      return

    if parsed.path.lower().endswith(".parquet"):
      self.handle_parquet_view()
      return

    super().do_GET()


with socketserver.TCPServer(("", PORT), SubdirectoryHandler) as httpd:
  print(f"Serving directory '{DIRECTORY}' at http://0.0.0.0:{PORT}")
  try:
    httpd.serve_forever()
  except KeyboardInterrupt:
    print("\nShutting down server...")
    httpd.server_close()

