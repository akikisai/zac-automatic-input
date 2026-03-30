import json
import os
import secrets
import queue
import threading
import traceback
import urllib.parse
from pathlib import Path
from collections import defaultdict

import requests as _http

from flask import Flask, Response, jsonify, render_template, request, session

from zac_runner import ZacRunner

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Strict",
    SESSION_COOKIE_SECURE=False,  # localhost運用のためFalse
)

_log_subscribers: dict[str, list[queue.Queue]] = defaultdict(list)
_session_state: dict[str, dict] = {}
_lock = threading.Lock()
_URL_LINKS_PATH = Path(__file__).resolve().parent / "config" / "url_links.json"

_URL_KEYS = (
    "login_url",
    "daily_report_list_url",
    "daily_report_data_url",
    "work_registration_url",
    "work_deletion_url",
    "fix_url",
    "cancel_fix_url",
)


def _load_json_dict(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(loaded, dict):
            return loaded
    except Exception:
        pass
    return {}


def _load_url_links() -> tuple[str, dict[str, str]]:
    loaded = _load_json_dict(_URL_LINKS_PATH)
    base_url_template = ""
    if isinstance(loaded.get("base_url"), str):
        base_url_template = loaded["base_url"].strip()

    endpoints = loaded.get("endpoints")
    endpoint_paths: dict[str, str] = {}
    if isinstance(endpoints, dict):
        for url_key in _URL_KEYS:
            value = endpoints.get(url_key)
            if isinstance(value, str) and value.strip():
                endpoint_paths[url_key] = value.strip()

    return base_url_template, endpoint_paths


def _normalize_base_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


def _join_base_and_path(base_url: str, path: str) -> str:
    return f"{_normalize_base_url(base_url)}{('/' + path.lstrip('/')) if path else ''}"


def _build_urls(base_url: str, endpoint_paths: dict[str, str]) -> dict:
    return {
        key: _join_base_and_path(base_url, endpoint_paths.get(key, ""))
        for key in _URL_KEYS
        if endpoint_paths.get(key)
    }


def _resolve_ui_url_defaults() -> dict:
    _, endpoint_paths = _load_url_links()

    env_base_url = _normalize_base_url(os.getenv("ZAC_BASE_URL") or "")

    if not env_base_url:
        return {}

    resolved = _build_urls(env_base_url, endpoint_paths)
    resolved["base_url"] = env_base_url
    return resolved


UI_URL_DEFAULTS = _resolve_ui_url_defaults()


def _url_config_warning_message() -> str:
    env_base_url = _normalize_base_url(os.getenv("ZAC_BASE_URL") or "")

    if env_base_url:
        return ""

    return "環境変数 ZAC_BASE_URL が未設定です。.env で ZAC_BASE_URL を設定して起動し直してください。"


def _log_url_config_warnings() -> None:
    warning = _url_config_warning_message()
    if not warning:
        return
    print(f"[WARN] {warning}")



_log_url_config_warnings()


def _make_zac_session(cookie_dict: dict) -> _http.Session:
    sess = _http.Session()
    for k, v in cookie_dict.items():
        sess.cookies.set(k, v)
    return sess


def _get_sid() -> str:
    sid = session.get("sid")
    if not sid:
        sid = secrets.token_urlsafe(24)
        session["sid"] = sid
    return sid


def _get_state() -> tuple[str, dict]:
    sid = _get_sid()
    with _lock:
        if sid not in _session_state:
            _session_state[sid] = {
                "running": False,
                "logged_in": False,
                "session_cookies": None,
            }
    return sid, _session_state[sid]


def _ensure_csrf() -> str:
    token = session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(24)
        session["csrf_token"] = token
    return token


def _verify_csrf() -> bool:
    sent = request.headers.get("X-CSRF-Token", "")
    expected = session.get("csrf_token", "")
    return bool(expected and sent and secrets.compare_digest(sent, expected))


def _broadcast(target_sid: str, message: str, color: str | None = None, done: bool = False) -> None:
    payload = json.dumps(
        {"message": message, "color": color, "done": done},
        ensure_ascii=False,
    )
    # Dockerコンテナ標準出力にも同じログを出し、障害時に docker logs で追跡しやすくする
    print(f"[sid={target_sid}] {message}")
    for q in list(_log_subscribers.get(target_sid, [])):
        try:
            q.put_nowait(payload)
        except queue.Full:
            pass


@app.before_request
def _prepare_request_context():
    _get_sid()
    _ensure_csrf()


@app.after_request
def _add_security_headers(resp):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, private"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["X-Frame-Options"] = "DENY"
    resp.headers["Referrer-Policy"] = "no-referrer"
    return resp


@app.route("/")
def index():
    return render_template(
        "index.html",
        default_urls=UI_URL_DEFAULTS,
        url_config_warning=_url_config_warning_message(),
    )


@app.route("/run", methods=["POST"])
def run():
    sid, state = _get_state()
    if not _verify_csrf():
        return jsonify({"error": "不正なリクエストです。画面を再読み込みしてください。"}), 403

    if not state["logged_in"] or not state["session_cookies"]:
        return jsonify({"error": "先にログインしてください。"}), 400

    with _lock:
        if state["running"]:
            return jsonify({"error": "実行中です。完了後に再試行してください。"}), 409
        state["running"] = True

    config = request.get_json(force=True)
    config["session_cookies"] = state["session_cookies"]

    def _task():
        try:
            ZacRunner(config, lambda m, c=None, d=False: _broadcast(sid, m, c, d)).execute()
        except Exception as exc:
            _broadcast(sid, f"予期しないエラーが発生しました: {exc}", "red")
            _broadcast(sid, traceback.format_exc(), "red")
        finally:
            _broadcast(sid, "━━ 処理終了 ━━", done=True)
            with _lock:
                state["running"] = False

    threading.Thread(target=_task, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/login", methods=["POST"])
def login():
    sid, state = _get_state()
    if not _verify_csrf():
        return jsonify({"error": "不正なリクエストです。画面を再読み込みしてください。"}), 403

    if state["running"]:
        return jsonify({"error": "実行中はログインできません。"}), 409

    payload = request.get_json(force=True)
    login_id = (payload.get("login_id") or "").strip()
    password = (payload.get("password") or "").strip()
    login_url = (payload.get("login_url") or "").strip()

    if not login_id or not password or not login_url:
        return jsonify({"error": "ログインID・パスワード・ログインURLを入力してください。"}), 400

    _broadcast(sid, "ログイン処理を開始します...")
    cookies = ZacRunner.login_only(login_id, password, login_url, lambda m, c=None, d=False: _broadcast(sid, m, c, d))

    if not cookies:
        state["logged_in"] = False
        state["session_cookies"] = None
        _broadcast(sid, "ログイン失敗。IDとpasswordの確認、または社内VPN接続を確認してください。", "red")
        return jsonify({"error": "ログイン失敗。IDとpasswordの確認、または社内VPN接続を確認してください。"}), 401

    state["session_cookies"] = cookies
    state["logged_in"] = True
    _broadcast(sid, "ログイン成功。セッションを保持しました。", "green")
    return jsonify({"status": "logged_in"})


@app.route("/logout", methods=["POST"])
def logout():
    sid, state = _get_state()
    if not _verify_csrf():
        return jsonify({"error": "不正なリクエストです。画面を再読み込みしてください。"}), 403

    if state["running"]:
        return jsonify({"error": "実行中はログアウトできません。"}), 409
    state["logged_in"] = False
    state["session_cookies"] = None
    _broadcast(sid, "ログアウトしました。")
    return jsonify({"status": "logged_out"})


@app.route("/status")
def status():
    _, state = _get_state()
    return jsonify({"running": state["running"], "logged_in": state["logged_in"]})


@app.route("/bootstrap")
def bootstrap():
    _, state = _get_state()
    return jsonify(
        {
            "csrf_token": _ensure_csrf(),
            "running": state["running"],
            "logged_in": state["logged_in"],
        }
    )


@app.route("/stream")
def stream():
    sid = _get_sid()
    q: queue.Queue = queue.Queue(maxsize=500)
    _log_subscribers[sid].append(q)

    def _generate():
        try:
            while True:
                try:
                    data = q.get(timeout=20)
                    yield f"data: {data}\n\n"
                    if json.loads(data).get("done"):
                        break
                except queue.Empty:
                    yield ": keepalive\n\n"
        finally:
            try:
                _log_subscribers[sid].remove(q)
                if not _log_subscribers[sid]:
                    del _log_subscribers[sid]
            except ValueError:
                pass

    return Response(
        _generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/api/project_search")
def api_project_search():
    _, state = _get_state()
    if not state["logged_in"] or not state["session_cookies"]:
        return jsonify({"error": "未ログイン"}), 401

    keyword = request.args.get("keyword", "").strip()
    base_url = request.args.get("base_url", "").rstrip("/")
    base_date = request.args.get("base_date", "").strip()

    if not keyword or not base_url:
        return jsonify({"Value": [], "OData": {"MaxCount": 0}})

    from datetime import date as _date
    if not base_date:
        base_date = _date.today().isoformat()

    url = (
        f"{base_url}/b/api/v2/reference/project"
        f"?$top=20&BaseDate={base_date}&Keyword={urllib.parse.quote(keyword)}"
    )
    sess = _make_zac_session(state["session_cookies"])
    try:
        resp = sess.get(url, headers={"Accept": "application/json"}, timeout=10)
        return jsonify(resp.json()), resp.status_code
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


@app.route("/api/project_sales")
def api_project_sales():
    _, state = _get_state()
    if not state["logged_in"] or not state["session_cookies"]:
        return jsonify({"error": "未ログイン"}), 401

    id_project = request.args.get("id_project", "").strip()
    base_url = request.args.get("base_url", "").rstrip("/")

    if not id_project or not base_url:
        return jsonify({"Value": []})

    url = (
        f"{base_url}/b/api/v2/reference/daily_report_project_sales"
        f"?IdProject={id_project}"
    )
    sess = _make_zac_session(state["session_cookies"])
    try:
        resp = sess.get(url, headers={"Accept": "application/json"}, timeout=10)
        return jsonify(resp.json()), resp.status_code
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
