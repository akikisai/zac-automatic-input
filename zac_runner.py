import calendar
import os
import json
from datetime import datetime
from typing import Callable, Optional

import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

_CHROME_CANDIDATES = ["/usr/bin/chromium", "/usr/bin/chromium-browser"]
_DRIVER_CANDIDATES = ["/usr/bin/chromedriver", "/usr/local/bin/chromedriver"]


def _find_executable(candidates: list[str]) -> Optional[str]:
    for path in candidates:
        if os.path.exists(path):
            return path
    return None


class ZacRunner:
    def __init__(self, config: dict, log_fn: Callable):
        self._cfg = config
        self._log_fn = log_fn

    def _log(self, msg: str, color: Optional[str] = None) -> None:
        self._log_fn(msg, color)

    @staticmethod
    def _resp_detail(resp: requests.Response) -> str:
        """レスポンス本文をログ向けに短く整形する。"""
        try:
            body = resp.json()
            text = json.dumps(body, ensure_ascii=False)
        except Exception:
            text = (resp.text or "").strip()
        if len(text) > 800:
            text = text[:800] + "..."
        return text

    @classmethod
    def login_only(
        cls,
        login_id: str,
        password: str,
        login_url: str,
        log_fn: Callable,
    ) -> Optional[dict]:
        runner = cls({}, log_fn)
        driver = runner._create_driver()
        try:
            session = runner._login(driver, login_id, password, login_url)
            if session is None:
                return None
            return session.cookies.get_dict()
        finally:
            driver.quit()

    # ── main entry ────────────────────────────────────────────────────────────

    def execute(self) -> None:
        cfg = self._cfg
        target_year = int(cfg["target_year"])
        target_month = int(cfg["target_month"])
        _, days_in_month = calendar.monthrange(target_year, target_month)

        start_day = cfg.get("start_day")
        end_day = cfg.get("end_day")
        if start_day and end_day:
            date_range = range(int(start_day), int(end_day) + 1)
        else:
            date_range = range(1, days_in_month + 1)

        session = self._session_from_cookies(cfg.get("session_cookies"))
        if session is None:
            self._log("先にログインしてください。", "red")
            return

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }

        if cfg.get("bulk_delete"):
            self._bulk_delete(session, headers, cfg, date_range, target_year, target_month)
        elif cfg.get("bulk_fix"):
            self._bulk_fix(session, headers, cfg, date_range, target_year, target_month)
        elif cfg.get("bulk_cancel_fix"):
            self._bulk_cancel_fix(session, headers, cfg, date_range, target_year, target_month)
        elif cfg.get("registration_mode"):
            self._registration(session, headers, cfg, date_range, target_year, target_month)
        else:
            self._log("実行モードが選択されていません", "red")

        self._log("処理が完了しました!", "green")
        self._log("zacの表示が正確かを確認してください\n")

    # ── driver setup ─────────────────────────────────────────────────────────

    def _create_driver(self) -> webdriver.Chrome:
        opts = Options()
        opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--disable-extensions")
        opts.add_argument("--window-size=1920,1080")

        chrome_bin = os.getenv("CHROME_BIN") or _find_executable(_CHROME_CANDIDATES)
        driver_bin = _find_executable(_DRIVER_CANDIDATES)

        if chrome_bin:
            opts.binary_location = chrome_bin

        if driver_bin:
            self._log(f"ChromeDriver: {driver_bin}")
            return webdriver.Chrome(service=Service(driver_bin), options=opts)

        # Fallback: Selenium Manager auto-download
        self._log("apt版ChromeDriverが見つからないためSelenium Managerで取得します")
        return webdriver.Chrome(options=opts)

    # ── login ─────────────────────────────────────────────────────────────────

    def _login(self, driver, login_id: str, password: str, login_url: str) -> Optional[requests.Session]:
        driver.get(login_url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "username"))
        ).send_keys(login_id)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "password"))
        ).send_keys(password)

        self._log("ログイン情報を入力中...")
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "cv-button"))
        ).click()
        self._log("ログイン中...")

        try:
            WebDriverWait(driver, 10).until(lambda d: d.current_url != login_url)
            try:
                driver.find_element(By.XPATH, "//*[contains(text(), 'エラー')]")
                self._log("ログイン失敗しました。ログイン情報またはVPN接続を確認してください", "red")
                return None
            except Exception:
                self._log("ログイン成功!\n", "green")
        except Exception:
            self._log("不明の原因でログイン失敗しました", "red")
            return None

        session = requests.Session()
        for cookie in driver.get_cookies():
            session.cookies.set(cookie["name"], cookie["value"])
        return session

    # ── helpers ───────────────────────────────────────────────────────────────

    def _session_from_cookies(self, cookie_dict: Optional[dict]) -> Optional[requests.Session]:
        if not cookie_dict:
            return None
        session = requests.Session()
        for name, value in cookie_dict.items():
            session.cookies.set(name, value)
        return session

    @staticmethod
    def _fmt_date(year: int, month: int, day: int) -> str:
        return datetime(year, month, day).strftime("%Y-%m-%d")

    def _fetch_report(self, session, headers, url_template: str, date: str) -> Optional[dict]:
        url = url_template.replace("yyyy-mm-dd", date)
        resp = session.get(url, headers=headers)
        if resp.status_code in (401, 403):
            self._log("セッションが無効です。ログインをやり直してください。", "red")
            return None
        if resp.status_code != 200:
            self._log(f"APIエラー: {resp.status_code}", "red")
            self._log(f"  URL: {url}", "orange")
            self._log(f"  DETAIL: {self._resp_detail(resp)}", "orange")
            return None
        return resp.json()

    def _cancel_fix(self, session, headers, url_template: str, date: str) -> bool:
        resp = session.post(
            url_template.replace("yyyy-mm-dd", date),
            json={"TargetDate": date},
            headers=headers,
        )
        if resp.status_code == 200:
            self._log("> 確定解除しました", "green")
            return True
        self._log(f"> 確定解除失敗: {resp.status_code} {resp.text}", "red")
        return False

    def _delete_entries(self, session, headers, url_template: str, date: str, entries: list) -> None:
        self._log("[削除処理]", "blue")
        if not entries:
            self._log("・削除：対象なし", "orange")
            self._log("")
            return
        for entry in entries:
            url = (
                url_template
                .replace("yyyy-mm-dd", date)
                .replace("target_id", str(entry["Id"]))
            )
            resp = session.delete(url, headers=headers)
            if resp.status_code == 200:
                pname = entry.get("Project", {}).get("Name", "不明な案件")
                sname = entry.get("ProjectSales", {}).get("Current", {}).get("Name", "不明な売上項目")
                self._log(f"・削除：{pname} / {sname}", "green")
            else:
                self._log(f"・削除失敗：ID={entry.get('Id')} ({resp.status_code})", "red")
        self._log("")

    def _prepare_day(self, session, headers, cfg, date: str, is_registration: bool = False):
        """日報取得・確定解除・既存エントリ削除。(data, no_time) を返す。"""
        data = self._fetch_report(session, headers, cfg["daily_report_data_url"], date)
        if data is None:
            return None, None

        fixed_status = data.get("NameStatus", "")
        entries = data.get("DailyReportDataList", [])

        if is_registration:
            time_in = data.get("TimeIn", 0) or 0
            time_out = data.get("TimeOut", 0) or 0
            no_time = not time_in or not time_out
            required_time = time_out - time_in if not no_time else 0
            self._log(f"要入力時間: {required_time} 分")

        self._log(f"確定状態: {fixed_status}")

        if fixed_status == "承認済":
            if not self._cancel_fix(session, headers, cfg["cancel_fix_url"], date):
                return None, None
            self._log("")
        else:
            self._log("未確定です。次の処理に進みます")
            self._log("")

        self._delete_entries(session, headers, cfg["work_deletion_url"], date, entries)

        if is_registration:
            return data, no_time
        return data, None

    # ── bulk delete ───────────────────────────────────────────────────────────

    def _bulk_delete(self, session, headers, cfg, date_range, year, month):
        for day in date_range:
            date = self._fmt_date(year, month, day)
            self._log("")
            self._log(f"━━━━ {date} の処理開始 ━━━━")
            data, _ = self._prepare_day(session, headers, cfg, date)
            if data is None:
                self._log("次の日付に進みます\n")

    def _bulk_fix(self, session, headers, cfg, date_range, year, month):
        for day in date_range:
            date = self._fmt_date(year, month, day)
            self._log("")
            self._log(f"━━━━ {date} の一括確定開始 ━━━━")

            data = self._fetch_report(session, headers, cfg["daily_report_data_url"], date)
            if data is None:
                self._log("次の日付に進みます\n")
                continue

            fixed_status = data.get("NameStatus", "")
            time_in = data.get("TimeIn", 0) or 0
            time_out = data.get("TimeOut", 0) or 0

            if fixed_status == "承認済":
                self._log("すでに承認済みのためスキップ", "orange")
                continue

            fix_url = cfg["fix_url"].replace("yyyy-mm-dd", date)
            payload = {
                "TargetDate": date,
                "TimeIn": time_in,
                "TimeOut": time_out,
                "UpdateDailyReportDataParameters": [],
            }
            resp = session.post(fix_url, json=payload, headers=headers)
            if resp.status_code == 200:
                self._log("確定成功", "green")
            else:
                self._log(f"確定失敗: {resp.status_code} {resp.text}", "red")

    def _bulk_cancel_fix(self, session, headers, cfg, date_range, year, month):
        for day in date_range:
            date = self._fmt_date(year, month, day)
            self._log("")
            self._log(f"━━━━ {date} の一括確定解除開始 ━━━━")

            data = self._fetch_report(session, headers, cfg["daily_report_data_url"], date)
            if data is None:
                self._log("次の日付に進みます\n")
                continue

            fixed_status = data.get("NameStatus", "")
            if fixed_status != "承認済":
                self._log("未確定のためスキップ", "orange")
                continue

            if self._cancel_fix(session, headers, cfg["cancel_fix_url"], date):
                self._log("確定解除成功", "green")

    # ── registration mode ─────────────────────────────────────────────────────

    def _registration(self, session, headers, cfg, date_range, year, month):
        projects = cfg.get("projects", [])
        daily_times = cfg.get("daily_times", [])

        for day in date_range:
            date = self._fmt_date(year, month, day)
            self._log("")
            self._log(f"━━━━ {date} の処理開始 ━━━━")

            data, no_time = self._prepare_day(session, headers, cfg, date, is_registration=True)
            if data is None:
                self._log("次の日付に進みます\n")
                continue

            if no_time:
                self._log("> KOT勤務時間データが取り込まれていないためスキップ\n", "orange")
                continue

            time_in = data.get("TimeIn", 0) or 0
            time_out = data.get("TimeOut", 0) or 0
            required_time = time_out - time_in

            self._log("[登録処理]", "blue")
            day_times = daily_times[day - 1] if day <= len(daily_times) else []
            remaining_time = required_time
            num = 0

            normal_items = []
            rest_items = []

            for idx, project in enumerate(projects):
                col_index = project.get("col_index", idx)
                try:
                    col_index = int(col_index)
                except (TypeError, ValueError):
                    col_index = idx
                value = day_times[col_index] if 0 <= col_index < len(day_times) else None
                project_id = project.get("project_id")
                sale_id = project.get("sale_id")
                if not project_id or not sale_id:
                    continue
                if value is None or value == "" or value == 0:
                    continue
                if value == "rest":
                    rest_items.append((col_index, project))
                    continue
                try:
                    minutes = int(value)
                except (ValueError, TypeError):
                    continue
                if minutes <= 0:
                    continue
                normal_items.append((col_index, project, minutes))

            def _register_project(project_idx: int, project: dict, minutes: int, rest_log: bool = False) -> bool:
                nonlocal num
                project_id = project.get("project_id")
                sale_id = project.get("sale_id")
                api_url = cfg["work_registration_url"].replace("yyyy-mm-dd", date)
                payload = {
                    "Id": None,
                    "No": 1,
                    "TimeRequired": minutes,
                    "TimeMove": None,
                    "TimeBegin": None,
                    "TimeEnd": None,
                    "IdProject": project_id,
                    "IdSagyouNaiyou": 1,
                    "IdProjectSales": sale_id,
                    "IdProjectNippouSagyouBunrui": None,
                    "IdTask": None,
                    "IdKoutei": None,
                    "Memo": None,
                }

                resp = session.post(api_url, json=payload, headers=headers)
                try:
                    resp_json = resp.json()
                except Exception:
                    resp_json = {}
                resp_list = resp_json.get("DailyReportDataList", [])

                if resp.status_code == 200:
                    num += 1
                    report = resp_list[num - 1] if num <= len(resp_list) else {}
                    pname = report.get("Project", {}).get("Name", project.get("name") or f"案件{project_idx + 1}")
                    sname = report.get("ProjectSales", {}).get("Current", {}).get("Name", "不明な売上項目")
                    if rest_log:
                        self._log(f"・登録：{pname} / {sname}  ->  KOT勤務時間の残りの{minutes}分", "green")
                    else:
                        self._log(f"・登録：{pname} / {sname}  ->  {minutes}分", "green")
                    return True

                self._log(f"・登録失敗：{resp.status_code}", "red")
                self._log(f"  URL: {api_url}", "orange")
                self._log(
                    f"  PAYLOAD: IdProject={project_id}, IdProjectSales={sale_id}, TimeRequired={minutes}",
                    "orange",
                )
                self._log(f"  DETAIL: {self._resp_detail(resp)}", "orange")
                return False

            overflow = False
            for idx, project, minutes in normal_items:
                if minutes > remaining_time:
                    p_name = project.get("name") or f"案件{idx + 1}"
                    self._log(f"・登録失敗：{p_name} は要入力時間を超過しています", "red")
                    overflow = True
                    break
                remaining_time -= minutes
                _register_project(idx, project, minutes)

            if not overflow and rest_items:
                rest_idx, rest_project = rest_items[0]
                rest_minutes = max(remaining_time, 0)
                _register_project(rest_idx, rest_project, rest_minutes, rest_log=True)
                if len(rest_items) > 1:
                    self._log("・警告：残りすべて設定が複数あるため、最初の1件のみ登録しました", "orange")

            self._log("> 登録終了", "green")
            self._log("")

            if cfg.get("registration_bulk_fix"):
                if num == 0:
                    self._log("登録件数が0件のため、確定処理をスキップします", "orange")
                    self._log("次の日付に進みます\n")
                    continue
                fix_url = cfg["fix_url"].replace("yyyy-mm-dd", date)
                fix_resp = session.post(
                    fix_url,
                    json={"TargetDate": date, "TimeIn": time_in, "TimeOut": time_out, "UpdateDailyReportDataParameters": []},
                    headers=headers,
                )
                if fix_resp.status_code == 200:
                    self._log("> 確定成功", "green")
                    self._log("")
                else:
                    self._log(f"確定失敗: {fix_resp.status_code}", "red")
                    self._log(f"  URL: {fix_url}", "orange")
                    self._log(f"  DETAIL: {self._resp_detail(fix_resp)}", "orange")
                    self._log("次の日付に進みます\n")
