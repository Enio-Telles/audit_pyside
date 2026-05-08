from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

from PySide6.QtCore import QObject, QThread, Signal
from PySide6.QtNetwork import QNetworkAccessManager, QNetworkReply, QNetworkRequest
from PySide6.QtCore import QEventLoop, QUrl

from interface_grafica import __version__
from utilitarios.project_paths import PROJECT_ROOT

logger = logging.getLogger(__name__)

GITHUB_API_URL = "https://api.github.com/repos/Enio-Telles/audit_pyside/releases/latest"


@dataclass
class ReleaseInfo:
    tag_name: str
    body: str
    zip_url: str
    checksums_url: str | None = None


class UpdateWorker(QThread):
    finished = Signal(object)  # Emits ReleaseInfo if update found, None otherwise
    error = Signal(str)

    def run(self):
        # Auto-update only enabled on Windows bundle (frozen)
        if sys.platform != "win32" or not getattr(sys, "frozen", False):
            logger.debug("Auto-update disabled: non-Windows or non-frozen environment.")
            self.finished.emit(None)
            return

        try:
            manager = QNetworkAccessManager()
            loop = QEventLoop()

            request = QNetworkRequest(QUrl(GITHUB_API_URL))
            request.setHeader(QNetworkRequest.UserAgentHeader, "audit_pyside_updater")

            reply = manager.get(request)
            reply.finished.connect(loop.quit)
            loop.exec()

            if reply.error() != QNetworkReply.NoError:
                # Silent failure as per research doc
                logger.warning(f"Failed to check for updates: {reply.errorString()}")
                self.finished.emit(None)
                return

            data = json.loads(reply.readAll().data().decode("utf-8"))
            tag_name = data.get("tag_name", "")

            if self.is_newer(tag_name, __version__):
                assets = data.get("assets", [])
                zip_url = ""
                checksums_url = None

                for asset in assets:
                    name = asset.get("name", "")
                    if name.endswith(".zip"):
                        zip_url = asset.get("browser_download_url", "")
                    elif name == "checksums.txt":
                        checksums_url = asset.get("browser_download_url", "")

                if zip_url and checksums_url:
                    self.finished.emit(
                        ReleaseInfo(
                            tag_name=tag_name,
                            body=data.get("body", ""),
                            zip_url=zip_url,
                            checksums_url=checksums_url,
                        )
                    )
                else:
                    self.finished.emit(None)
            else:
                self.finished.emit(None)

        except Exception as e:
            logger.exception("Error in UpdateWorker")
            self.error.emit(str(e))

    def is_newer(self, latest: str, current: str) -> bool:
        # Simple semantic version comparison
        def parse(v):
            return [int(x) for x in v.lstrip("v").split(".")]

        try:
            return parse(latest) > parse(current)
        except Exception:
            return latest != current


class UpdateService(QObject):
    download_progress = Signal(int)
    download_finished = Signal(str)  # path to downloaded zip
    update_error = Signal(str)

    def __init__(self):
        super().__init__()
        self.manager = QNetworkAccessManager()
        self._zip_path = None
        self._expected_hash = None

    def start_update_download(self, release_info: ReleaseInfo):
        if release_info.checksums_url:
            self._download_checksums(release_info)
        else:
            self._download_zip(release_info.zip_url)

    def _download_checksums(self, release_info: ReleaseInfo):
        request = QNetworkRequest(QUrl(release_info.checksums_url))
        reply = self.manager.get(request)
        reply.finished.connect(lambda: self._on_checksums_downloaded(reply, release_info))

    def _on_checksums_downloaded(self, reply, release_info: ReleaseInfo):
        if reply.error() != QNetworkReply.NoError:
            logger.error(f"Failed to download checksums: {reply.errorString()}")
            self.update_error.emit(f"Erro ao baixar checksums: {reply.errorString()}")
            return

        content = reply.readAll().data().decode("utf-8").strip()
        # Expecting either just the hash or "hash  filename"
        self._expected_hash = content.split()[0]
        self._download_zip(release_info.zip_url)

    def _download_zip(self, url: str):
        request = QNetworkRequest(QUrl(url))
        self.reply = self.manager.get(request)
        self.reply.downloadProgress.connect(self._on_progress)
        self.reply.finished.connect(self._on_download_finished)

    def _on_progress(self, received, total):
        if total > 0:
            progress = int((received / total) * 100)
            self.download_progress.emit(progress)

    def _on_download_finished(self):
        if self.reply.error() != QNetworkReply.NoError:
            self.update_error.emit(f"Download failed: {self.reply.errorString()}")
            return

        temp_dir = Path(tempfile.gettempdir()) / "audit_pyside_update"
        temp_dir.mkdir(parents=True, exist_ok=True)
        zip_path = temp_dir / "update.zip"

        with open(zip_path, "wb") as f:
            f.write(self.reply.readAll().data())

        if self._expected_hash:
            if not self.verify_hash(str(zip_path), self._expected_hash):
                self.update_error.emit("Integrity check failed: SHA-256 mismatch.")
                return

        self.download_finished.emit(str(zip_path))

    def verify_hash(self, zip_path: str, expected_hash: str) -> bool:
        sha256_hash = hashlib.sha256()
        with open(zip_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest() == expected_hash

    def prepare_swap_script(self, zip_path: str) -> str:
        if getattr(sys, "frozen", False):
            app_dir = Path(sys.executable).resolve().parent
        else:
            app_dir = PROJECT_ROOT

        temp_dir = Path(zip_path).parent
        extract_dir = temp_dir / "extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)

        # We need to extract the zip first to know what to copy
        shutil.unpack_archive(zip_path, extract_dir)

        app_dir_name = Path(app_dir).name

        bat_content = f"""@echo off
setlocal enabledelayedexpansion

set "APP_DIR={app_dir}"
set "TEMP_DIR={temp_dir}"
set "EXTRACT_DIR={extract_dir}"
set "BACKUP_DIR=%APP_DIR%.bak"
set "BROKEN_DIR=%APP_DIR%.broken"
set "EXE_NAME=FiscalParquetAnalyzer.exe"

echo Aguardando o encerramento do aplicativo (timeout 30s)...
set /a count=0
:wait_loop
tasklist /fi "imagename eq %EXE_NAME%" | findstr /i "%EXE_NAME%" > nul
if %errorlevel% equ 0 (
    set /a count+=1
    if !count! gtr 30 (
        echo Erro: O aplicativo nao encerrou a tempo. Abortando.
        pause
        exit /b 1
    )
    timeout /t 1 /nobreak > nul
    goto wait_loop
)

echo Criando backup em %BACKUP_DIR%...
if exist "%BACKUP_DIR%" rd /s /q "%BACKUP_DIR%"
xcopy /e /i /h /y "%APP_DIR%" "%BACKUP_DIR%"

echo Aplicando atualizacao...
for /f "delims=" %%f in ('dir /b /a "%EXTRACT_DIR%"') do (
    set "skip="
    if /i "%%f"=="dados" set skip=1
    if /i "%%f"=="workspace" set skip=1
    if /i "%%f"=="sql" set skip=1
    if /i "%%f"==".env" set skip=1

    if not defined skip (
        echo Atualizando %%f...
        if exist "%APP_DIR%\\%%f" (
            dir /a:d "%APP_DIR%\\%%f" >nul 2>nul
            if !errorlevel! equ 0 (
                rd /s /q "%APP_DIR%\\%%f"
            ) else (
                del /f /q "%APP_DIR%\\%%f"
            )
        )
        if exist "%EXTRACT_DIR%\\%%f\\*" (
            xcopy /e /i /h /y "%EXTRACT_DIR%\\%%f" "%APP_DIR%\\%%f"
        ) else (
            copy /y "%EXTRACT_DIR%\\%%f" "%APP_DIR%\\%%f"
        )
    )
)

echo Lancando nova versao...
start "" "%APP_DIR%\\%EXE_NAME%"
timeout /t 10 /nobreak > nul

tasklist /fi "imagename eq %EXE_NAME%" | findstr /i "%EXE_NAME%" > nul
if %errorlevel% neq 0 (
    echo Falha na inicializacao. Restaurando backup...
    if exist "%BROKEN_DIR%" rd /s /q "%BROKEN_DIR%"
    ren "%APP_DIR%" "{app_dir_name}.broken"
    xcopy /e /i /h /y "%BACKUP_DIR%" "%APP_DIR%"
    echo Rollback concluido. Versao defeituosa movida para %BROKEN_DIR%.
    start "" "%APP_DIR%\\%EXE_NAME%"
    pause
    exit /b 1
)

echo Atualizacao concluida com sucesso.
rd /s /q "%BACKUP_DIR%"
rd /s /q "%TEMP_DIR%"
exit /b 0
"""
        bat_path = temp_dir / "apply_update.bat"
        bat_path.write_text(bat_content, encoding="latin-1")
        return str(bat_path)
