"""
Worker assíncrono para testar conexão Oracle sem bloquear a UI.

Aceita os parâmetros da conexão diretamente (não lê do .env),
para que o teste reflita exatamente o que está digitado nos campos.
"""

from __future__ import annotations

import re
from time import perf_counter

import structlog
from PySide6.QtCore import QThread, Signal


_LOG = structlog.get_logger(__name__)
_ORA_CODE_RE = re.compile(r"ORA-\d{5}", re.IGNORECASE)


def _extrair_codigo_oracle(exc: Exception) -> str | None:
    for arg in getattr(exc, "args", () ):
        codigo = getattr(arg, "code", None)
        if isinstance(codigo, int):
            return f"ORA-{codigo:05d}"

        full_code = getattr(arg, "full_code", None)
        if isinstance(full_code, str):
            texto = full_code.strip().upper()
            if texto:
                match = _ORA_CODE_RE.search(texto)
                if match is not None:
                    return match.group(0).upper()

        if isinstance(arg, str):
            match = _ORA_CODE_RE.search(arg)
            if match is not None:
                return match.group(0).upper()

        texto = str(arg).strip()
        if texto:
            match = _ORA_CODE_RE.search(texto)
            if match is not None:
                return match.group(0).upper()

    texto = str(exc).strip()
    if texto:
        match = _ORA_CODE_RE.search(texto)
        if match is not None:
            return match.group(0).upper()

    return None


def _classificar_erro_oracle(exc: Exception) -> tuple[str, str, str]:
    codigo = _extrair_codigo_oracle(exc) or "ORACLE-ERROR"
    if codigo == "ORA-01017":
        return codigo, "Credenciais Oracle invalidas.", "oracle.connection.invalid_credentials"
    if codigo == "ORA-12541":
        return codigo, "Listener/host indisponivel.", "oracle.connection.listener_unavailable"
    if codigo == "ORA-12170":
        return codigo, "Tempo esgotado na conexao Oracle.", "oracle.connection.connect_timeout"

    mensagem = str(exc).strip() or "Falha ao testar a conexao Oracle."
    return codigo, mensagem, "oracle.connection.database_error"


class OracleConnectionTestWorker(QThread):
    """Testa uma conexão Oracle em background e emite o resultado."""

    # (sucesso: bool, codigo: str, mensagem: str)
    resultado = Signal(bool, str, str)

    def __init__(
        self,
        host: str,
        port: str,
        service: str,
        user: str,
        password: str,
        parent=None,
    ) -> None:
        super().__init__(parent)
        self._host = host.strip()
        self._port = port.strip()
        self._service = service.strip()
        self._user = user.strip()
        self._password = password.strip()

    def cancelar(self) -> None:
        self.requestInterruption()

    def _emitir_resultado(self, sucesso: bool, codigo: str, mensagem: str) -> None:
        if self.isInterruptionRequested():
            self.resultado.emit(False, "CANCELADO", "Teste de conexao cancelado.")
            return
        self.resultado.emit(sucesso, codigo, mensagem)

    def run(self) -> None:
        t0 = perf_counter()
        dsn = ""
        try:
            if self.isInterruptionRequested():
                self._emitir_resultado(False, "CANCELADO", "Teste de conexao cancelado.")
                return

            import oracledb  # lazy import — safe inside thread

            if (
                not self._host
                or not self._service
                or not self._user
                or not self._password
            ):
                self._emitir_resultado(
                    False,
                    "VALIDACAO",
                    "Preencha host, servico, usuario e senha antes de testar.",
                )
                return

            porta = int(self._port) if self._port.isdigit() else 1521
            dsn = oracledb.makedsn(self._host, porta, service_name=self._service)
            conn = oracledb.connect(
                user=self._user,
                password=self._password,
                dsn=dsn,
                tcp_connect_timeout=8,
            )

            if self.isInterruptionRequested():
                try:
                    conn.close()
                except Exception as close_exc:  # pragma: no cover - caminho operacional
                    _LOG.warning(
                        "oracle.connection.close_failed",
                        dsn=dsn,
                        erro=str(close_exc),
                    )
                self._emitir_resultado(False, "CANCELADO", "Teste de conexao cancelado.")
                return

            versao = ""
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1")
                    row = cur.fetchone()
                    if row:
                        versao = row[0]
            finally:
                try:
                    conn.close()
                except Exception as close_exc:  # pragma: no cover - limpeza operacional
                    _LOG.warning(
                        "oracle.connection.close_failed",
                        dsn=dsn,
                        erro=str(close_exc),
                    )

            tempo_ms = int((perf_counter() - t0) * 1000)
            mensagem = f"Conexao OK ({tempo_ms} ms)"
            if versao:
                mensagem += f"\n{versao.splitlines()[0]}"
            self._emitir_resultado(True, "OK", mensagem)

        except Exception as exc:  # noqa: BLE001
            tempo_ms = int((perf_counter() - t0) * 1000)
            if self.isInterruptionRequested():
                self._emitir_resultado(False, "CANCELADO", "Teste de conexao cancelado.")
                return

            try:
                import oracledb  # lazy import — safe inside thread

                if isinstance(exc, oracledb.DatabaseError):
                    codigo, mensagem, evento = _classificar_erro_oracle(exc)
                    if codigo in {"ORA-01017", "ORA-12541", "ORA-12170"}:
                        _LOG.warning(
                            evento,
                            dsn=dsn,
                            codigo=codigo,
                            tempo_ms=tempo_ms,
                        )
                    else:
                        _LOG.error(
                            evento,
                            dsn=dsn,
                            codigo=codigo,
                            tempo_ms=tempo_ms,
                            exc_info=exc,
                        )
                    self._emitir_resultado(False, codigo, mensagem)
                    return
            except Exception:
                pass

            _LOG.error(
                "oracle.connection.unexpected_error",
                dsn=dsn,
                tempo_ms=tempo_ms,
                exc_info=exc,
            )
            self._emitir_resultado(
                False,
                "UNEXPECTED",
                "Falha inesperada ao testar a conexao Oracle.",
            )
