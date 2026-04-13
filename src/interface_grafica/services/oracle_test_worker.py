"""
Worker assíncrono para testar conexão Oracle sem bloquear a UI.

Aceita os parâmetros da conexão diretamente (não lê do .env),
para que o teste reflita exatamente o que está digitado nos campos.
"""
from __future__ import annotations

from time import perf_counter

from PySide6.QtCore import QThread, Signal


class OracleConnectionTestWorker(QThread):
    """Testa uma conexão Oracle em background e emite o resultado."""

    # (sucesso: bool, mensagem: str, tempo_ms: int)
    resultado = Signal(bool, str, int)

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

    def run(self) -> None:
        t0 = perf_counter()
        try:
            import oracledb  # lazy import — safe inside thread

            if not self._host or not self._service or not self._user or not self._password:
                self.resultado.emit(False, "Preencha host, serviço, usuário e senha antes de testar.", 0)
                return

            porta = int(self._port) if self._port.isdigit() else 1521
            dsn = oracledb.makedsn(self._host, porta, service_name=self._service)
            conn = oracledb.connect(
                user=self._user,
                password=self._password,
                dsn=dsn,
                tcp_connect_timeout=8,
            )
            versao = ""
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT BANNER FROM V$VERSION WHERE ROWNUM = 1"
                    )
                    row = cur.fetchone()
                    if row:
                        versao = row[0]
            finally:
                conn.close()

            tempo_ms = int((perf_counter() - t0) * 1000)
            msg = f"Conexão OK ({tempo_ms} ms)"
            if versao:
                # exibe apenas a primeira linha da banner
                msg += f"\n{versao.splitlines()[0]}"
            self.resultado.emit(True, msg, tempo_ms)

        except Exception as exc:  # noqa: BLE001
            tempo_ms = int((perf_counter() - t0) * 1000)
            self.resultado.emit(False, str(exc), tempo_ms)
