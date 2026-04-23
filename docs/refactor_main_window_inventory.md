# Inventario real: decomposicao de main_window.py

Data: 2026-04-23
Branch: refactor/p3-main-window-codex-gpt54

## Divergencia de caminho

O pedido cita `src/interface_grafica/main_window.py`, mas o checkout atual usa `src/interface_grafica/ui/main_window.py` como arquivo real. Os comandos de reconhecimento foram executados contra esse caminho real.

## Reconhecimento inicial

### wc -l src/interface_grafica/ui/main_window.py

```text
9539
```

### grep -nE '^class ' src/interface_grafica/ui/main_window.py

```text
91:class FloatDelegate(QStyledItemDelegate):
100:class PipelineWorker(QThread):
147:class ServiceTaskWorker(QThread):
189:class DetachedTableWindow(QMainWindow):
370:class ViewState:
381:class MainWindow(QMainWindow):
```

### grep -nE '^    def |^def ' src/interface_grafica/ui/main_window.py | wc -l

```text
262
```

### grep -nE 'QTabWidget|addTab|setTabText' src/interface_grafica/ui/main_window.py | head -40

```text
43:    QTabWidget,
672:        self.tabs = QTabWidget()
677:        self.tabs.addTab(self._build_tab_configuracoes(), "Configurações")
678:        self.tabs.addTab(self._build_tab_consulta(), "Consulta")
679:        self.tabs.addTab(self._build_tab_sql_query(), "Consulta SQL")
680:        self.tabs.addTab(self._build_tab_agregacao(), "Agregacao")
682:        self.tabs.addTab(self.tab_conversao, "Conversao")
683:        self.tabs.addTab(self._build_tab_estoque(), "Estoque")
685:        self.tabs.addTab(self.tab_nfe_entrada, "NFe Entrada")
686:        self.tabs.addTab(self._build_tab_analise_lote_cnpj(), "Análise Lote CNPJ")
687:        self.tabs.addTab(self._build_tab_logs(), "Logs")
1551:        """Retorna o painel Fisconforme não Atendido como aba do QTabWidget."""
1722:        self.estoque_tabs = QTabWidget()
1725:        self.estoque_tabs.addTab(self.tab_mov_estoque, "Tabela mov_estoque")
1728:        self.estoque_tabs.addTab(self.tab_aba_mensal, "Tabela mensal")
1731:        self.estoque_tabs.addTab(self.tab_aba_anual, "Tabela anual")
1733:        self.estoque_tabs.addTab(self.tab_aba_periodos, "Tabela períodos")
1736:        self.estoque_tabs.addTab(self.tab_resumo_global, "Resumo Global")
1739:        self.estoque_tabs.addTab(
1744:        self.estoque_tabs.addTab(self.tab_id_agrupados, "id_agrupados")
4553:            self.estoque_tabs.setTabText(idx, "Tabela mov_estoque")
4555:        self.estoque_tabs.setTabText(idx, f"Tabela mov_estoque ({visiveis})")
4566:            self.estoque_tabs.setTabText(idx, "Tabela anual")
4569:            self.estoque_tabs.setTabText(idx, f"Tabela anual ({visiveis})")
4571:        self.estoque_tabs.setTabText(idx, f"Tabela anual ({visiveis}/{total})")
4582:            self.estoque_tabs.setTabText(idx, "Tabela mensal")
4585:            self.estoque_tabs.setTabText(idx, f"Tabela mensal ({visiveis})")
4587:        self.estoque_tabs.setTabText(idx, f"Tabela mensal ({visiveis}/{total})")
4600:            self.estoque_tabs.setTabText(idx, "Produtos selecionados")
4603:            self.estoque_tabs.setTabText(idx, f"Produtos selecionados ({visiveis})")
4605:        self.estoque_tabs.setTabText(idx, f"Produtos selecionados ({visiveis}/{total})")
4616:            self.estoque_tabs.setTabText(idx, "id_agrupados")
4619:            self.estoque_tabs.setTabText(idx, f"id_agrupados ({visiveis})")
4621:        self.estoque_tabs.setTabText(idx, f"id_agrupados ({visiveis}/{total})")
4632:            self.estoque_tabs.setTabText(idx, "Tabela mensal")
4634:        self.estoque_tabs.setTabText(idx, f"Tabela mensal ({visiveis}/{total})")
4645:            self.estoque_tabs.setTabText(idx, "Tabela anual")
4647:        self.estoque_tabs.setTabText(idx, f"Tabela anual ({visiveis}/{total})")
4658:            self.tabs.setTabText(idx, "NFe Entrada")
4661:            self.tabs.setTabText(idx, f"NFe Entrada ({visiveis})")
```

### grep -nE 'setStyleSheet|background-color' src/interface_grafica/ui/main_window.py | head -20

```text
212:        self.lbl_titulo.setStyleSheet(
242:        self.lbl_status.setStyleSheet(
568:        botao.setStyleSheet(self._estilo_botao_destacar())
627:        self.btn_apagar_dados.setStyleSheet("QPushButton { color: #e57373; }")
632:        self.btn_apagar_cnpj.setStyleSheet(
879:        btn_salvar.setStyleSheet(self._estilo_botao_destacar())
938:        lbl.setStyleSheet("color: #ccaa00;")
954:                lbl.setStyleSheet("color: #4caf50; font-weight: bold;")
962:                lbl.setStyleSheet("color: #e57373;")
992:        lbl.setStyleSheet("color: #ccaa00;")
1007:                lbl.setStyleSheet("color: #4caf50;")
1010:                lbl.setStyleSheet("color: #e57373;")
1167:        self.table_view.setStyleSheet("QTableView::item { padding: 4px 2px; }")
1376:        self.btn_sql_execute.setStyleSheet(
1399:        self.sql_text_view.setStyleSheet(
1429:        self.sql_status_label.setStyleSheet(
1461:        self.sql_result_table.setStyleSheet("QTableView::item { padding: 4px 2px; }")
1523:        self.lbl_produto_sel.setStyleSheet("font-weight: bold; color: #1e40af;")
1526:        self.btn_apply_unid_ref.setStyleSheet("font-weight: bold;")
1581:        self.lbl_mov_estoque_titulo.setStyleSheet(
```

### ls -la src/interface_grafica/

```text
AGENTS.md
config.py
__init__.py
fisconforme/
models/
services/
ui/
utils/
windows/
```

## Marcos

- M0 inventario: concluido.
- M1 scaffold: concluido.
- M2 relatorios: concluido.
- M3 agregacao: concluido.
- M4 auditoria: concluido.
- M5 importacao: concluido.
- M6 cleanup main_window: parcial.
- M7 AGENTS: concluido.

## Estrutura apos M7

```text
src/interface_grafica/
  controllers/__init__.py
  widgets/__init__.py
  themes/__init__.py
  themes/noir.qss
  windows/__init__.py
  windows/main_window.py
  windows/aba_relatorios.py
  windows/aba_agregacao.py
  windows/aba_auditoria.py
  windows/aba_importacao.py
  ui/main_window.py
```

## Linha por arquivo apos M7

```text
aba_agregacao.py: 180
aba_auditoria.py: 105
aba_importacao.py: 205
aba_relatorios.py: 314
windows/main_window.py: 8
ui/main_window.py: 9093
```

## Validacao estrutural

| Marco | Widgets antes | Sinais antes | Widgets depois | Sinais depois | Resultado |
|---|---:|---:|---:|---:|---|
| M2 relatorios | 404 | 280 | 457 | 280 | ok |
| M3 agregacao | 402 | 280 | 463 | 280 | ok |
| M4 auditoria | 370 | 276 | 469 | 280 | ok |
| M5 importacao | 353 | 276 | 478 | 280 | ok |

## BLOCKED

BLOCKED: o gate "main_window.py final < 800 linhas" ainda nao foi cumprido para
`src/interface_grafica/ui/main_window.py`, que permanece com 9093 linhas. A PR
deve ficar em draft ate que os callbacks, workers, delegates, preferencias de
tabela e subtabs restantes sejam movidos para modulos menores sem alterar
comportamento. O ponto canonico `src/interface_grafica/windows/main_window.py`
tem 8 linhas, mas o legado ainda existe como implementacao transitoria.
