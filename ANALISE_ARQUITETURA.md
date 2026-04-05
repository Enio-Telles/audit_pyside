# Análise de Arquitetura e Validação de Observações

Este documento contém a validação das observações levantadas sobre a arquitetura atual do projeto.

## 1. app.py faz bootstrap via inserção manual de src/ no sys.path
**Status: Validado.**
O arquivo `app.py` utiliza a manipulação direta do `sys.path` para garantir que as importações dos pacotes dentro do diretório `src/` e `src/utilitarios/` funcionem sem a necessidade de instalação formal do pacote (ex: via `pip install -e .`).
Isso pode ser visto logo no início de `app.py`:
```python
ROOT_DIR = Path(__file__).parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))
```

## 2. src/orquestrador_pipeline.py usa registry e lazy import
**Status: Validado.**
O arquivo `src/orquestrador_pipeline.py` implementa o padrão Registry na classe `_TabelaRegistro`. O uso de lazy import é uma boa prática aplicada no método `resolver`, que importa os módulos responsáveis pelo processamento apenas no momento da execução, prevenindo loops de importação e overhead de carregamento:
```python
def resolver(self) -> Callable:
    if self._func is None:
        modulo_path, nome_func = self.funcao_path.rsplit(":", 1)
        import importlib
        mod = importlib.import_module(modulo_path)
        self._func = getattr(mod, nome_func)
    return self._func
```

## 3. src/interface_grafica/ui/main_window.py concentra grande volume de responsabilidades
**Status: Validado.**
A classe `MainWindow` no arquivo `src/interface_grafica/ui/main_window.py` é massiva, contendo mais de **6800 linhas de código**. Ela gerencia não apenas o layout e eventos da UI (PySide6), mas também instila diretamente regras de negócios, manipula DataFrames, exporta planilhas e gerencia threads. Esse acúmulo de responsabilidades fere o princípio de Single Responsibility Principle (SRP) e o padrão MVC/MVVM.

## 4. Há sinais de duplicação de métodos no main_window.py
**Status: Validado.**
Ao longo do código de `main_window.py`, encontramos diversas duplicações, principalmente de funções internas de callback (ex: `def _on_success(ok) -> None:`) aninhadas em diferentes métodos da interface. Funções com o mesmo nome e lógica muito parecida aparecem repetidas vezes em blocos diferentes, como nas linhas 3794, 6006, e 6559. Além disso, blocos repetitivos de tratamento de erro ou recarregamento de abas espalhados aumentam o esforço de manutenção.

## 5. Há leitura direta de parquet dentro da camada de UI
**Status: Validado.**
A camada de interface está acoplada à estrutura do sistema de arquivos e lê arquivos Parquet diretamente através da biblioteca `polars` no meio dos métodos da janela. Por exemplo:
```python
# Linha 2932:
self._mov_estoque_df = pl.read_parquet(path)
# Linha 4131:
pl.read_parquet(path_nfe).with_columns(pl.lit("NFe").alias("fonte_documento"))
# Linha 4336:
self._id_agrupados_df = pl.read_parquet(path)
```
Idealmente, a interface deveria solicitar os dados de uma camada de serviço abstraída, sem conhecer os caminhos dos arquivos ou a biblioteca de dados utilizada.

## 6. Há uso de openpyxl e exportação sendo coordenados pela janela principal
**Status: Validado.**
A lógica de formatação e escrita de relatórios do Excel está embutida na interface. A janela principal importa `Workbook` e realiza as exportações chamando métodos que instanciam ativamente os workbooks, violando as fronteiras de arquitetura (deveria estar na camada de infraestrutura/serviço).
```python
# main_window.py, linha 4418 e linha 4952
wb = Workbook()
ws_id_agrupados = wb.active
ws_id_agrupados.title = "id_agrupados"
```

## 7. Há forte acoplamento entre eventos de interface e processamento de dados
**Status: Validado.**
A execução de pipelines pesadas, recalculos de agregações e interações do sistema de arquivos (criação e carregamento de planilhas/parquets) ocorrem na mesma classe que lida com os cliques nos botões e manipulação de tabelas. Essa falta de separação entre o "Model" e a "View" cria um forte acoplamento em `main_window.py`.

## 8. Frontend já implementa as propostas sugeridas de modernização
**Status: Validado e Implementado.**
Analisando o código fonte na pasta `frontend/`, observamos que todas as propostas indicadas já estão plenamente em uso:
1. **Adicionar TypeScript**: O projeto foi inteiramente construído com React + TypeScript (ex. `App.tsx`, extensos tipos mapeados em `api/types.ts`). A configuração de build (`tsconfig.app.json` e `tsconfig.node.json`) está pronta.
2. **Gerenciamento de Estado**: Está sendo utilizado o **Zustand**. O arquivo `src/store/appStore.ts` demonstra a gestão centralizada do CNPJ selecionado, filtros e tabs ativas.
3. **Testes Automatizados**: A suite de testes está configurada com `Vitest` e `@testing-library/react`. Um exemplo funcional de teste para hooks (`useRelatorio.test.tsx`) garante o bom funcionamento via mocks de requisições API.
4. **Padronização de Estilos**: A UI utiliza inteiramente **Tailwind CSS**, que é integrado via `@tailwindcss/vite`, substituindo CSS manual por classes utilitárias no JSX.

Não há refatorações requeridas, pois a infraestrutura do frontend alcançou 100% da meta destas propostas.
