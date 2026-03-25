"""Re-exports for backward compatibility after Phase 2 restructuring."""

# tabelas_base
from transformacao.tabelas_base.tabela_documentos import gerar_tabela_documentos  # noqa
from transformacao.tabelas_base import item_unidades  # noqa
from transformacao.tabelas_base import itens  # noqa

# rastreabilidade_produtos
from transformacao.rastreabilidade_produtos import descricao_produtos  # noqa
from transformacao.rastreabilidade_produtos import produtos_final_v2  # noqa
from transformacao.rastreabilidade_produtos import produtos_agrupados  # noqa
from transformacao.rastreabilidade_produtos import id_agrupados  # noqa
from transformacao.rastreabilidade_produtos import fontes_produtos  # noqa
from transformacao.rastreabilidade_produtos import fatores_conversao  # noqa
from transformacao.rastreabilidade_produtos import precos_medios_produtos_final  # noqa

# movimentacao_estoque
from transformacao.movimentacao_estoque_pkg import movimentacao_estoque  # noqa
from transformacao.movimentacao_estoque_pkg import c170_xml  # noqa
from transformacao.movimentacao_estoque_pkg import c176_xml  # noqa
from transformacao.movimentacao_estoque_pkg import co_sefin  # noqa
from transformacao.movimentacao_estoque_pkg import co_sefin_class  # noqa

# calculos
from transformacao.calculos_mensais_pkg import calculos_mensais  # noqa
from transformacao.calculos_anuais_pkg import calculos_anuais  # noqa
