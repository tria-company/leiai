"""
schemas.py — Colunas CSV, chaves internas e funções de conversão de payload.
"""

import re

# Colunas do CSV final (nomes exatos)
CSV_COLUMNS = [
    'arquivo_origem',
    'pessoa',
    'analise_cobertura_segurado',
    'analise_cobertura_terceiros',
    'identificacao_envolvidos_protocolo_segurado',
    'identificacao_envolvidos_placa_segurado',
    'identificacao_veiculo_cliente_chave_natural',
    'identificacao_veiculo_veiculo',
    'analise_franquia_fipe_grupo_titular_crlv',
    'analise_franquia_fipe_grupo_fipe_mes_fato',
    'analise_franquia_fipe_grupo_grupo',
    'analise_franquia_fipe_grupo_tipo_franquia',
    'analise_franquia_fipe_grupo_franquia_numero',
    'analise_franquia_fipe_grupo_franquia_valor',
    'analise_cobertura_sinistro_data_fato',
    'analise_cobertura_sinistro_hora_fato',
    'analise_cobertura_sinistro_dia_semana',
    'analise_cobertura_sinistro_data_registro_bo',
    'analise_cobertura_sinistro_hora_registro_bo',
    'analise_cobertura_sinistro_numero_bo',
    'analise_cobertura_sinistro_tipo_bo',
    'analise_cobertura_sinistro_analise_financeira_vencimento_erp',
    'analise_cobertura_sinistro_analise_financeira_data_pagamento',
    'analise_cobertura_sinistro_analise_financeira_tipo',
    'analise_cobertura_sinistro_analise_financeira_esta_coberto',
    'analise_cobertura_sinistro_assistencia_24hrs',
    'consulta_detran_cnh_status',
    'consulta_detran_cnh_categoria',
    'consulta_detran_cnh_obs',
    'consulta_detran_cnh_validade',
    'consulta_detran_cnh_pontos',
    'consulta_detran_detalhamento_restricao_cnh',
    'consulta_detran_crlv_ano',
    'consulta_detran_ipva_licenciamento',
    'consulta_detran_multas',
    'consulta_detran_uf',
    'consulta_detran_multa_sinistro',
    'consulta_detran_restricoes',
    'analise_detalhamento_sinistro_local_fato_url',
    'analise_detalhamento_sinistro_relato_fato_bo',
    'analise_detalhamento_sinistro_ressarcimento',
    'analise_detalhamento_sinistro_item_regulamento_segurado',
    'analise_detalhamento_sinistro_art_ctb',
    'analise_detalhamento_sinistro_solicitado_sindicancia_pericia',
    'analise_detalhamento_sinistro_responsavel_sindicancia',
    'analise_detalhamento_sinistro_resultado_sindicancia',
    'analise_detalhamento_sinistro_pontos_exaltar_analista',
    'analise_detalhamento_sinistro_resumo_fato',
    'analise_detalhamento_sinistro_parecer_regulagem',
    'conclusao_analise_conclusao_segurado',
    'conclusao_analise_danos_veiculo_segurado',
    'conclusao_analise_classificacao_no_fato',
    'conclusao_analise_monta',
    'conclusao_analise_analista_responsavel',
    'conclusao_analise_inicio_analise',
    'conclusao_analise_conclusao_analise',
    'conclusao_analise_sinistro_aberto_em',
    'conclusao_analise_analise_recebida_em',
    'conclusao_analise_analise_liberada_em',
    'conclusao_analise_observacoes',
]

# Chaves internas usadas na extração (curtas, para facilitar o código)
INTERNAL_KEYS = [
    'arquivo_origem', 'pessoa',
    'cobertura_segurado', 'cobertura_terceiros',
    'protocolo_segurado', 'placa_segurado',
    'cliente_chave_natural', 'veiculo',
    'titular_crlv', 'fipe_mes_fato', 'grupo',
    'tipo_franquia', 'franquia_numero', 'franquia_valor',
    'data_fato', 'hora_fato', 'dia_semana',
    'data_registro_bo', 'hora_registro_bo', 'numero_bo', 'tipo_bo',
    'vencimento_erp', 'data_pagamento', 'tipo_pagamento', 'esta_coberto',
    'assistencia_24hrs',
    'cnh_status', 'cnh_categoria', 'cnh_obs', 'cnh_validade', 'cnh_pontos',
    'detalhamento_restricao_cnh',
    'crlv_ano', 'ipva_licenciamento', 'multas', 'uf',
    'multa_sinistro', 'restricoes',
    'local_fato_url', 'relato_fato_bo',
    'ressarcimento',
    'item_regulamento_segurado', 'art_ctb_segurado',
    'solicitado_sindicancia', 'responsavel_sindicancia', 'resultado_sindicancia',
    'pontos_exaltar_analista', 'resumo_fato', 'parecer_regulagem',
    'conclusao_segurado', 'danos_veiculo_segurado',
    'classificacao_fato_segurado', 'monta_segurado',
    'analista_responsavel_segurado', 'inicio_analise', 'conclusao_analise',
    'sinistro_aberto_em', 'analise_recebida_em', 'analise_liberada_em',
    'observacoes',
    # Campos extras extraídos do XLSX (não vão para o CSV principal)
    'protocolo_terceiro', 'placa_terceiro',
    'vistoria_previa', 'pecas_depreciacao', 'cadastro_leilao',
    'num_apolice_seguro', 'ano_modelo_fabricacao',
    'assistencia_data', 'assistencia_hora',
    'veiculo_rastreado', 'empresa_rastreador',
    'lmi_rcf_terceiros', 'consulta_recall',
    'relato_fatos_segurado', 'relato_fatos_terceiro',
    'homonimos_segurado_terceiros',
    'item_regulamento_terceiros', 'art_ctb_terceiros',
    'avarias_preexistentes', 'avaliacao_pneus',
    'conclusao_terceiro', 'conclusao_terceiro_02',
    'danos_veiculo_terceiro', 'classificacao_fato_terceiro', 'monta_terceiro',
    'analista_responsavel_terceiro', 'inicio_analise_terceiro', 'conclusao_analise_terceiro',
    'pasta_origem',
]

# Mapeamento: chave interna → coluna CSV
_KEY_TO_CSV = {
    'arquivo_origem':               'arquivo_origem',
    'pessoa':                       'pessoa',
    'cobertura_segurado':           'analise_cobertura_segurado',
    'cobertura_terceiros':          'analise_cobertura_terceiros',
    'protocolo_segurado':           'identificacao_envolvidos_protocolo_segurado',
    'placa_segurado':               'identificacao_envolvidos_placa_segurado',
    'cliente_chave_natural':        'identificacao_veiculo_cliente_chave_natural',
    'veiculo':                      'identificacao_veiculo_veiculo',
    'titular_crlv':                 'analise_franquia_fipe_grupo_titular_crlv',
    'fipe_mes_fato':                'analise_franquia_fipe_grupo_fipe_mes_fato',
    'grupo':                        'analise_franquia_fipe_grupo_grupo',
    'tipo_franquia':                'analise_franquia_fipe_grupo_tipo_franquia',
    'franquia_numero':              'analise_franquia_fipe_grupo_franquia_numero',
    'franquia_valor':               'analise_franquia_fipe_grupo_franquia_valor',
    'data_fato':                    'analise_cobertura_sinistro_data_fato',
    'hora_fato':                    'analise_cobertura_sinistro_hora_fato',
    'dia_semana':                   'analise_cobertura_sinistro_dia_semana',
    'data_registro_bo':             'analise_cobertura_sinistro_data_registro_bo',
    'hora_registro_bo':             'analise_cobertura_sinistro_hora_registro_bo',
    'numero_bo':                    'analise_cobertura_sinistro_numero_bo',
    'tipo_bo':                      'analise_cobertura_sinistro_tipo_bo',
    'vencimento_erp':               'analise_cobertura_sinistro_analise_financeira_vencimento_erp',
    'data_pagamento':               'analise_cobertura_sinistro_analise_financeira_data_pagamento',
    'tipo_pagamento':               'analise_cobertura_sinistro_analise_financeira_tipo',
    'esta_coberto':                 'analise_cobertura_sinistro_analise_financeira_esta_coberto',
    'assistencia_24hrs':            'analise_cobertura_sinistro_assistencia_24hrs',
    'cnh_status':                   'consulta_detran_cnh_status',
    'cnh_categoria':                'consulta_detran_cnh_categoria',
    'cnh_obs':                      'consulta_detran_cnh_obs',
    'cnh_validade':                 'consulta_detran_cnh_validade',
    'cnh_pontos':                   'consulta_detran_cnh_pontos',
    'detalhamento_restricao_cnh':   'consulta_detran_detalhamento_restricao_cnh',
    'crlv_ano':                     'consulta_detran_crlv_ano',
    'ipva_licenciamento':           'consulta_detran_ipva_licenciamento',
    'multas':                       'consulta_detran_multas',
    'uf':                           'consulta_detran_uf',
    'multa_sinistro':               'consulta_detran_multa_sinistro',
    'restricoes':                   'consulta_detran_restricoes',
    'local_fato_url':               'analise_detalhamento_sinistro_local_fato_url',
    'relato_fato_bo':               'analise_detalhamento_sinistro_relato_fato_bo',
    'ressarcimento':                'analise_detalhamento_sinistro_ressarcimento',
    'item_regulamento_segurado':    'analise_detalhamento_sinistro_item_regulamento_segurado',
    'art_ctb_segurado':             'analise_detalhamento_sinistro_art_ctb',
    'solicitado_sindicancia':       'analise_detalhamento_sinistro_solicitado_sindicancia_pericia',
    'responsavel_sindicancia':      'analise_detalhamento_sinistro_responsavel_sindicancia',
    'resultado_sindicancia':        'analise_detalhamento_sinistro_resultado_sindicancia',
    'pontos_exaltar_analista':      'analise_detalhamento_sinistro_pontos_exaltar_analista',
    'resumo_fato':                  'analise_detalhamento_sinistro_resumo_fato',
    'parecer_regulagem':            'analise_detalhamento_sinistro_parecer_regulagem',
    'conclusao_segurado':           'conclusao_analise_conclusao_segurado',
    'danos_veiculo_segurado':       'conclusao_analise_danos_veiculo_segurado',
    'classificacao_fato_segurado':  'conclusao_analise_classificacao_no_fato',
    'monta_segurado':               'conclusao_analise_monta',
    'analista_responsavel_segurado':'conclusao_analise_analista_responsavel',
    'inicio_analise':               'conclusao_analise_inicio_analise',
    'conclusao_analise':            'conclusao_analise_conclusao_analise',
    'sinistro_aberto_em':           'conclusao_analise_sinistro_aberto_em',
    'analise_recebida_em':          'conclusao_analise_analise_recebida_em',
    'analise_liberada_em':          'conclusao_analise_analise_liberada_em',
    'observacoes':                  'conclusao_analise_observacoes',
}


# ---------------------------------------------------------------------------
# Funções de conversão
# ---------------------------------------------------------------------------

def empty_payload() -> dict:
    return {k: '' for k in INTERNAL_KEYS}


def _clean_for_csv(val) -> str:
    """Remove quebras de linha e espaços extras para não quebrar o CSV."""
    if val is None:
        return ''
    s = str(val).strip()
    s = s.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
    s = re.sub(r'\s{2,}', ' ', s)
    return s


def payload_to_csv_row(payload: dict) -> dict:
    """Converte payload com chaves internas para dict com nomes de coluna CSV."""
    row = {}
    for internal_key, csv_col in _KEY_TO_CSV.items():
        row[csv_col] = _clean_for_csv(payload.get(internal_key, ''))
    return row


def payload_to_json_row(payload: dict) -> dict:
    """Converte payload com chaves internas para dict com nomes de coluna CSV (para JSON)."""
    row = {}
    for internal_key, csv_col in _KEY_TO_CSV.items():
        val = payload.get(internal_key, '')
        row[csv_col] = str(val).strip() if val else ''
    return row
