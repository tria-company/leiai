"""
pdf_extractor.py — Extração de campos de formulários PDF.

MUDANÇAS vs original:
  - pdfplumber substituído por PyMuPDF (fitz) → 5-10x mais rápido
  - OCR recebe rate_limiter para thread safety
  - Todas as ~170 linhas de regex são IDÊNTICAS ao original
"""

import re
import base64

import fitz  # PyMuPDF

from .config import MIN_TEXT_LEN, _STOP, OCR_MODEL, OCR_MAX_PAGES, OCR_ZOOM_FACTOR
from .schemas import empty_payload


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_markdown(md: str) -> str:
    """Remove formatação markdown e limpa texto para extração com regex."""
    lines = md.split('\n')
    cleaned = []
    for line in lines:
        if re.match(r'^[\s|:-]+$', line):
            continue
        line = line.replace('|', ' ')
        line = re.sub(r'^#{1,6}\s*', '', line)
        line = re.sub(r'\*{1,2}([^*]+)\*{1,2}', r'\1', line)
        line = re.sub(r'\s{2,}', ' ', line).strip()
        if line:
            cleaned.append(line)
    text = '\n'.join(cleaned)
    text = re.sub(r'([a-záéíóúãõâêîôûç])([A-ZÁÉÍÓÚÃÕÂÊÎÔÛÇ])', r'\1 \2', text)
    text = re.sub(r'([A-Za-zÀ-ÿ]):', r'\1: ', text)
    return text


def _ocr_with_gpt4o_vision(filepath: str, api_key: str, rate_limiter=None) -> str:
    """OCR de PDFs baseados em imagem via GPT-4o-mini Vision.

    Renderiza todas as páginas como imagem e envia TUDO em uma única chamada de API.
    """
    import openai

    if not api_key:
        raise ValueError("OPENAI_API_KEY não encontrada em leiai/.env")

    client = openai.OpenAI(api_key=api_key, timeout=120)
    doc = fitz.open(filepath)
    max_pages = min(len(doc), OCR_MAX_PAGES)
    print(f"    [OCR Vision] Renderizando {max_pages} página(s)...", flush=True)

    # Renderiza todas as páginas de uma vez
    mat = fitz.Matrix(OCR_ZOOM_FACTOR, OCR_ZOOM_FACTOR)
    content_parts = [
        {
            "type": "text",
            "text": (
                "Extraia TODO o texto destas imagens de formulário exatamente como está escrito, "
                "preservando a estrutura, campos e quebras de linha. "
                "Retorne apenas o texto extraído, sem comentários adicionais."
            )
        }
    ]

    total_kb = 0
    for i in range(max_pages):
        page = doc[i]
        pix = page.get_pixmap(matrix=mat)
        img_bytes = pix.tobytes("png")
        total_kb += len(img_bytes) // 1024
        img_b64 = base64.b64encode(img_bytes).decode()
        content_parts.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/png;base64,{img_b64}",
                "detail": "auto"
            }
        })
    doc.close()

    print(f"    [OCR Vision] {max_pages} página(s), {total_kb}KB total, enviando 1 chamada...", flush=True)

    if rate_limiter:
        rate_limiter.wait()

    try:
        response = client.chat.completions.create(
            model=OCR_MODEL,
            messages=[{"role": "user", "content": content_parts}],
            temperature=0.0,
            max_tokens=4096 * max_pages,
        )
        result = response.choices[0].message.content or ""
    except Exception as e:
        print(f"    [OCR Vision] Erro: {e}", flush=True)
        result = ""

    print(f"    [OCR Vision] Concluído: {len(result)} chars extraídos.", flush=True)
    return result


# ---------------------------------------------------------------------------
# Regex pré-compilados (compilados 1x no import, usados N vezes)
# ---------------------------------------------------------------------------
_DI = re.DOTALL | re.IGNORECASE
_I = re.IGNORECASE

# Cobertura
_RE_COB_BOTH = re.compile(r'SEGURADO:.*?TERCEIROS:.*?\n\s*(SIM|N[ÃA]O)\s+(SIM|N[ÃA]O)', re.DOTALL)
_RE_COB_SEG = re.compile(r'SEGURADO:(?:\s*\n?\s*|[^\n]*\n\s*)(SIM|N[ÃA]O)', _I)
_RE_COB_TER = re.compile(r'TERCEIROS:\s*\n?\s*(SIM|N[ÃA]O)', _DI)
_RE_COB_SEG2 = re.compile(r'AN[AÁ]LISE\s+DE\s+COBERTURA.*?SEGURADO:\s*\n?\s*(SIM|N[ÃA]O)', _DI)

# Identificação
_RE_PROT_SEG = re.compile(r'PROTOC\w*[^:]*SEGURADO:\s*(\d+)', _DI)
_RE_PROT_SEG2 = re.compile(r'PROTOC\w*\s+DO\s+SEGURADO:\s*(\d+)', _DI)
_RE_PLACA_SEG = re.compile(r'PLACA\s+(?:DO\s+)?(?:VE[IÍ]CULO|SEGURADO):\s*([A-Z0-9]+)', _DI)
_RE_PROT_TER = re.compile(r'PROTOC\w*[^:]*TERCEIRO:\s*(\d+)', _DI)
_RE_PLACA_TER = re.compile(r'PLACA\s+(?:DO\s+)?TERCEIRO:\s*([A-Z0-9]+)', _DI)

# Veículo
_RE_CLIENTE = re.compile(r'CLIENTE/(?:CHAVE\s+NATURAL|UNIDADE):\s*(.+?)(?:\n|$)', _DI)
_RE_VEICULO = re.compile(r'VE[IÍ]CULO:\s*(.+?)(?:\n|TITULAR|$)', _DI)
_RE_APOLICE = re.compile(r'AP[OÓ]LICE[^:]*:\s*(.+?)(?:\n|$)', _DI)

# Franquia
_RE_TITULAR = re.compile(r'(PESSOA\s+F[IÍ]SICA|PESSOA\s+JUR[IÍ]DICA)', _DI)
_RE_FIPE1 = re.compile(r'R\$\s*FIPE\s+M[EÊ]S\s+FATO:?\s*(R?\$?\s*[\d.,]+)', _DI)
_RE_FIPE2 = re.compile(r'PESSOA\s+F[IÍ]SICA\s+(R\$\s*[\d.,]+)', _DI)
_RE_GRUPO = re.compile(rf'GRUPO:\s*\n?\s*(.*?)(?=[ÚU]NICA\b|TIPO\b|{_STOP})', _DI)
_RE_TIPO_FRANQ = re.compile(r'TIPO\s+DE\s+FRANQUIA:\s*\n?\s*(\w+)', _DI)
_RE_FRANQ_VAL = re.compile(r'FRANQUIA\s*\n?\s*(?:R\$\s*[\d.,]+\s*\n?\s*)?(\d+)\s+(R\$\s*[\d.,]+)', _I)
_RE_FRANQ_VAL2 = re.compile(r'(R\$\s*[\d.,]+)\s*$', _DI)

# Cobertura sinistro
_RE_DATA_FATO = re.compile(r'DATA\s+DO\s+(?:FATO|ACIDENTE|SINISTRO):\s*([\d/]+)', _DI)
_RE_HORA_FATO = re.compile(r'HORA\s+(?:DO\s+(?:FATO|BO|SINISTRO)|DO\s+FATO):\s*([\d:]+)', _DI)
_RE_HORA_BO = re.compile(r'HORA\s+DO\s+BO:\s*([\d:]+)', _DI)
_RE_DIA_SEMANA = re.compile(r'DIA\s+DA\s+SEMANA:\s*(.+?)(?:\n|$)', _DI)
_RE_DATA_REG_BO = re.compile(r'DATA\s+REGISTRO\s+(?:DO\s+)?BO:\s*([\d/]+)', _DI)
_RE_HORA_REG = re.compile(r'HORA\s+REG(?:ISTRO)?:\s*([\d:]+)', _DI)
_RE_NUM_BO = re.compile(r'N[ºo°]\s*B\.?O\.?\s*([\d/]+)', _DI)
_RE_TIPO_BO = re.compile(r'TIPO\s+(?:B\.?O\.?|BO)[\s:]*(\w+)', _DI)

# Financeiro
_RE_VENC_ERP = re.compile(r'V[EI]NC[.\s]*(?:ERP)?[:\s]*([\d/]+)', _DI)
_RE_DATA_PGT = re.compile(r'DATA\s+PGT:\s*([\d/]+)', _DI)
_RE_TIPO_PGT = re.compile(r'TIPO:\s*(\w+)', _DI)
_RE_COBERTO = re.compile(r'EST[AE]\s+COBERT[OA]\??\s*(SIM|NAO|NÃO|N[ÃA]O)', _DI)

# Assistência
_RE_ASSIST = re.compile(r'ASSIST[EÊ]NCIA\s+24\s*H(?:RS)?:\s*(SIM|NAO|NÃO|N[ÃA]O)', _DI)

# Detran
_RE_CNH_STATUS = re.compile(r'(?:CNH|CIV):\s*(REGULAR|IRREGULAR|SUSPENSA|CASSADA)', _DI)
_RE_CNH_CAT = re.compile(r'CATEGORIA:\s*([A-E]+)', _DI)
_RE_CNH_OBS = re.compile(r'(?:OBS|CNR):\s*(\w+)\s+VALIDADE', _DI)
_RE_CNH_VAL = re.compile(r'VALIDADE:\s*([\d/]+)', _DI)
_RE_CNH_PONTOS = re.compile(r'(?:PTOS|PDTO):\s*(\w+)', _DI)
_RE_DETALH_RESTR = re.compile(rf'DETALHAMENTO\s+DE\s+RESTRI[ÇC][AÃ]O\s+D[AE]\s*(?:CNH|C/?N)?:?\s*(.+?)(?=CRLV|CIV|{_STOP})', _DI)
_RE_CRLV_ANO = re.compile(r'(?:CRLV|CIV)\s*(?:\(?\s*ANO\s*\)?\s*)?:\s*(\d{4})', _DI)
_RE_IPVA = re.compile(r'(?:IPVA/LICENC\.?|P/PLACA):\s*(\w+)', _DI)
_RE_MULTAS = re.compile(r'MULTAS:\s*(\w+)', _DI)
_RE_UF = re.compile(r'UF[I]?:\s*([A-Z]{2})', _DI)
_RE_MULTA_SIN = re.compile(r'MULTA\s*X?\s*SINISTRO:\s*(SIM|NAO|NÃO|N[ÃA]O|X)', _DI)
_RE_RESTRICOES = re.compile(r'RESTRI[ÇC][OÕ]ES:\s*(\w+)', _DI)

# Detalhamento
_RE_LOCAL_URL = re.compile(r'LOCAL\s+D[OE]\s+(?:FATO|ACIDENTE|SINISTRO):\s*(https?://\S+)', _DI)
_RE_RELATO1 = re.compile(r'RELATO\s+DO\s+(?:FATO\s+EM\s+)?BO[^:]*:?\s*\n?(.+?)(?:\nRESSARCIMENTO|\nPONTOS\s+A\s+EXALTAR)', _DI)
_RE_RELATO2 = re.compile(r'Descri[çc][aã]o\s+ocorr[eê]ncia[^:]*:\s*(.+?)(?:Hist[oó]rico|RESSARCIMENTO)', _DI)
_RE_RESSARC = re.compile(r'RESSARCIMENTO:\s*(SIM|NAO|NÃO|N[ÃA]O)', _DI)
_RE_ITEM_REG1 = re.compile(r'ITEM\s+DO\s+REGULAMENTO\s+PARA\s+SEGURADO:?\s*(.+?)(?=ART\.?\s*CTB|ITEM\s+DO\s+REGULAMENTO\s+PARA\s+TERCEIRO|SOLICITADO)', _DI)
_RE_ITEM_REG2 = re.compile(r'ITEM\s+DO\s+REGULAMENTO\s+PARA\s*\n(.*?)(?:SEGURADO|SOLICITADO)', _DI)
_RE_ART_CTB = re.compile(r'ART\.?\s*CTB:\s*([\d\s,Ee]+)', _I)
_RE_SINDIC = re.compile(r'SOLICITADO\s+SINDIC[AÂ]NCIA/PER[IÍ]CIA:\s*(SIM|NAO|NÃO|N[ÃA]O)', _DI)
_RE_RESP_SINDIC = re.compile(r'RESPONS[AÁ]VEL:\s*(\w+)', _DI)
_RE_RESULT_SINDIC = re.compile(r'RESULTADO\s+DA\s+SINDIC[AÂ]NCIA:\s*(\w+)', _DI)
_RE_PONTOS = re.compile(rf'PONTOS\s+A\s+EXALTAR[^:]*:\s*(.+?)(?={_STOP})', _DI)
_RE_RESUMO = re.compile(rf'RESUMO\s+DO\s+(?:FATO|OCORRIDO)\s*:?\s*\n(.+?)(?=PARECER|AVARIAS|AVALIA[CÇ]|{_STOP})', _DI)
_RE_PARECER1 = re.compile(rf'PARECER\s+[AÀ]\s+RE[GS]ULA[GR][EAI]\w*:\s*(.+?)(?=AVALIA[CÇ]|{_STOP})', _DI)
_RE_PARECER2 = re.compile(rf'PARECER\s+[AÀ]\s+\w+:\s*(.+?)(?=AVALIA[CÇ]|CONCLUS|{_STOP})', _DI)

# Conclusão
_RE_CONCL_SEG = re.compile(rf'CONCLUS[AÃ]O\s+D[AOE]\s+AN[AÁ]LIS[EI]S?\s+(?:DO\s+)?SEGURAD[OA]?:?\s*\n?(.+?)(?=DANOS|DADOS|CONCLUS[AÃ]O\s+D[AOE]\s+AN[AÁ]LIS|{_STOP})', _DI)
_RE_CONCL_SEG2 = re.compile(rf'CONCLUS[AÃ]O\s+D[AOE]\s+AN[AÁ]LIS[EI]S?\s+(?:DO\s+)?SINISTRO:?\s*\n?(.+?)(?=DANOS|DADOS|{_STOP})', _DI)
_RE_DANOS_SEG = re.compile(rf'(?:DANOS|DADOS)\s+(?:DO\s+)?VE[IÍ]CULO\s+SEGURADO:\s*(.+?)(?=CLASSIF|MONTA|(?:DANOS|DADOS)\s+(?:DO\s+)?VE[IÍ]CULO\s+TERCEIRO|{_STOP})', _DI)
_RE_CLASSIF = re.compile(r'CLASSIFIC.{1,5}O\s+NO\s+(?:FATO|RATO|FAT0):\s*(\w+)', _DI)
_RE_MONTA = re.compile(r'MONTA:\s*(.+?)(?:\n|$)', _DI)
_RE_ANALISTA = re.compile(rf'ANALISTA\s+RESPONS[AÁ]VEL[^:]*:\s*(.+?)(?=IN[IÍ]CIO|N[ºo°°]|NUM|{_STOP})', _DI)
_RE_INICIO = re.compile(r'(?:IN[IÍ]CIO|N[ºo°]\s*D[AE])\s+AN[AÁ]LIS[EI]:\s*([\d/]+)', _DI)
_RE_DATAS_ANALISE = re.compile(r'IN[IÍ]CIO\s+AN[AÁ]LISE:\s*([\d/]+)\s+([\d/]+)', _I)
_RE_CONCL_ANALISE = re.compile(r'CONCLUS[AÃ]O\s+AN[AÁ]LISE:\s*([\d/]+)', _DI)
_RE_DATAS_SINISTRO = re.compile(r'SINISTRO\s+ABERTO\s+EM:\s*([\d/]+)\s+([\d/]+)\s+([\d/]+)', _I)
_RE_SIN_ABERTO = re.compile(r'SINISTRO\s+ABERTO\s+EM:\s*([\d/]+)', _DI)
_RE_AN_RECEB = re.compile(r'AN[AÁ]LISE\s+RECEBIDA\s+EM:\s*([\d/]+)', _DI)
_RE_AN_LIBER = re.compile(r'AN[AÁ]LISE\s+LIBERADA\s+EM:\s*([\d/]+)', _DI)
_RE_OBS = re.compile(r'OBSERVA[CÇ][OÕ]ES\s+(.+?)(?:CASO DE IDENTIF|$)', _DI)


# ---------------------------------------------------------------------------
# Extração principal
# ---------------------------------------------------------------------------

def extract_pdf(filepath: str, api_key: str = '', rate_limiter=None, error_log: list = None) -> dict:
    """Extrai campos de um formulário PDF usando PyMuPDF (com fallback GPT-4o Vision para OCR)."""

    p = empty_payload()
    full_text = ''

    # --- Extração de texto com PyMuPDF (rápido, thread-safe) ---
    doc = fitz.open(filepath)
    full_text = "\n".join(page.get_text() for page in doc)
    doc.close()

    # Fallback: se PyMuPDF extraiu pouco texto, o PDF provavelmente é imagem
    if len(full_text.strip()) < MIN_TEXT_LEN:
        print(f"    [OCR] fitz extraiu pouco texto ({len(full_text.strip())} chars), usando GPT-4o Vision...", flush=True)
        try:
            full_text = _ocr_with_gpt4o_vision(filepath, api_key, rate_limiter)
            print(f"    [OCR] GPT-4o Vision extraiu {len(full_text.strip())} chars", flush=True)
        except Exception as e:
            msg = f"Falha no OCR GPT-4o Vision: {e}"
            print(f"    [OCR] {msg}", flush=True)
            if error_log is not None:
                error_log.append({
                    'arquivo': filepath,
                    'erro': msg,
                    'tipo': 'ocr',
                })

    full_text = _strip_markdown(full_text)

    def search(pat, text=full_text, group=1):
        m = pat.search(text)
        return m.group(group).strip() if m else ''

    # ===================================================================
    # Regex de extração (pré-compilados no nível do módulo)
    # ===================================================================

    # Cobertura
    m_cob = _RE_COB_BOTH.search(full_text)
    if m_cob:
        p['cobertura_segurado'] = m_cob.group(1)
        p['cobertura_terceiros'] = m_cob.group(2)
    else:
        p['cobertura_segurado'] = search(_RE_COB_SEG)
        p['cobertura_terceiros'] = search(_RE_COB_TER)
    if not p['cobertura_segurado']:
        m_cob2 = _RE_COB_SEG2.search(full_text)
        if m_cob2:
            p['cobertura_segurado'] = m_cob2.group(1)

    # Identificação
    p['protocolo_segurado'] = search(_RE_PROT_SEG)
    if not p['protocolo_segurado']:
        p['protocolo_segurado'] = search(_RE_PROT_SEG2)
    p['placa_segurado'] = search(_RE_PLACA_SEG)
    p['protocolo_terceiro'] = search(_RE_PROT_TER)
    p['placa_terceiro'] = search(_RE_PLACA_TER)

    # Veículo
    p['cliente_chave_natural'] = search(_RE_CLIENTE)
    p['veiculo'] = search(_RE_VEICULO)
    p['num_apolice_seguro'] = search(_RE_APOLICE)

    # Franquia
    p['titular_crlv'] = search(_RE_TITULAR)
    p['fipe_mes_fato'] = search(_RE_FIPE1)
    if not p['fipe_mes_fato']:
        p['fipe_mes_fato'] = search(_RE_FIPE2)
    p['grupo'] = search(_RE_GRUPO)
    p['tipo_franquia'] = search(_RE_TIPO_FRANQ)
    p['franquia_valor'] = search(_RE_FRANQ_VAL)
    if p['franquia_valor']:
        m = _RE_FRANQ_VAL.search(full_text)
        if m:
            p['franquia_numero'] = m.group(1)
            p['franquia_valor'] = m.group(2)
    else:
        p['franquia_valor'] = search(_RE_FRANQ_VAL2)

    # Cobertura sinistro
    p['data_fato'] = search(_RE_DATA_FATO)
    p['hora_fato'] = search(_RE_HORA_FATO)
    if not p['hora_fato']:
        p['hora_fato'] = search(_RE_HORA_BO)
    p['dia_semana'] = search(_RE_DIA_SEMANA)
    p['data_registro_bo'] = search(_RE_DATA_REG_BO)
    p['hora_registro_bo'] = search(_RE_HORA_REG)
    p['numero_bo'] = search(_RE_NUM_BO)
    p['tipo_bo'] = search(_RE_TIPO_BO)

    # Financeiro
    p['vencimento_erp'] = search(_RE_VENC_ERP)
    p['data_pagamento'] = search(_RE_DATA_PGT)
    p['tipo_pagamento'] = search(_RE_TIPO_PGT)
    p['esta_coberto'] = search(_RE_COBERTO)

    # Assistência
    p['assistencia_24hrs'] = search(_RE_ASSIST)

    # Detran
    p['cnh_status'] = search(_RE_CNH_STATUS)
    p['cnh_categoria'] = search(_RE_CNH_CAT)
    p['cnh_obs'] = search(_RE_CNH_OBS)
    p['cnh_validade'] = search(_RE_CNH_VAL)
    p['cnh_pontos'] = search(_RE_CNH_PONTOS)
    p['detalhamento_restricao_cnh'] = search(_RE_DETALH_RESTR)
    p['crlv_ano'] = search(_RE_CRLV_ANO)
    p['ipva_licenciamento'] = search(_RE_IPVA)
    p['multas'] = search(_RE_MULTAS)
    p['uf'] = search(_RE_UF)
    p['multa_sinistro'] = search(_RE_MULTA_SIN)
    p['restricoes'] = search(_RE_RESTRICOES)

    # Detalhamento
    p['local_fato_url'] = search(_RE_LOCAL_URL)
    m_relato = _RE_RELATO1.search(full_text)
    if m_relato:
        p['relato_fato_bo'] = m_relato.group(1).strip()
    else:
        m_relato = _RE_RELATO2.search(full_text)
        if m_relato:
            p['relato_fato_bo'] = m_relato.group(1).strip()
    p['ressarcimento'] = search(_RE_RESSARC)
    m_reg = _RE_ITEM_REG1.search(full_text)
    if not m_reg:
        m_reg = _RE_ITEM_REG2.search(full_text)
    if m_reg:
        val = m_reg.group(1).strip()
        val = re.sub(r'\s*ART\.?\s*CTB:.*$', '', val, flags=re.DOTALL).strip()
        if val:
            p['item_regulamento_segurado'] = val
    p['art_ctb_segurado'] = search(_RE_ART_CTB)
    m = _RE_ART_CTB.findall(full_text)
    if len(m) >= 1:
        p['art_ctb_segurado'] = m[0].strip()
    if len(m) >= 2:
        p['art_ctb_terceiros'] = m[1].strip()
    p['solicitado_sindicancia'] = search(_RE_SINDIC)
    p['responsavel_sindicancia'] = search(_RE_RESP_SINDIC)
    p['resultado_sindicancia'] = search(_RE_RESULT_SINDIC)
    p['pontos_exaltar_analista'] = search(_RE_PONTOS)
    p['resumo_fato'] = search(_RE_RESUMO)
    p['parecer_regulagem'] = search(_RE_PARECER1)
    if not p['parecer_regulagem']:
        p['parecer_regulagem'] = search(_RE_PARECER2)

    # Conclusão
    p['conclusao_segurado'] = search(_RE_CONCL_SEG)
    if not p['conclusao_segurado']:
        p['conclusao_segurado'] = search(_RE_CONCL_SEG2)
    p['danos_veiculo_segurado'] = search(_RE_DANOS_SEG)
    p['classificacao_fato_segurado'] = search(_RE_CLASSIF)
    p['monta_segurado'] = search(_RE_MONTA)
    p['analista_responsavel_segurado'] = search(_RE_ANALISTA)
    p['inicio_analise'] = search(_RE_INICIO)
    m_datas_analise = _RE_DATAS_ANALISE.search(full_text)
    if m_datas_analise:
        p['inicio_analise'] = m_datas_analise.group(1)
        p['conclusao_analise'] = m_datas_analise.group(2)
    else:
        p['conclusao_analise'] = search(_RE_CONCL_ANALISE)
    m_datas_sinistro = _RE_DATAS_SINISTRO.search(full_text)
    if m_datas_sinistro:
        p['sinistro_aberto_em'] = m_datas_sinistro.group(1)
        p['analise_recebida_em'] = m_datas_sinistro.group(2)
        p['analise_liberada_em'] = m_datas_sinistro.group(3)
    else:
        p['sinistro_aberto_em'] = search(_RE_SIN_ABERTO)
        p['analise_recebida_em'] = search(_RE_AN_RECEB)
        p['analise_liberada_em'] = search(_RE_AN_LIBER)
    p['observacoes'] = search(_RE_OBS)

    return p
