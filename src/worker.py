"""
Worker simplificado que exporta resultados para CSV
- Prompt customizável
- Estrutura de dados flexível
- Não salva no Supabase (apenas atualiza status)
"""

import time
import json
import os
import tempfile
import threading
import csv
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client
from config import settings
# from gemini_client import GeminiClient
from openai_client import OpenAIClient

# Variáveis globais (inicializadas no main_loop)
# supabase = None # Removed global client
ai_client = None
semaphore = None

# Thread-local storage for Supabase clients
_thread_local = threading.local()

def get_supabase():
    if not hasattr(_thread_local, 'client'):
        _thread_local.client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
    return _thread_local.client

# Arquivo CSV de saída
CSV_OUTPUT = "resultados_analise.csv"

def extract_schema_keys(prompt_text: str) -> list:
    """
    Extrai as chaves do schema JSON do prompt
    Retorna lista de chaves esperadas
    """
    # #region agent log
    # Debug logging removed
    # #endregion
    try:
        # Tentar encontrar chaves listadas explicitamente (mais confiável)
        keys_match = re.search(r'CHAVES OBRIGATÓRIAS[^\n]*\n([^\n]+)', prompt_text)
        # #region agent log
        # Debug logging removed
        # #endregion
        if keys_match:
            keys_str = keys_match.group(1)
            # Extrair chaves separadas por vírgula
            keys = [k.strip() for k in keys_str.split(',')]
            filtered_keys = [k for k in keys if k and not k.startswith('(')]
            # #region agent log
            # Debug logging removed
            # #endregion
            if filtered_keys:
                return filtered_keys
        
        # Procurar por um bloco JSON no prompt (método alternativo)
        # Procurar por { seguido de conteúdo JSON válido
        json_start = prompt_text.find('{')
        if json_start != -1:
            # Encontrar o fechamento correspondente
            bracket_count = 0
            json_end = json_start
            for i in range(json_start, len(prompt_text)):
                if prompt_text[i] == '{':
                    bracket_count += 1
                elif prompt_text[i] == '}':
                    bracket_count -= 1
                    if bracket_count == 0:
                        json_end = i + 1
                        break
            
            if json_end > json_start:
                schema_str = prompt_text[json_start:json_end]
                try:
                    schema = json.loads(schema_str)
                    return list(schema.keys())
                except json.JSONDecodeError:
                    pass
        
        # #region agent log
        # Debug logging removed
        # #endregion
        return []
    except Exception as e:
        # #region agent log
        # Debug logging removed
        # #endregion
        print(f"[AVISO] Erro ao extrair chaves do schema: {e}")
        return []

def find_key_in_dict(data: dict, key: str) -> any:
    """
    Busca uma chave no dicionário, mesmo que esteja aninhada ou achatada
    """
    # Buscar diretamente
    if key in data:
        return data[key]
    
    # Buscar versão achatada (ex: partes_autor)
    for k, v in data.items():
        if k == key or k.endswith(f'_{key}') or k.startswith(f'{key}_'):
            return v
    
    # Buscar em objetos aninhados
    for k, v in data.items():
        if isinstance(v, dict) and key in v:
            return v[key]
    
    return None

def load_prompt_from_db(supabase_client, projeto_id=None):
    """Carrega prompt do Supabase por projeto. Se projeto_id for None, tenta id=1 (legado)."""
    try:
        if projeto_id:
            result = supabase_client.table('prompt_config').select('prompt_text').eq('projeto_id', projeto_id).maybe_single().execute()
        else:
            result = supabase_client.table('prompt_config').select('prompt_text').eq('id', 1).maybe_single().execute()
        # Fix: Check if result is not None before accessing data
        # maybe_single().execute() might return None if no record found
        if result and hasattr(result, 'data') and result.data and result.data.get('prompt_text'):
            return result.data['prompt_text']
    except Exception as e:
        print(f"[AVISO] Erro ao carregar prompt do Supabase: {e}")
    try:
        # Tentar carregar de docs/prompt_custom.txt
        prompt_path = os.path.join("docs", "prompt_custom.txt")
        if not os.path.exists(prompt_path):
             prompt_path = "prompt_custom.txt" # Fallback para raiz
        
        with open(prompt_path, "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        pass
    return None

def flatten_dict(d: dict, parent_key: str = '', sep: str = '_') -> dict:
    """
    Achata um dicionário aninhado para formato CSV
    Ex: {"partes": {"autor": "João"}} -> {"partes_autor": "João"}
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Se for lista, converter para string separada por vírgula
            items.append((new_key, ', '.join(str(item) for item in v) if v else ''))
        else:
            items.append((new_key, v))
    return dict(items)

def save_to_csv(data: dict):
    """
    Salva resultado em CSV com todas as colunas achatadas
    """
    # Achatando objetos aninhados
    flat_data = flatten_dict(data)
    
    # Garantir que arquivo_original sempre existe
    if 'arquivo_original' not in flat_data:
        flat_data['arquivo_original'] = data.get('arquivo_original', 'N/A')
    
    file_exists = os.path.exists(CSV_OUTPUT)
    
    # Ler cabeçalhos existentes se o arquivo já existe
    existing_headers = set()
    if file_exists:
        try:
            with open(CSV_OUTPUT, 'r', encoding='utf-8-sig', newline='') as f:
                reader = csv.DictReader(f)
                if reader.fieldnames:
                    existing_headers = set(reader.fieldnames)
        except Exception:
            pass
    
    # Combinar cabeçalhos existentes com novos
    all_headers = sorted(list(existing_headers.union(flat_data.keys())))
    
    # Se o arquivo não existe, criar com cabeçalho
    if not file_exists:
        with open(CSV_OUTPUT, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=all_headers)
            writer.writeheader()
            # Preencher valores faltantes com vazio
            row = {header: flat_data.get(header, '') for header in all_headers}
            writer.writerow(row)
    else:
        # Adicionar linha ao CSV existente
        with open(CSV_OUTPUT, 'a', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=all_headers)
            # Preencher valores faltantes com vazio
            row = {header: flat_data.get(header, '') for header in all_headers}
            writer.writerow(row)
    
    print(f"   Salvo em: {CSV_OUTPUT} ({len(all_headers)} colunas)")

def apply_regex_fix(raw_text: str, data: dict):
    """
    Aplica correção via Regex para campos de Cobertura quando o texto está "quebrado" (colunar).
    Padrão: SEGURADO: ... TERCEIROS: ... SIM ... SIM
    """
    try:
        if not raw_text: return data
        
        # Regex busca: SEGURADO: (texto qualquer) TERCEIROS: (texto qualquer) (SIM/NÃO) (texto qualquer) (SIM/NÃO)
        # O re.DOTALL faz o . casar com quebras de linha
        # Adicionei \s* para flexibilidade extra
        pattern = r'SEGURADO:.*?TERCEIROS:.*?(\bSIM\b|\bNÃO\b).*?(\bSIM\b|\bNÃO\b)'
        match = re.search(pattern, raw_text, re.DOTALL | re.IGNORECASE)
        
        if match:
            segurado_val = match.group(1).upper()
            terceiros_val = match.group(2).upper()
            
            print(f"   REGEX FIX: Substituindo Cobertura pela leitura direta do texto.")
            print(f"      Segurado: {segurado_val} | Terceiros: {terceiros_val}")
            
            if 'analise_cobertura' not in data: data['analise_cobertura'] = {}
            if isinstance(data['analise_cobertura'], dict):
                data['analise_cobertura']['segurado'] = segurado_val
                data['analise_cobertura']['terceiros'] = terceiros_val
    except Exception as e:
        print(f"[AVISO] Erro no Regex Fix: {e}")
    
    return data

def process_file_task(record):
    """Tarefa individual de processamento"""
    filename = record['filename']
    storage_path = record.get('storage_path')
    doc_id_db = record['id']
    projeto_id = record.get('projeto_id')
    
    with semaphore:  # Garante limite de threads ativas
        tmp_path = None
        try:
            print(f"Processando: {filename}" + (f" [projeto: {str(projeto_id)[:8]}...]" if projeto_id else ""))
            
            # 1. Marcar como PROCESSANDO
            get_supabase().table(settings.TABLE_GERENCIAMENTO).update(
                {"status": "PROCESSANDO", "started_at": "now()"}
            ).eq("id", doc_id_db).execute()

            # 2. Baixar do Supabase Storage
            if not storage_path:
                raise ValueError("Caminho do arquivo no storage não encontrado")

            # Detectar tipo de arquivo
            file_extension = os.path.splitext(storage_path)[1].lower()
            is_excel = file_extension in ['.xlsx', '.xls']
            
            print(f"   Baixando: {storage_path} ({file_extension})")
            
            # 2.1 Download do arquivo
            data = get_supabase().storage.from_("processos").download(storage_path)
            
            # 2.2 Processar de acordo com o tipo
            if is_excel:
                # Excel: converter para texto/JSON
                import pandas as pd
                import io as io_lib
                
                print(f"   Processando Excel...")
                excel_buffer = io_lib.BytesIO(data)
                df = pd.read_excel(excel_buffer)
                
                # Converter DataFrame para texto estruturado
                excel_text = "## Dados do Excel:\n\n"
                excel_text += df.to_string(index=False)
                excel_text += "\n\n## Dados em JSON:\n"
                excel_text += df.to_json(orient='records', indent=2, force_ascii=False)
                
                raw_text = excel_text
                tmp_path = None  # Não precisa de arquivo temporário
            else:
                # PDF: salvar temporariamente
                with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                    tmp.write(data)
                    tmp_path = tmp.name

            # 3. Carregar prompt do projeto (ou legado id=1)
            current_prompt = load_prompt_from_db(get_supabase(), projeto_id)
            if not current_prompt:
                raise ValueError("Prompt não configurado. Configure no frontend ou crie prompt_custom.txt")
            
            print(f"   Enviando para IA...")
            prompt_final = f"""{current_prompt}

REGRAS CRÍTICAS DE EXTRAÇÃO:
1. Retorne APENAS o JSON válido, usando EXATAMENTE as chaves definidas no schema acima.
2. NÃO adicione nem remova chaves.
3. Extraia APENAS informações que estão LITERALMENTE escritas no documento.
4. Se uma informação NÃO estiver explicitamente presente, deixe o campo em branco ("" para strings, null para números/objetos).
5. NUNCA invente, deduza, presuma, ou interprete informações.
6. NUNCA use valores de exemplo ou placeholder.
7. Seja LITERAL - copie o texto exato, não parafraseie."""
            
            # 4. Análise de acordo com o tipo
            if is_excel:
                # Excel: enviar texto diretamente para IA
                print(f"   Enviando Excel para análise...")
                json_text = ai_client._call_openai(raw_text, prompt_final)
            else:
                # PDF: usar analyze_document
                json_text, raw_text = ai_client.analyze_document(tmp_path, prompt_final)
            data_analise = json.loads(json_text)

            # Aplicar Correção Híbrida (Regex sobrepõe AI para campos críticos)
            data_analise = apply_regex_fix(raw_text, data_analise)
            
            # Validação se IA retornou Lista ao invés de Objeto
            if isinstance(data_analise, list):
                if len(data_analise) > 0:
                    print(f"[AVISO] Alerta: IA retornou uma lista ({len(data_analise)} itens). Usando o primeiro item.")
                    data_analise = data_analise[0]
                else:
                    data_analise = {}
            elif not isinstance(data_analise, dict):
                 data_analise = {}
            
            # Extrair chaves esperadas do schema no prompt
            expected_keys = extract_schema_keys(current_prompt)
            
            # Filtrar apenas as chaves esperadas (remover campos extras que a IA pode ter adicionado)
            if expected_keys:
                filtered_data = {}
                for key in expected_keys:
                    # Buscar a chave (pode estar achatada ou aninhada)
                    value = find_key_in_dict(data_analise, key)
                    if value is not None:
                        filtered_data[key] = value
                    else:
                        filtered_data[key] = 'N/A'
                data_analise = filtered_data
                print(f"   Filtrado para {len(expected_keys)} chaves esperadas: {', '.join(expected_keys[:5])}...")
            
            # Apenas nome do arquivo, data de processamento e campos pedidos pelo usuário (sem detalhes do arquivo)
            data_analise['numero_caso'] = record.get('caso_id', 'N/A')
            data_analise['arquivo_original'] = filename
            data_analise['data_processamento'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 4. Salvar em CSV local (DESATIVADO PARA ALTA PERFORMANCE)
            # save_to_csv(data_analise)
            
            # 5. Salvar também no Supabase para exportação via frontend (com projeto_id)
            try:
                insert_payload = {
                    'arquivo_original': filename,
                    'dados_json': data_analise
                }
                if projeto_id:
                    insert_payload['projeto_id'] = projeto_id
                get_supabase().table('resultados_analise').insert(insert_payload).execute()
                print(f"   Resultado salvo no Supabase para exportacao")
            except Exception as db_error:
                print(f"[AVISO] Erro ao salvar no Supabase (CSV local foi salvo): {db_error}")
            
            # 5. Remover arquivo do Storage (bucket)
            try:
                get_supabase().storage.from_("processos").remove([storage_path])
                print(f"   Arquivo removido do Storage")
            except Exception as delete_error:
                print(f"[AVISO] Erro ao remover do bucket (confira RLS DELETE em storage.objects): {delete_error}")
            
            # 6. Remover registro da fila (limpar banco)
            try:
                get_supabase().table(settings.TABLE_GERENCIAMENTO).delete().eq("id", doc_id_db).execute()
                print(f"   Registro removido da fila (documento_gerenciamento)")
            except Exception as db_del_error:
                # Fallback: marcar como CONCLUIDO se delete falhar (ex.: RLS)
                try:
                    get_supabase().table(settings.TABLE_GERENCIAMENTO).update(
                        {"status": "CONCLUIDO", "completed_at": "now()"}
                    ).eq("id", doc_id_db).execute()
                    print(f"   [AVISO] Delete da fila falhou; status atualizado para CONCLUIDO: {db_del_error}")
                except Exception as update_error:
                    print(f"[AVISO] Erro ao atualizar/remover da fila: {update_error}")
            
            print(f"[OK] Sucesso: {filename}")

        except Exception as e:
            print(f"[ERRO] Erro {filename}: {str(e)}")
            import traceback
            traceback.print_exc()
            error_msg = str(e)[:500]  # Limitar tamanho da mensagem
            
            # Salvar erro no banco
            try:
                get_supabase().table(settings.TABLE_GERENCIAMENTO).update(
                    {"status": "ERRO", "error_message": error_msg}
                ).eq("id", doc_id_db).execute()
            except Exception as update_error:
                print(f"[AVISO] Erro ao atualizar status: {update_error}")
            
        finally:
            # Limpar arquivo temporário local
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception as cleanup_error:
                    print(f"[AVISO] Erro ao limpar arquivo temporário: {cleanup_error}")

def main_loop():
    # Inicialização
    global ai_client, semaphore
    
    # supabase = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY) # Global disabled
    
    # ai_client = GeminiClient()
    try:
        ai_client = OpenAIClient()
    except Exception as e:
        print(f"[ERRO] Erro ao iniciar OpenAI: {e}")
        return

    semaphore = threading.Semaphore(settings.MAX_WORKERS)
    
    # Verificar se há prompt configurado (qualquer projeto ou legado)
    test_prompt = load_prompt_from_db(get_supabase(), None)
    if not test_prompt:
        print("[ERRO] Prompt nao configurado!")
        print("   Configure o prompt no frontend ou crie 'prompt_custom.txt'")
        print("   Execute: create_prompt_table.sql no Supabase primeiro")
        return
    
    print("[OK] Worker CSV iniciado!")
    print(f"   - Threads: {settings.MAX_WORKERS}")
    print(f"   - Arquivo de saída: {CSV_OUTPUT}")
    print(f"   - Prompt: Carregado do Supabase (atualizado dinamicamente)")
    print(f"   - Modelo: {ai_client.model_name}")
    print()
    
    executor = ThreadPoolExecutor(max_workers=settings.MAX_WORKERS)
    TABLE_PROCESSAR_AGORA = "processar_agora"
    
    while True:
        try:
            # 1. Watchdog: Reset jobs stuck in PROCESSANDO for > 15 min
            from datetime import timedelta
            timeout_limit = datetime.now() - timedelta(minutes=15)
            
            try:
                # Reset stuck documents
                get_supabase().table(settings.TABLE_GERENCIAMENTO).update(
                    {"status": "PENDENTE", "error_message": "Watchdog: Reset após timeout (15min)"}
                ).eq("status", "PROCESSANDO") \
                 .lt("started_at", timeout_limit.isoformat()) \
                 .execute()
            except Exception as w_err:
                print(f"[WATCHDOG] Erro ao verificar timeouts: {w_err}")

            # 2. Check Triggers (Priority Boost)
            trigger_resp = get_supabase().table(TABLE_PROCESSAR_AGORA).select("id, projeto_id").limit(1).execute()
            triggers = trigger_resp.data or []
            
            if not triggers:
                # No triggers -> sleep and wait
                time.sleep(5)
                continue
            
            for trigger in triggers:
                projeto_id = trigger.get("projeto_id")
                print(f"[WORKER] Trigger detectado para projeto: {projeto_id}")
                
                # LOOP DO PROJETO: Processa até acabar os pendentes deste projeto
                empty_cycles = 0
                while True:
                    try:
                        # Busca lote de PENDENTES
                        query = get_supabase().table(settings.TABLE_GERENCIAMENTO).select("*") \
                            .eq("status", "PENDENTE").not_.is_("storage_path", "null") \
                            .limit(settings.MAX_WORKERS)
                        
                        if projeto_id:
                            query = query.eq("projeto_id", projeto_id)
                            
                        response = query.execute()
                        files = response.data or []
                        
                        if not files:
                            empty_cycles += 1
                            if empty_cycles >= 2: # Confirm twice before exiting
                                print(f"[WORKER] Fim da fila para projeto {projeto_id}. Removendo trigger.")
                                get_supabase().table(TABLE_PROCESSAR_AGORA).delete().eq("id", trigger["id"]).execute()
                                break
                            time.sleep(2)
                            continue
                            
                        empty_cycles = 0
                        print(f"[WORKER] Processando lote de {len(files)} arquivos...")

                        # CRITICAL FIX: Mark as PROCESSANDO immediately in main thread to prevent duplicates
                        file_ids = [f['id'] for f in files]
                        try:
                            get_supabase().table(settings.TABLE_GERENCIAMENTO).update(
                                {"status": "PROCESSANDO", "started_at": "now()"}
                            ).in_("id", file_ids).execute()
                        except Exception as update_err:
                            print(f"[ERRO] Falha ao marcar lote como PROCESSANDO: {update_err}")
                            time.sleep(1)
                            continue
                        
                        futures = []
                        for file_record in files:
                            futures.append(executor.submit(process_file_task, file_record))
                        
                        # Wait for batch completion to manage memory? 
                        # No, let the semaphore handle concurrency. 
                        # Just ensure we don't flood the queue faster than processing.
                        while executor._work_queue.qsize() > settings.MAX_WORKERS * 2:
                            time.sleep(1)
                            
                    except Exception as loop_error:
                        print(f"[ERRO] Erro no loop do projeto: {loop_error}")
                        time.sleep(5)
            
            time.sleep(1)

        except KeyboardInterrupt:
            print("\nWorker interrompido pelo usuario")
            executor.shutdown(wait=True)
            break
        except Exception as e:
            print(f"[AVISO] Erro no loop principal: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)

if __name__ == "__main__":
    main_loop()
