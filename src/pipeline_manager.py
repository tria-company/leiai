import time
import threading
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client
from config import settings
from salesforce_client import SalesforceClient
from zip_processor import ZipProcessor

# Globals
# supabase = None # Removed global client
sf_client = None
zip_processor = None
semaphore = None

# Thread-local storage for Supabase clients
_thread_local = threading.local()

def get_supabase():
    if not hasattr(_thread_local, 'client'):
        _thread_local.client = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY)
    return _thread_local.client

def process_case_task(case_record):
    """
    1. Fetch URL from Salesforce
    2. Process ZIP
    3. Update Status
    """
    case_id = case_record['id']
    case_number = case_record['numero_caso']
    
    with semaphore:
        try:
            print(f"Iniciando Caso: {case_number}")
            
            print(f"Iniciando Caso: {case_number}")
            
            # CRITICAL: Mark as PROCESSANDO immediately to prevent duplicate processing
            update_result = get_supabase().table(settings.TABLE_CASOS).update(
                {"status": "PROCESSANDO", "updated_at": "now()"}
            ).eq("id", case_id).eq("status", "PENDENTE").execute()
            
            # If no rows updated, another thread already grabbed this case
            if not update_result.data or len(update_result.data) == 0:
                print(f"   Caso {case_number} ja esta sendo processado por outra thread, pulando.")
                return
            
            # 1. Fetch ALL ZIP URLs
            print(f"   Buscando caso {case_number} na API...")
            zip_urls = sf_client.get_case_zip_urls(case_number)
            
            if not zip_urls:
                print(f"   [AVISO] Nenhum ZIP encontrado para caso {case_number}.")
            if not zip_urls:
                print(f"   [AVISO] Nenhum ZIP encontrado para caso {case_number}.")
                get_supabase().table(settings.TABLE_CASOS).update({
                    "status": "CONCLUIDO",
                    "error_message": "Nenhum arquivo ZIP disponivel",
                    "updated_at": "now()"
                }).eq("id", case_id).execute()
                return
            
            print(f"   [OK] {len(zip_urls)} ZIP(s) encontrado(s)")
            
            # Use dedicated Salesforce project ID if none provided
            projeto_id = case_record.get('projeto_id') or settings.SALESFORCE_PROJECT_ID
            
            # 2. Process each ZIP
            total_processed = 0
            total_failed = 0
            total_skipped = 0
            errors = []
            
            for idx, zip_url in enumerate(zip_urls, 1):
                try:
                    print(f"\n   === Processando ZIP {idx}/{len(zip_urls)} ===")
                    print(f"   URL: {zip_url[:50]}...")
                    
                    # Update DB status
                    get_supabase().table(settings.TABLE_CASOS).update({
                        "zip_url": zip_url,
                        "status": "PROCESSA_ZIP"
                    }).eq("id", case_id).execute()
                    
                    result = zip_processor.process_zip_url(
                        case_number,
                        zip_url,
                        case_id,
                        projeto_id=projeto_id
                    )
                    
                    if result['success']:
                        print(f"   [OK] ZIP {idx} processado com sucesso!")
                        total_processed += 1
                    else:
                        error_msg = result.get('error', 'Erro desconhecido')
                        
                        # Distinguish between real errors and "no files found" warnings
                        if "Nenhum arquivo" in error_msg and ("ANALISE" in error_msg or "PDF ou Excel" in error_msg):
                            print(f"   [AVISO] ZIP {idx}: {error_msg}")
                            total_skipped += 1
                        else:
                            print(f"   [ERRO] ZIP {idx} falhou: {error_msg}")
                            total_failed += 1
                            errors.append(f"ZIP {idx}: {error_msg}")
                        
                except Exception as zip_error:
                    print(f"[ERRO] Erro processando ZIP {idx}: {zip_error}")
                    total_failed += 1
                    errors.append(f"ZIP {idx}: {str(zip_error)}")
            
            # 3. Update final status
            if total_failed == 0:
                if total_skipped > 0:
                    print(f"\n[OK] Caso {case_number}: {total_processed} ZIP(s) processado(s), {total_skipped} ZIP(s) sem arquivos relevantes")
                else:
                    print(f"\n[OK] Caso {case_number}: {total_processed} ZIP(s) processado(s) com sucesso!")
                get_supabase().table(settings.TABLE_CASOS).update({
                    "status": "CONCLUIDO",
                    "updated_at": "now()",
                    "error_message": None
                }).eq("id", case_id).execute()
            else:
                error_summary = f"{total_processed} sucesso, {total_failed} erro(s)"
                if errors:
                    error_summary += ": " + "; ".join(errors[:2])
                print(f"\n[AVISO] Caso {case_number}: {error_summary}")
                get_supabase().table(settings.TABLE_CASOS).update({
                    "status": "CONCLUIDO" if total_processed > 0 else "ERRO",
                    "updated_at": "now()",
                    "error_message": error_summary[:500]
                }).eq("id", case_id).execute()

        except Exception as e:
            error_msg = str(e)[:1000]
            print(f"[ERRO] Caso {case_number}: {error_msg}")
            
            # Check if it's a server disconnection error
            is_server_disconnect = "Server disconnected" in error_msg or "RemoteProtocolError" in error_msg
            
            # Fetch current case from DB to get retry_count
            try:
                current_case = get_supabase().table(settings.TABLE_CASOS).select("retry_count").eq("id", case_id).single().execute()
                current_retry_count = current_case.data.get('retry_count', 0) if current_case.data else 0
            except:
                current_retry_count = 0
            
            if is_server_disconnect and current_retry_count < 3:
                # Re-queue for retry
                new_retry_count = current_retry_count + 1
                print(f"   🔄 Erro de conexão detectado. Recolocando na fila (tentativa {new_retry_count}/3)...")
                get_supabase().table(settings.TABLE_CASOS).update({
                    "status": "PENDENTE",
                    "retry_count": new_retry_count,
                    "updated_at": "now()",
                    "error_message": f"Retry {new_retry_count}/3: {error_msg[:300]}"
                }).eq("id", case_id).execute()
                print(f"   [OK] Caso recolocado na fila para nova tentativa.")
            else:
                # Final error - exceeded retries or different error type
                if is_server_disconnect:
                    error_msg = f"Falhou após 3 tentativas: {error_msg}"
                print(f"   Marcando caso como ERRO no banco...")
                get_supabase().table(settings.TABLE_CASOS).update({
                    "status": "ERRO",
                    "error_message": error_msg,
                    "updated_at": "now()"
                }).eq("id", case_id).execute()
                print(f"   [OK] Status de erro registrado.")

def main_loop():
    global sf_client, zip_processor, semaphore
    
    # Initialize
    # supabase = create_client(settings.SUPABASE_URL, settings.SUPABASE_KEY) # Global disabled
    sf_client = SalesforceClient()
    zip_processor = ZipProcessor()
    
    # Max concurrent imports (don't kill Salesforce API)
    # 5-10 is reasonable.
    start_workers = 5 
    semaphore = threading.Semaphore(start_workers)
    executor = ThreadPoolExecutor(max_workers=start_workers)
    
    print(f"[OK] Pipeline Salesforce Iniciado! ({start_workers} threads)")
    
    # Iniciar Worker de Análise em paralelo
    import worker
    print("Iniciando Worker de Analise (AI) em background...")
    ai_thread = threading.Thread(target=worker.main_loop, daemon=True)
    ai_thread.start()
    
    print(f"   Aguardando casos na tabela '{settings.TABLE_CASOS}'...")

    # Watchdog loop
    while True:
        try:
            # 1. Watchdog: Reset cases stuck for > 20 min
            try:
                from datetime import timedelta, datetime
                timeout_limit = datetime.now() - timedelta(minutes=20)
                
                get_supabase().table(settings.TABLE_CASOS).update(
                    {"status": "PENDENTE", "error_message": "Watchdog: Reset após timeout (20min)"}
                ).in_("status", ["PROCESSANDO", "PROCESSA_ZIP", "BAIXANDO"]) \
                 .lt("updated_at", timeout_limit.isoformat()) \
                 .execute()
            except Exception as w_err:
                print(f"[WATCHDOG] Erro: {w_err}")

            # 2. Poll PENDING cases
            response = get_supabase().table(settings.TABLE_CASOS)\
                .select("*")\
                .eq("status", "PENDENTE")\
                .limit(start_workers)\
                .execute()
                
            cases = response.data if response.data else []
            
            if not cases:
                time.sleep(5)
                continue
            
            print(f"Encontrados {len(cases)} casos pendentes.")
            for case in cases:
                executor.submit(process_case_task, case)
            
            time.sleep(2)
            
        except KeyboardInterrupt:
            print("\nPipeline interrompido.")
            executor.shutdown(wait=True)
            break
        except Exception as e:
            print(f"[AVISO] Erro no loop principal: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main_loop()
