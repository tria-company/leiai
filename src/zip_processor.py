import io
import zipfile
import requests
import fitz  # PyMuPDF
import os
import tempfile
from datetime import datetime
from supabase import create_client
from config import settings
from browser_downloader import BrowserDownloader
from openai_client import OpenAIClient
import threading

# Thread-local storage for Supabase Service Role clients (Bypass RLS)
_thread_local = threading.local()

def get_supabase_service_role():
    if not hasattr(_thread_local, 'client'):
        _thread_local.client = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY)
    return _thread_local.client

class ZipProcessor:
    def __init__(self):
        # Use Service Role Key to bypass RLS for storage uploads
        # self.supabase = create_client(settings.SUPABASE_URL, settings.SUPABASE_SERVICE_ROLE_KEY) # Global disabled
        self.browser_downloader = BrowserDownloader()
        # Initialize OpenAI client for OCR validation
        try:
            self.ai_client = OpenAIClient()
        except Exception as e:
            print(f"[AVISO] Não foi possível inicializar OpenAI para OCR: {e}")
            self.ai_client = None

    def process_zip_url(self, case_number: str, zip_url: str, case_id: int, projeto_id: str = None):
        """
        Downloads ZIP, finds valid PDF or Excel, uploads to Storage, and registers in DB.
        Priority: PDF first, then Excel as fallback.
        """
        try:
            # 1. Download ZIP using browser (handles JavaScript redirects)
            print(f"   Baixando ZIP via navegador: {zip_url[:50]}...")
            zip_bytes = self.browser_downloader.download_file(zip_url, timeout_ms=60000)
            
            # ZIP Size Limit Check
            max_size_mb = getattr(settings, 'MAX_ZIP_SIZE_MB', 200) # Default 200MB
            if len(zip_bytes) > max_size_mb * 1024 * 1024:
                 error_msg = f"ZIP muito grande ({len(zip_bytes) // (1024*1024)}MB > {max_size_mb}MB)"
                 print(f"   [AVISO] {error_msg}")
                 return {"success": False, "error": error_msg}

            zip_buffer = io.BytesIO(zip_bytes)
            
            # 2. Open ZIP and find files
            found_valid_file = False
            error_msg = None
            
            with zipfile.ZipFile(zip_buffer) as z:
                # Filter files with "analise" OR "formulario" in the name (case-insensitive, accent-insensitive)
                import unicodedata
                import re
                
                def normalize_text(text):
                    """Remove acentos e converte para minúsculas"""
                    text = unicodedata.normalize('NFKD', text)
                    text = text.encode('ASCII', 'ignore').decode('ASCII')
                    return text.lower()
                
                all_files = z.namelist()
                pdf_files = []
                excel_files = []
                
                for f in all_files:
                    normalized = normalize_text(f)
                    # Busca por "analise" OU "formulario"
                    if "analise" in normalized or "formulario" in normalized:
                        if f.lower().endswith('.pdf'):
                            pdf_files.append(f)
                        elif f.lower().endswith('.xlsx') or f.lower().endswith('.xls'):
                            excel_files.append(f)
                
                print(f"   Encontrados: {len(pdf_files)} PDF(s) e {len(excel_files)} Excel(s) com 'analise'")
                
                if not pdf_files and not excel_files:
                    error_msg = "Nenhum arquivo PDF ou Excel com 'ANALISE' no nome encontrado no ZIP."
                    print(f"   [AVISO] {error_msg}")
                    return {"success": False, "error": error_msg}

                # 3. Try Excel FIRST (priority changed)
                valid_file_bytes = None
                valid_filename = None
                file_type = None
                
                if excel_files:
                    for filename in excel_files:
                        print(f"   Verificando Excel: {filename}")
                        with z.open(filename) as f_excel:
                            excel_bytes = f_excel.read()
                            print(f"   ✅ Excel encontrado: {filename}")
                            valid_file_bytes = excel_bytes
                            valid_filename = filename
                            file_type = "EXCEL"
                            break
                
                # 4. If no Excel found, try PDF as fallback
                if not valid_file_bytes and pdf_files:
                    print("   Excel não encontrado. Tentando PDF como fallback...")
                    for filename in pdf_files:
                        print(f"   Verificando PDF: {filename}")
                        with z.open(filename) as f_pdf:
                            pdf_bytes = f_pdf.read()
                            
                            if self._validate_pdf_content(pdf_bytes):
                                print(f"   ✅ PDF válido encontrado: {filename}")
                                valid_file_bytes = pdf_bytes
                                valid_filename = filename
                                file_type = "PDF"
                                break
                            else:
                                print(f"   ❌ PDF inválido (não é Formulário de Sinistro)")
                
                if not valid_file_bytes:
                    error_msg = "Nenhum arquivo válido encontrado (PDF deve ser Formulário de Sinistro)."
                    print(f"   [AVISO] {error_msg}")
                    return {"success": False, "error": error_msg}
                
                # 5. Sanitize filename for Supabase Storage
                import unicodedata
                import re
                
                safe_filename = os.path.basename(valid_filename)
                # Normalize unicode (remove accents)
                safe_filename = unicodedata.normalize('NFKD', safe_filename)
                safe_filename = safe_filename.encode('ASCII', 'ignore').decode('ASCII')
                # Replace spaces and special chars with underscore
                safe_filename = re.sub(r'[^a-zA-Z0-9._-]', '_', safe_filename)
                # Remove multiple underscores
                safe_filename = re.sub(r'_+', '_', safe_filename)
                
                print(f"   Nome sanitizado: {safe_filename}")
                
                # 6. Upload to Supabase Storage
                storage_path = f"salesforce/{case_number}/{safe_filename}"
                content_type = "application/pdf" if file_type == "PDF" else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                
                try:
                    get_supabase_service_role().storage.from_("processos").upload(
                        path=storage_path,
                        file=valid_file_bytes,
                        file_options={"content-type": content_type, "upsert": "true"}
                    )
                    print(f"   [OK] Arquivo enviado para Storage: {storage_path}")
                except Exception as upload_error:
                    error_msg = str(upload_error)
                    if 'already exists' in error_msg.lower() or 'duplicate' in error_msg.lower() or '403' in error_msg:
                        print(f"   Arquivo já existe no Storage, pulando upload.")
                    else:
                        raise
                
                # 7. Register in DB
                try:
                    get_supabase_service_role().table(settings.TABLE_GERENCIAMENTO).insert({
                        "filename": safe_filename,
                        "storage_path": storage_path,
                        "status": "PENDENTE",
                        "caso_id": case_id,
                        "projeto_id": projeto_id,
                        "origem": f"SALESFORCE_{file_type}",  # Mark if PDF or EXCEL
                        "created_at": "now()"
                    }).execute()
                    print(f"   [OK] Documento registrado no banco ({file_type})")
                except Exception as db_error:
                    error_msg = str(db_error)
                    if 'duplicate' in error_msg.lower() or 'unique' in error_msg.lower():
                        print(f"   Documento já registrado no banco, pulando.")
                    else:
                        raise
                
                # 8. Trigger Worker immediately
                if projeto_id:
                    try:
                        get_supabase_service_role().table(settings.TABLE_PROCESSAR_AGORA).insert({
                            "projeto_id": projeto_id
                        }).execute()
                        print("   Worker acionado automaticamente.")
                    except Exception:
                        pass
                
                found_valid_file = True

            if found_valid_file:
                print(f"   [OK] Processamento concluído: 1 arquivo válido ({file_type}) encontrado")
                return {"success": True, "files_processed": 1, "file_type": file_type}
            else:
                print(f"   [AVISO] Nenhum arquivo válido encontrado no ZIP")
                return {"success": False, "error": "Nenhum arquivo válido encontrado."}

        except Exception as e:
            print(f"   [ERRO] Erro processando ZIP: {e}")
            import traceback
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _validate_pdf_content(self, pdf_bytes: bytes) -> bool:
        """
        Checks if document contains keywords: formulario, analise, sinistro
        Requires at least 2 out of 3 keywords to be present
        Uses OCR fallback if text extraction fails or keywords not found
        """
        try:
            print(f"\n   🔍 [DEBUG] Iniciando validação de PDF ({len(pdf_bytes)} bytes)")
            
            # Try standard text extraction first
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            if len(doc) < 1:
                print("   ❌ [DEBUG] PDF vazio ou sem páginas")
                return False
            
            text = doc[0].get_text().upper()
            print(f"   📄 [DEBUG] Texto extraído: {len(text)} caracteres")
            if len(text) > 0:
                print(f"   📝 [DEBUG] Preview: {text[:200]}...")
            
            # Normalize text (remove accents)
            import unicodedata
            def normalize(s):
                s = unicodedata.normalize('NFKD', s)
                return s.encode('ASCII', 'ignore').decode('ASCII').upper()
            
            normalized_text = normalize(text)
            
            # Keywords to search for (at least 2 must be present)
            keywords = ["FORMULARIO", "ANALISE", "SINISTRO"]
            found_keywords = sum(1 for kw in keywords if kw in normalized_text)
            
            print(f"   🔎 [DEBUG] Palavras-chave encontradas: {found_keywords}/3")
            for kw in keywords:
                status = "✅" if kw in normalized_text else "❌"
                print(f"      {status} {kw}")
            
            # If found at least 2 keywords with standard extraction
            if len(text.strip()) > 20 and found_keywords >= 2:
                print(f"   ✅ Documento válido: encontradas {found_keywords}/3 palavras-chave")
                return True
            
            # If not found or text too short, try OCR
            print(f"   ⚠️ Apenas {found_keywords}/3 palavras-chave encontradas. Tentando OCR...")
            
            # Check if OpenAI client is available
            if not self.ai_client:
                print("   ❌ [DEBUG] OpenAI client não disponível (self.ai_client is None)")
                return found_keywords >= 1
            
            print("   ✅ [DEBUG] OpenAI client disponível, iniciando OCR...")
            
            if self.ai_client:
                try:
                    # Save to temp file for OCR processing
                    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                        tmp.write(pdf_bytes)
                        tmp_path = tmp.name
                    
                    print(f"   💾 [DEBUG] PDF temporário salvo em: {tmp_path}")
                    
                    try:
                        print(f"   📡 [DEBUG] Chamando OpenAI Vision API direta para extração de texto...")
                        extracted_text = self.ai_client.extract_text_with_vision(tmp_path)
                        print(f"\n   📥 [DEBUG] ===== TEXTO EXTRAÍDO PELO OCR =====")
                        print(extracted_text)
                        print(f"   📥 [DEBUG] ===== FIM DO TEXTO =====\n")
                        
                        # Now check for keywords in the extracted text
                        import unicodedata
                        def normalize(s):
                            s = unicodedata.normalize('NFKD', s)
                            return s.encode('ASCII', 'ignore').decode('ASCII').upper()
                        
                        normalized_ocr = normalize(extracted_text)
                        keywords = ["FORMULARIO", "ANALISE", "SINISTRO"]
                        found_in_ocr = sum(1 for kw in keywords if kw in normalized_ocr)
                        
                        print(f"   🔎 [DEBUG] Palavras-chave no OCR: {found_in_ocr}/3")
                        for kw in keywords:
                            status = "✅" if kw in normalized_ocr else "❌"  
                            print(f"      {status} {kw}")
                        
                        # Accept if found at least 2 keywords
                        if found_in_ocr >= 2:
                            print("   ✅ OCR confirmou: documento válido")
                            return True
                        else:
                            print("   ❌ OCR: não contém palavras-chave suficientes")
                            return False
                    finally:
                        # Cleanup temp file
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                            print(f"   🗑️ [DEBUG] Arquivo temporário removido")
                            
                except Exception as ocr_error:
                    print(f"   ❌ [DEBUG] Erro no OCR de validação: {type(ocr_error).__name__}: {ocr_error}")
                    import traceback
                    traceback.print_exc()
                    # Fallback: accept if found at least 1 keyword
                    fallback = found_keywords >= 1
                    print(f"   🔄 [DEBUG] Fallback ativado: aceitar = {fallback} (keywords={found_keywords})")
                    return fallback
            else:
                print("   [AVISO] OpenAI não disponível para OCR")
                # Accept if found at least 1 keyword
                return found_keywords >= 1
            
        except Exception as e:
            print(f"   ❌ [DEBUG] Erro na validação: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return False
