import openai
import time
import threading
from config import settings
# import fitz  # PyMuPDF (Removed)
from tenacity import retry, stop_after_attempt, wait_exponential

class OpenAIClient:
    def __init__(self):
        if not settings.OPENAI_API_KEY:
            raise ValueError("OPENAI_API_KEY não configurada no .env")
        
        self.client = openai.OpenAI(api_key=settings.OPENAI_API_KEY)
        self.model_name = settings.MODEL_OPENAI
        
        # Rate Limiting (Prevent 429 Errors)
        self._rpm_limit = getattr(settings, 'OPENAI_RPM_LIMIT', 500) # Default Tier 1: 500 RPM
        self._call_interval = 60.0 / self._rpm_limit
        self._last_call_time = 0
        self._rate_lock = threading.Lock()
        
        print(f"[OK] Cliente OpenAI inicializado: {self.model_name} (RPM Limit: {self._rpm_limit})")

    def _wait_for_rate_limit(self):
        """Block thread to respect Rate Limit"""
        with self._rate_lock:
            current_time = time.time()
            elapsed = current_time - self._last_call_time
            
            if elapsed < self._call_interval:
                sleep_time = self._call_interval - elapsed
                time.sleep(sleep_time)
            
            self._last_call_time = time.time()

    def analyze_document(self, file_path: str, prompt_text: str) -> str:
        """
        Analisa documento usando OpenAI GPT-4o e Docling para extração.
        Estratégia: Converter PDF para Markdown estruturado e enviar como mensagem.
        OCR: Usa APENAS OpenAI Vision (gpt-4o), não usa OCR local.
        """
        
        # 1. Extrair texto estruturado do PDF com Docling
        text_content = None
        ocr_activated = False
        
        try:
            print(f"   🚀 Iniciando conversão padrão com Docling (SEM OCR local): {file_path}")
            from docling.document_converter import DocumentConverter, PdfFormatOption
            from docling.datamodel.base_models import InputFormat
            from docling.datamodel.pipeline_options import PdfPipelineOptions, VlmPipelineOptions, ApiVlmOptions
            
            # Tentativa 1: Conversão Padrão SEM OCR (Rápida/Local)
            try:
                # Configurar para NÃO usar OCR local
                pipeline_options_no_ocr = PdfPipelineOptions()
                pipeline_options_no_ocr.do_ocr = False  # Desabilitar OCR local
                
                converter = DocumentConverter(
                    format_options={
                        InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options_no_ocr)
                    }
                )
                result = converter.convert(file_path)
                text_content = result.document.export_to_markdown()
            except Exception as extraction_error:
                print(f"   ⚠️ Erro na extração padrão: {extraction_error}")
                print("   🔄 Ativando OpenAI Vision devido ao erro...")
                ocr_activated = True
            
            # Validação: Se não há texto suficiente OU houve erro, tentar OCR via OpenAI (VLM)
            if ocr_activated or (text_content and len(text_content.strip()) < 50):
                if not ocr_activated:
                    print("   [AVISO] Texto insuficiente (< 50 chars). Ativando OpenAI Vision...")
                
                # Configurar Pipeline VLM com OpenAI (DESABILITA OCR LOCAL!)
                pipeline_options = PdfPipelineOptions()
                pipeline_options.do_ocr = False  # Não usar OCR local (RapidOCR)
                pipeline_options.do_table_structure = True
                pipeline_options.table_structure_options.do_cell_matching = True
                
                # Configurar VLM para usar OpenAI gpt-4o
                pipeline_options.vlm_options = VlmPipelineOptions(
                    api_options=ApiVlmOptions(
                        api_key=settings.OPENAI_API_KEY,
                        model="gpt-4o",
                        prompt="Extract all text from this document.",
                        response_format="markdown"
                    )
                )
                
                # Re-instanciar conversor com opções VLM
                converter_vlm = DocumentConverter(
                    format_options={
                        InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
                    }
                )
                
                print(f"   👁️ Iniciando OCR com OpenAI Vision (gpt-4o)...")
                result_vlm = converter_vlm.convert(file_path)
                text_content = result_vlm.document.export_to_markdown()
                print("   ✅ OCR com OpenAI Vision concluído.")

            # Estimativa básica de tokens
            token_est = len(text_content) // 4
            print(f"   📄 Markdown extraído: {token_est} tokens (aprox.)")
            
            # DEBUG CRÍTICO
            print("\n   🔍 --- INÍCIO DO MARKDOWN EXTRAÍDO (DEBUG) ---")
            print(text_content[:600])
            print("   🔍 --- FIM DO DEBUG ---\n")
                
        except Exception as e:
            print(f"[ERRO] Falha na extração com Docling (incluindo OCR): {e}")
            raise ValueError(f"Erro ao processar PDF com Docling: {e}")

        # 2. Enviar para OpenAI
        json_resp = self._call_openai(text_content, prompt_text)
        return json_resp, text_content

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _call_openai(self, text: str, prompt: str) -> str:
        try:
            # Enforce Rate Limit
            self._wait_for_rate_limit()

            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {
                        "role": "system", 
                        "content": """Você é um assistente jurídico especializado em análise de documentos. 

REGRAS ESTRITAS:
1. Responda estritamente em JSON.
2. Extraia APENAS informações que estão EXPLICITAMENTE presentes no documento.
3. NUNCA invente, deduza, ou presuma informações.
4. Se uma informação não estiver no documento, deixe o campo em branco ("" ou null).
5. Seja literal - copie exatamente o que está escrito, não interprete."""
                    },
                    {"role": "user", "content": f"{prompt}\n\nDOCUMENTO:\n{text}"}
                ],
                response_format={"type": "json_object"},
                temperature=0.0  # Zero temperature for maximum determinism
            )
            
            content = response.choices[0].message.content
            if not content:
                raise ValueError("Resposta vazia da OpenAI")
            
            return content
            
        except Exception as e:
            # Tratamento básico de erros
            raise ValueError(f"Erro na OpenAI API: {e}")

    def extract_text_with_vision(self, pdf_path: str) -> str:
        """
        Extract text from PDF using OpenAI Vision API directly (gpt-4o-mini).
        Converts PDF first page to image since Vision API only accepts images.
        Returns plain text without JSON formatting.
        """
        try:
            import base64
            import fitz  # PyMuPDF
            import io
            from PIL import Image
            
            # Convert first page of PDF to image
            print(f"   🖼️ [DEBUG] Convertendo PDF para imagem...")
            doc = fitz.open(pdf_path)
            if len(doc) < 1:
                raise ValueError("PDF vazio ou sem páginas")
            
            # Render first page to pixmap (image)
            page = doc[0]
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # 2x resolution for better OCR
            
            # Convert pixmap to PNG bytes
            img_bytes = pix.pil_tobytes(format="PNG")
            
            # Convert to base64
            img_base64 = base64.b64encode(img_bytes).decode('utf-8')
            print(f"   ✅ [DEBUG] Imagem criada: {len(img_base64)} chars base64")
            
            # Use Vision API with image input
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "Extraia TODO o texto desta imagem. Retorne APENAS o texto bruto, exatamente como aparece, sem formatação adicional."
                            },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/png;base64,{img_base64}"
                                }
                            }
                        ]
                    }
                ],
                max_tokens=4000,
                temperature=0.0
            )
            
            extracted_text = response.choices[0].message.content
            return extracted_text if extracted_text else ""
            
        except Exception as e:
            print(f"   ❌ [DEBUG] Erro na extração Vision API: {type(e).__name__}: {e}")
            raise ValueError(f"Erro ao extrair texto com Vision API: {e}")
