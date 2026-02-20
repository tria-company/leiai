"""
Browser-based file downloader for Salesforce pages that require JavaScript execution.
Uses Playwright to handle pages that need button clicks or form submissions.
"""

import tempfile
import os
import time
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from config import settings
import requests
import threading

# Thread-local storage for Playwright instances
_thread_local = threading.local()

class BrowserDownloader:
    """
    Downloads files from pages that require browser interaction.
    Specifically designed for Salesforce download pages that use JavaScript redirects.
    """
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError, Exception)),
        reraise=True
    )
    def download_file(self, url: str, timeout_ms: int = 60000) -> bytes:
        """
        Downloads a file using a headless browser.
        
        Args:
            url: The Salesforce download page URL
            timeout_ms: Maximum time to wait for download (default 60s)
            
        Returns:
            bytes: The downloaded file content
            
        Raises:
            Exception: If download fails or times out
        """
    def __init__(self):
        """Initialize the browser pool (Lazy loaded per thread)"""
        pass

    def _get_browser(self):
        """Get or create a thread-local browser instance"""
        if not hasattr(_thread_local, 'playwright'):
            print(f"   🌐 [Thread {threading.get_ident()}] Inicializando navegador headless...")
            _thread_local.playwright = sync_playwright().start()
            _thread_local.browser = _thread_local.playwright.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
        return _thread_local.browser
        
    def close(self):
        """Cleanup browser resources for the current thread"""
        if hasattr(_thread_local, 'browser'):
            _thread_local.browser.close()
            del _thread_local.browser
        if hasattr(_thread_local, 'playwright'):
            _thread_local.playwright.stop()
            del _thread_local.playwright

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=30),
        retry=retry_if_exception_type((requests.exceptions.ConnectionError, Exception)),
        reraise=True
    )
    def download_file(self, url: str, timeout_ms: int = 60000) -> bytes:
        """
        Downloads a file using a context from the persistent browser pool.
        """
        # Get thread-local browser
        browser = self._get_browser()
        
        # Create a new isolated context for this download (lightweight)
        context = browser.new_context(
            accept_downloads=True,
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        try:
            page = context.new_page()
            
            # Set up download handler
            download_promise = None
            
            def handle_download(download):
                nonlocal download_promise
                download_promise = download
            
            page.on('download', handle_download)
            
            # Navigate to the page
            print(f"   📄 Carregando página Salesforce...")
            
            # Set up network monitoring
            download_url_from_network = None
            
            def handle_response(response):
                nonlocal download_url_from_network
                if response.status == 200 and 'content-disposition' in response.headers:
                    download_url_from_network = response.url
            
            page.on('response', handle_response)
            
            page.goto(url, wait_until='networkidle', timeout=timeout_ms)
            
            # ... click logic remains the same ...
            
            # Wait for page to fully load
            # print(f"   ⏳ Aguardando página carregar...")
            page.wait_for_timeout(3000)
            
            # Try to find and click download button
            print(f"   🔍 Procurando botão de download...")
            
            selectors = [
                'button.downloadbutton',
                'button[title="Fazer download"]',
                'button.bare.downloadbutton',
                'button[aria-label*="download"]',
                'button:has-text("Fazer download")',
                'button:has-text("Download")',
                'a:has-text("Download")',
                'button:has-text("Baixar")',
                'a:has-text("Baixar")',
                'button[title*="Download"]',
                'a[title*="Download"]',
                'button[aria-label*="Download"]',
                'a[download]',
                '.downloadButton',
                '#downloadButton',
                'a[href*="download"]',
                'button[onclick*="download"]',
                'input[type="button"][value*="Download"]',
                'input[type="submit"][value*="Download"]',
                'button',
                'a[href]'
            ]
            
            button_clicked = False
            for selector in selectors:
                try:
                    count = page.locator(selector).count()
                    if count > 0:
                        print(f"   [OK] Elemento encontrado: {selector}")
                        page.locator(selector).first.click(timeout=5000)
                        button_clicked = True
                        print(f"   🖱️ Clique executado, aguardando download...")
                        page.wait_for_timeout(10000)
                        break
                except Exception:
                    continue
            
            if not button_clicked and not download_url_from_network:
                 # Take screenshot for debugging
                screenshot_path = os.path.join(tempfile.gettempdir(), f"salesforce_no_button_{int(time.time())}.png")
                page.screenshot(path=screenshot_path)
                print(f"   📸 Screenshot salvo: {screenshot_path}")
                raise Exception(f"Nenhum botão de download encontrado na página.")
            
            # Check if we got a download from network monitoring
            if download_url_from_network and not download_promise:
                print(f"   🌐 URL de download capturada via rede: {download_url_from_network[:50]}...")
                @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=4, max=30), reraise=True)
                def download_with_retry(url):
                    print(f"   ⬇️ Tentando download direto...")
                    resp = requests.get(url, timeout=300)
                    resp.raise_for_status()
                    return resp.content
                
                return download_with_retry(download_url_from_network)
            
            # Wait for download to complete
            if download_promise:
                print(f"   ⬇️ Download iniciado (Playwright)...")
                
                # Save to temp file
                temp_dir = tempfile.mkdtemp()
                file_path = os.path.join(temp_dir, download_promise.suggested_filename)
                download_promise.save_as(file_path)
                
                # Read file bytes
                with open(file_path, 'rb') as f:
                    file_bytes = f.read()
                
                # Cleanup
                os.remove(file_path)
                os.rmdir(temp_dir)
                
                print(f"   [OK] Download concluido: {len(file_bytes)} bytes")
                return file_bytes
            else:
                raise Exception("Nenhum download iniciado.")
                
        except PlaywrightTimeoutError as e:
            raise Exception(f"Timeout ao carregar página: {e}")
        except Exception as e:
            raise Exception(f"Erro no download: {e}")
        finally:
            context.close()
