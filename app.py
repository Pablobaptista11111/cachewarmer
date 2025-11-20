from flask import Flask
import asyncio
import aiohttp
import requests
import time
import gzip
import threading
import re
import sys

# --- CONFIGURAÇÃO ---
SITEMAP_URL = "https://fullbai.com.ar/sitemap_index.xml"
# --------------------

app = Flask(__name__)

is_running = False

# Headers idênticos a um Chrome real para enganar firewall
HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    # Mantemos o CacheBot aqui caso sua regra do Cloudflare exija ele
    'X-Bot-Name': 'CacheBot' 
}

def log_print(msg):
    print(msg, flush=True)
    sys.stdout.flush()

def get_urls_via_regex(text_content):
    return re.findall(r'<loc>(.*?)</loc>', text_content)

def get_all_sitemap_urls_sync(url_inicial):
    """Função síncrona e robusta para baixar os sitemaps"""
    urls_finais = set()
    sitemaps_para_visitar = [url_inicial]
    visitados = set()

    log_print(f"--- [V4] BAIXANDO SITEMAPS (Timeout: 60s) ---")

    while sitemaps_para_visitar:
        atual = sitemaps_para_visitar.pop(0)
        if atual in visitados:
            continue
        visitados.add(atual)

        try:
            log_print(f"Baixando: {atual} ...")
            # Timeout aumentado para 60 segundos!
            r = requests.get(atual, headers=HEADERS_FAKE, timeout=60)
            
            if r.status_code != 200:
                log_print(f"ERRO {r.status_code} ao baixar {atual}")
                continue

            content = r.content
            # Tenta descompactar automaticamente
            if atual.endswith('.gz') or r.headers.get('Content-Type') == 'application/x-gzip':
                try:
                    content = gzip.decompress(content)
                except:
                    pass
            
            texto = content.decode('utf-8', errors='ignore')
            links = get_urls_via_regex(texto)
            
            novos_sitemaps = 0
            novos_produtos = 0
            
            for link in links:
                link = link.strip()
                if 'sitemap' in link and (link.endswith('.xml') or link.endswith('.gz')):
                    if link not in visitados:
                        sitemaps_para_visitar.append(link)
                        novos_sitemaps += 1
                else:
                    urls_finais.add(link)
                    novos_produtos += 1
            
            # Feedback visual para saber que não travou
            if novos_sitemaps > 0:
                log_print(f"-> Encontrados +{novos_sitemaps} sitemaps para ler.")
                    
        except Exception as e:
            log_print(f"FALHA GRAVE em {atual}: {str(e)}")

    log_print(f"--- TOTAL ENCONTRADO: {len(urls_finais)} URLs ---")
    return list(urls_finais)

async def fetch_url(session, url):
    try:
        async with session.get(url, headers=HEADERS_FAKE, timeout=30) as response:
            await response.read()
            cf = response.headers.get('cf-cache-status', 'MISS')
            if response.status != 200:
                log_print(f"ERRO HTTP: {url} [{response.status}]")
            elif 'HIT' not in cf:
                log_print(f"ESQUENTADO: {url} [CF: {cf}]")
    except:
        pass

async def worker_logic():
    log_print("Iniciando lógica assíncrona...")
    
    # 1. Baixa tudo de forma síncrona (mais seguro contra erros)
    loop = asyncio.get_running_loop()
    urls = await loop.run_in_executor(None, get_all_sitemap_urls_sync, SITEMAP_URL)
    
    total = len(urls)
    if total == 0:
        log_print("ABORTANDO: Nenhuma URL encontrada. Verifique se a loja está online.")
        return

    log_print(f"Iniciando visitas em {total} produtos...")
    
    # 2. Visita assíncrona
    semaphore = asyncio.Semaphore(15) 
    async with aiohttp.ClientSession() as session:
        async def bound_fetch(url):
            async with semaphore:
                await fetch_url(session, url)
        tasks = [bound_fetch(url) for url in urls]
        await asyncio.gather(*tasks)
    
    log_print("--- PROCESSO FINALIZADO COM SUCESSO ---")

def run_background_thread():
    global is_running
    # Cria um novo loop exclusivo para esta thread (evita o erro RuntimeError)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(worker_logic())
    finally:
        loop.close()
        is_running = False

@app.route('/')
def index():
    return "<h1>Gerador V4 (Blindado)</h1><a href='/iniciar'><button>INICIAR</button></a>"

@app.route('/iniciar')
def iniciar():
    global is_running
    if not is_running:
        is_running = True
        t = threading.Thread(target=run_background_thread)
        t.start()
    return "Iniciado V4! Verifique o log."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
