from flask import Flask
import asyncio
import aiohttp
import requests
import time
import gzip
import threading
import re
import sys # Importante para forçar o log

# --- CONFIGURAÇÃO ---
SITEMAP_URL = "https://fullbai.com.ar/sitemap_index.xml"
# --------------------

app = Flask(__name__)

is_running = False
log_mensagens = []

def log_print(msg):
    """Função forçada para escrever no log imediatamente"""
    print(msg, flush=True)
    sys.stdout.flush()

def get_urls_via_regex(text_content):
    return re.findall(r'<loc>(.*?)</loc>', text_content)

def get_all_sitemap_urls(url_inicial):
    urls_finais = set()
    sitemaps_para_visitar = [url_inicial]
    visitados = set()

    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; CacheBot/2.0)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }

    log_print(f"--- [V3] INICIANDO VARREDURA EM: {url_inicial} ---")

    while sitemaps_para_visitar:
        atual = sitemaps_para_visitar.pop(0)
        if atual in visitados:
            continue
        visitados.add(atual)

        try:
            log_print(f"Baixando Sitemap: {atual} ...")
            r = requests.get(atual, headers=headers, timeout=20)
            
            if r.status_code != 200:
                log_print(f"ERRO {r.status_code} ao baixar {atual}")
                continue

            content = r.content
            if atual.endswith('.gz') or r.headers.get('Content-Type') == 'application/x-gzip':
                try:
                    content = gzip.decompress(content)
                except:
                    pass
            
            texto = content.decode('utf-8', errors='ignore')
            links_encontrados = get_urls_via_regex(texto)
            
            for link in links_encontrados:
                link = link.strip()
                if 'sitemap' in link and (link.endswith('.xml') or link.endswith('.gz')):
                    if link not in visitados:
                        sitemaps_para_visitar.append(link)
                else:
                    urls_finais.add(link)
                    
        except Exception as e:
            log_print(f"Erro critico ao processar {atual}: {e}")

    log_print(f"--- VARREDURA CONCLUIDA: {len(urls_finais)} URLs encontradas ---")
    return list(urls_finais)

async def fetch_url(session, url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; CacheBot/2.0)'}
        async with session.get(url, headers=headers, timeout=15) as response:
            await response.read()
            cf = response.headers.get('cf-cache-status', 'MISS')
            # Printa apenas a cada 50 requisições para não poluir demais, ou se der erro
            if response.status != 200:
                log_print(f"FALHA: {url} [{response.status}]")
            elif 'HIT' in cf:
                pass # Não printa HIT para economizar log visual
            else:
                # Printa só os MISS (que estamos esquentando)
                log_print(f"ESQUENTADO: {url} [CF: {cf}]")
            return True
    except:
        return False

async def worker():
    global is_running
    is_running = True
    log_mensagens.clear()
    log_mensagens.append("Iniciando V3... Olhe o log preto!")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    urls = await loop.run_in_executor(None, get_all_sitemap_urls, SITEMAP_URL)
    
    total = len(urls)
    log_print(f"Total de URLs para visitar: {total}")

    if total > 0:
        semaphore = asyncio.Semaphore(20) 
        async with aiohttp.ClientSession() as session:
            async def bound_fetch(url):
                async with semaphore:
                    await fetch_url(session, url)
            tasks = [bound_fetch(url) for url in urls]
            await asyncio.gather(*tasks)
    
    log_print("--- CICLO FINALIZADO ---")
    is_running = False

def run_async_background():
    asyncio.run(worker())

@app.route('/')
def index():
    return f"""
    <h1>Gerador V3 (Tagarela)</h1>
    <a href="/iniciar"><button>INICIAR AGORA</button></a>
    """

@app.route('/iniciar')
def iniciar():
    global is_running
    if not is_running:
        t = threading.Thread(target=run_async_background)
        t.start()
    return "Iniciado! Agora é impossível o log não aparecer."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
