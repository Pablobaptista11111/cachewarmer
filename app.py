from flask import Flask
import asyncio
import aiohttp
import requests
import time
import gzip
import threading
import re # Vamos usar Regex que é mais garantia

# --- CONFIGURAÇÃO ---
SITEMAP_URL = "https://fullbai.com.ar/sitemap_index.xml"
# --------------------

app = Flask(__name__)

is_running = False
log_mensagens = []

def get_urls_via_regex(text_content):
    """Extrai URLs usando força bruta de texto, ignorando estrutura XML complexa"""
    # Procura tudo que está entre <loc> e </loc>
    return re.findall(r'<loc>(.*?)</loc>', text_content)

def get_all_sitemap_urls(url_inicial):
    urls_finais = set()
    sitemaps_para_visitar = [url_inicial]
    visitados = set()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }

    print(f"--- INICIANDO VARREDURA EM: {url_inicial} ---")

    while sitemaps_para_visitar:
        atual = sitemaps_para_visitar.pop(0)
        if atual in visitados:
            continue
        visitados.add(atual)

        try:
            print(f"Baixando Sitemap: {atual} ...")
            r = requests.get(atual, headers=headers, timeout=20)
            
            if r.status_code != 200:
                print(f"ERRO {r.status_code} ao baixar {atual}")
                continue

            # Tenta descompactar se for .gz ou se o header disser que é
            content = r.content
            if atual.endswith('.gz') or r.headers.get('Content-Type') == 'application/x-gzip':
                try:
                    content = gzip.decompress(content)
                except:
                    pass # Talvez não fosse gzip
            
            # Converte para texto
            texto = content.decode('utf-8', errors='ignore')
            
            # Pega todos os links dentro de <loc>
            links_encontrados = get_urls_via_regex(texto)
            
            for link in links_encontrados:
                link = link.strip()
                # Se o link termina em .xml ou .xml.gz, é outro sitemap
                if 'sitemap' in link and (link.endswith('.xml') or link.endswith('.gz')):
                    if link not in visitados:
                        sitemaps_para_visitar.append(link)
                else:
                    # Se não é sitemap, é produto/página
                    urls_finais.add(link)
                    
        except Exception as e:
            print(f"Erro critico ao processar {atual}: {e}")

    print(f"--- VARREDURA CONCLUIDA: {len(urls_finais)} URLs encontradas ---")
    return list(urls_finais)

async def fetch_url(session, url):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; CacheBot/2.0)'}
        async with session.get(url, headers=headers, timeout=15) as response:
            await response.read()
            cf = response.headers.get('cf-cache-status', 'MISS')
            print(f"Visitado: {url} [{response.status}] CF: {cf}")
            return True
    except:
        return False

async def worker():
    global is_running
    is_running = True
    log_mensagens.clear()
    log_mensagens.append("Lendo Sitemaps (Isso pode demorar 1 ou 2 minutos)... olhe o log preto!")
    
    # Roda a função de pegar URLs (que agora tem prints)
    loop = asyncio.get_event_loop()
    urls = await loop.run_in_executor(None, get_all_sitemap_urls, SITEMAP_URL)
    
    total = len(urls)
    log_mensagens.append(f"Total encontradas: {total}. Iniciando visitas...")

    if total > 0:
        semaphore = asyncio.Semaphore(15) # Aumentei um pouco a velocidade
        async with aiohttp.ClientSession() as session:
            async def bound_fetch(url):
                async with semaphore:
                    await fetch_url(session, url)
            tasks = [bound_fetch(url) for url in urls]
            await asyncio.gather(*tasks)
    
    log_mensagens.append("FINALIZADO!")
    is_running = False

def run_async_background():
    asyncio.run(worker())

@app.route('/')
def index():
    status = "RODANDO" if is_running else "PARADO"
    return f"""
    <h1>Gerador V2 (Modo Regex)</h1>
    <p>Status: <b>{status}</b></p>
    <a href="/iniciar"><button>INICIAR AGORA</button></a>
    <p>Ultima msg: {log_mensagens[-1] if log_mensagens else '...'}</p>
    """

@app.route('/iniciar')
def iniciar():
    global is_running
    if not is_running:
        t = threading.Thread(target=run_async_background)
        t.start()
    return "Iniciado! Verifique o LOG PRETO no EasyPanel para ver os links aparecendo."

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
