from flask import Flask, redirect, url_for
import asyncio
import aiohttp
import requests
import threading
import re
import time
import gzip
# Desabilita avisos de SSL inseguro (necessário para o modo turbo)
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
SITEMAP_URL = "https://fullbai.com.ar/sitemap_index.xml"
# --------------------

app = Flask(__name__)

# Memória do servidor
status_global = "PARADO"
logs_memoria = []

# Cabeçalhos idênticos ao Chrome
HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def adicionar_log(msg):
    """Guarda a mensagem na memória para mostrar no site"""
    timestamp = time.strftime("%H:%M:%S")
    linha = f"[{timestamp}] {msg}"
    print(linha, flush=True) # Tenta mostrar na tela preta tbm
    logs_memoria.insert(0, linha) # Adiciona no topo da lista
    # Mantém apenas as ultimas 200 linhas para não lotar a memoria
    if len(logs_memoria) > 200:
        logs_memoria.pop()

def get_urls_via_regex(text_content):
    return re.findall(r'<loc>(.*?)</loc>', text_content)

def get_all_sitemap_urls_sync(url_inicial):
    urls_finais = set()
    sitemaps_para_visitar = [url_inicial]
    visitados = set()

    adicionar_log(f"--- BAIXANDO SITEMAPS (Timeout: 60s) ---")

    while sitemaps_para_visitar:
        atual = sitemaps_para_visitar.pop(0)
        if atual in visitados:
            continue
        visitados.add(atual)

        try:
            adicionar_log(f"Lendo: {atual} ...")
            # verify=False é o segredo para não travar no SSL
            r = requests.get(atual, headers=HEADERS_FAKE, timeout=60, verify=False)
            
            if r.status_code != 200:
                adicionar_log(f"ERRO {r.status_code} em {atual}")
                continue

            content = r.content
            if atual.endswith('.gz'):
                try:
                    content = gzip.decompress(content)
                except:
                    pass
            
            texto = content.decode('utf-8', errors='ignore')
            links = get_urls_via_regex(texto)
            
            novos = 0
            for link in links:
                link = link.strip()
                if 'sitemap' in link and (link.endswith('.xml') or link.endswith('.gz')):
                    if link not in visitados:
                        sitemaps_para_visitar.append(link)
                else:
                    urls_finais.add(link)
                    novos += 1
            
            if novos > 0:
                adicionar_log(f"-> Achados +{novos} produtos.")
                    
        except Exception as e:
            adicionar_log(f"FALHA: {str(e)}")

    adicionar_log(f"--- TOTAL: {len(urls_finais)} URLs para visitar ---")
    return list(urls_finais)

async def fetch_url(session, url):
    try:
        # verify_ssl=False para aiohttp
        async with session.get(url, headers=HEADERS_FAKE, timeout=30, ssl=False) as response:
            await response.read()
            if response.status != 200:
                adicionar_log(f"ERRO HTTP {response.status}: {url}")
    except:
        pass

async def worker_logic():
    global status_global
    adicionar_log("Iniciando lógica V5...")
    
    # 1. Baixa Sitemaps (Síncrono)
    loop = asyncio.get_running_loop()
    urls = await loop.run_in_executor(None, get_all_sitemap_urls_sync, SITEMAP_URL)
    
    total = len(urls)
    if total == 0:
        adicionar_log("ABORTANDO: Zero URLs encontradas.")
        status_global = "PARADO (ERRO)"
        return

    adicionar_log(f"Iniciando visitas em {total} produtos...")
    
    # 2. Visita (Assíncrono)
    semaphore = asyncio.Semaphore(20) 
    async with aiohttp.ClientSession() as session:
        async def bound_fetch(url):
            async with semaphore:
                await fetch_url(session, url)
        
        # Barra de progresso fake nos logs
        tarefas = []
        for i, url in enumerate(urls):
            tarefas.append(bound_fetch(url))
            if i % 50 == 0: # A cada 50, imprime status
                adicionar_log(f"Progresso: {i}/{total} urls enviadas...")
        
        await asyncio.gather(*tarefas)
    
    adicionar_log("--- PROCESSO FINALIZADO COM SUCESSO ---")
    status_global = "CONCLUÍDO"

def run_background_thread():
    global status_global
    status_global = "RODANDO"
    logs_memoria.clear()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(worker_logic())
    finally:
        loop.close()
        if status_global == "RODANDO":
            status_global = "PARADO"

@app.route('/')
def index():
    return redirect(url_for('monitorar'))

@app.route('/iniciar')
def iniciar():
    global status_global
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background_thread)
        t.start()
    return redirect(url_for('monitorar'))

@app.route('/monitorar')
def monitorar():
    # HTML que se atualiza sozinho a cada 2 segundos
    log_html = "<br>".join(logs_memoria)
    cor = "green" if status_global == "RODANDO" else "red"
    
    return f"""
    <html>
    <head>
        <meta http-equiv="refresh" content="2">
        <style>
            body {{ font-family: monospace; padding: 20px; background: #f0f0f0; }}
            .box {{ background: white; padding: 20px; border: 1px solid #ccc; border-radius: 8px; }}
            .btn {{ background: blue; color: white; padding: 10px 20px; text-decoration: none; font-size: 18px; display: inline-block; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <h1>Gerador V5 (Monitoramento Real)</h1>
        <p>Status: <b style="color:{cor}">{status_global}</b></p>
        
        <a href="/iniciar" class="btn">INICIAR / REINICIAR</a>
        
        <div class="box">
            <h3>Log de Eventos (Atualiza a cada 2s):</h3>
            {log_html}
        </div>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
