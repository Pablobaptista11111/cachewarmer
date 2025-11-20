from flask import Flask, redirect, url_for
import asyncio
import aiohttp
import requests
import threading
import re
import time
import gzip
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
SITEMAP_INDEX = "https://fullbai.com.ar/sitemap_index.xml"
BASE_URL = "https://fullbai.com.ar"
# --------------------

app = Flask(__name__)

status_global = "PARADO"
logs_memoria = []

HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def adicionar_log(msg):
    timestamp = time.strftime("%H:%M:%S")
    linha = f"[{timestamp}] {msg}"
    print(linha, flush=True)
    logs_memoria.insert(0, linha)
    if len(logs_memoria) > 300:
        logs_memoria.pop()

def get_urls_via_regex(text_content):
    return re.findall(r'<loc>(.*?)</loc>', text_content)

def gerar_lista_manual():
    """Gera a lista na ordem EXATA que você pediu"""
    lista = []
    adicionar_log(">>> GERANDO LISTA MANUAL (ORDEM PRIORITÁRIA) <<<")
    
    # 1. PRIORIDADE MÁXIMA: Páginas (Home, etc)
    lista.append(f"{BASE_URL}/page-sitemap.xml")
    
    # 2. PRIORIDADE: Produtos (do 1 ao 210)
    for i in range(1, 211):
        lista.append(f"{BASE_URL}/product-sitemap{i}.xml")
    
    # 3. PRIORIDADE FINAL: Categorias
    # Nota: Adicionei as duas variações comuns do Yoast para garantir
    lista.append(f"{BASE_URL}/category-sitemap.xml")      # Categorias de Blog
    lista.append(f"{BASE_URL}/product_cat-sitemap.xml")   # Categorias de Produto (Importante para loja)
    
    adicionar_log(f"Lista gerada com {len(lista)} sitemaps na ordem correta.")
    return lista

def processar_sitemap_individual(url_sitemap):
    produtos_encontrados = set()
    novos_sitemaps = [] # Não vamos usar recursão no modo manual para respeitar a ordem
    
    try:
        adicionar_log(f"Lendo: {url_sitemap} ...")
        r = requests.get(url_sitemap, headers=HEADERS_FAKE, timeout=60, verify=False)
        
        if r.status_code != 200:
            adicionar_log(f"Ignorado: {url_sitemap} (Status {r.status_code})")
            return set()

        content = r.content
        if url_sitemap.endswith('.gz'):
            try:
                content = gzip.decompress(content)
            except:
                pass
        
        texto = content.decode('utf-8', errors='ignore')
        links = get_urls_via_regex(texto)
        
        for link in links:
            link = link.strip()
            # Se achou link de produto/pagina, adiciona
            if not ('sitemap' in link and link.endswith('.xml')):
                produtos_encontrados.add(link)
                
    except Exception as e:
        adicionar_log(f"Erro ao ler {url_sitemap}: {str(e)}")
    
    return produtos_encontrados

def get_all_sitemap_urls_sync():
    urls_finais = [] # Mudamos para lista para MANTER A ORDEM DE VISITA
    visitados = set()

    # Pula direto para o modo manual para respeitar sua ordem
    # (Ignora o sitemap_index.xml que bagunça a ordem e dá timeout)
    sitemaps_para_visitar = gerar_lista_manual()

    adicionar_log(f"Iniciando leitura sequencial de {len(sitemaps_para_visitar)} sitemaps...")
    
    count = 0
    for sitemap in sitemaps_para_visitar:
        prods = processar_sitemap_individual(sitemap)
        
        # Adiciona os produtos encontrados mantendo a ordem de chegada
        novos = 0
        for p in prods:
            if p not in visitados:
                visitados.add(p)
                urls_finais.append(p)
                novos += 1
        
        count += 1
        if novos > 0:
            adicionar_log(f"-> +{novos} URLs extraídas deste sitemap.")

    return urls_finais

async def fetch_url(session, url):
    try:
        async with session.get(url, headers=HEADERS_FAKE, timeout=40, ssl=False) as response:
            await response.read()
            # Sem logs para cada URL para não travar o navegador
    except:
        pass

async def worker_logic():
    global status_global
    adicionar_log("Iniciando V7 (Ordem Prioritária)...")
    
    loop = asyncio.get_running_loop()
    urls = await loop.run_in_executor(None, get_all_sitemap_urls_sync)
    
    total = len(urls)
    adicionar_log(f"--- TOTAL FINAL: {total} URLs na fila ---")
    
    if total == 0:
        adicionar_log("PARANDO: Nada encontrado.")
        status_global = "PARADO"
        return

    # Aumentei para 50 threads (visitantes simultâneos)
    semaphore = asyncio.Semaphore(50) 
    async with aiohttp.ClientSession() as session:
        async def bound_fetch(url):
            async with semaphore:
                await fetch_url(session, url)
        
        tarefas = []
        adicionar_log("Disparando visitas na ordem da lista...")
        for i, url in enumerate(urls):
            tarefas.append(bound_fetch(url))
            if i > 0 and i % 200 == 0:
                adicionar_log(f"Progresso: {i}/{total} visitas enviadas...")
        
        await asyncio.gather(*tarefas)
    
    adicionar_log("--- TUDO FINALIZADO ---")
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
        if status_global == "RODANDO": status_global = "PARADO"

@app.route('/')
def index(): return redirect(url_for('monitorar'))

@app.route('/iniciar')
def iniciar():
    global status_global
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background_thread)
        t.start()
    return redirect(url_for('monitorar'))

@app.route('/monitorar')
def monitorar():
    log_html = "<br>".join(logs_memoria)
    cor = "green" if status_global == "RODANDO" else "red"
    return f"""
    <html>
    <head>
        <meta http-equiv="refresh" content="2">
        <style>
            body {{ font-family: monospace; padding: 20px; background: #222; color: #fff; }}
            .box {{ background: #333; padding: 20px; border: 1px solid #444; border-radius: 8px; }}
            .btn {{ background: #007bff; color: white; padding: 15px 30px; text-decoration: none; font-size: 18px; display: inline-block; margin-bottom: 20px; border-radius: 5px; }}
        </style>
    </head>
    <body>
        <h1>Gerador V7 (Páginas > Produtos > Categorias)</h1>
        <p>Status: <b style="color:{cor}">{status_global}</b></p>
        <a href="/iniciar" class="btn">INICIAR ROBO</a>
        <div class="box"><h3>Logs:</h3>{log_html}</div>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
