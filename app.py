from flask import Flask, redirect, url_for, request, jsonify
import asyncio
import aiohttp
import requests
import threading
import re
import time
import gzip
import json
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
BASE_URL = "https://fullbai.com.ar"
ARQUIVO_CACHE = "lista_urls.json"
TOKEN_SECRETO = "fullbai123"
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
    if len(logs_memoria) > 2000:
        logs_memoria.pop()

def get_urls_via_regex(text_content):
    return re.findall(r'<loc>(.*?)</loc>', text_content)

def carregar_do_cache():
    if os.path.exists(ARQUIVO_CACHE):
        try:
            with open(ARQUIVO_CACHE, 'r') as f:
                dados = json.load(f)
                return dados
        except:
            return None
    return None

def salvar_no_cache(lista_urls):
    try:
        with open(ARQUIVO_CACHE, 'w') as f:
            json.dump(lista_urls, f)
        adicionar_log("LISTA SALVA NO DISCO COM SUCESSO.")
    except Exception as e:
        adicionar_log(f"Erro ao salvar cache: {e}")

def processar_sitemap_individual(url_sitemap):
    produtos_encontrados = set()
    try:
        adicionar_log(f"Lendo: {url_sitemap} ...")
        time.sleep(0.5) 
        # AUMENTEI O TIMEOUT PARA 90 SEGUNDOS (Para nao dar erro no seu server)
        r = requests.get(url_sitemap, headers=HEADERS_FAKE, timeout=90, verify=False)
        if r.status_code != 200:
            adicionar_log(f"Erro/Vazio: {url_sitemap}")
            return None
        content = r.content
        if url_sitemap.endswith('.gz'):
            try: content = gzip.decompress(content)
            except: pass
        texto = content.decode('utf-8', errors='ignore')
        links = get_urls_via_regex(texto)
        for link in links:
            link = link.strip()
            if not ('sitemap' in link and link.endswith('.xml')):
                produtos_encontrados.add(link)
    except Exception as e:
        adicionar_log(f"Erro ao ler {url_sitemap}: {str(e)}")
        return None
    return produtos_encontrados

def scanner_inteligente():
    urls_finais = []
    visitados = set()
    
    adicionar_log("--- CRIANDO NOVA LISTA (Isso acontece pq a lista nao existia) ---")

    # Fase 1: Páginas
    prods = processar_sitemap_individual(f"{BASE_URL}/page-sitemap.xml")
    if prods: urls_finais.extend(list(prods))

    # Fase 2: Produtos
    erros_seguidos = 0
    for i in range(1, 300):
        if erros_seguidos >= 3: 
            adicionar_log(f"Parando leitura no sitemap {i} (fim dos produtos).")
            break
        url = f"{BASE_URL}/product-sitemap{i}.xml"
        resultado = processar_sitemap_individual(url)
        if resultado is None or len(resultado) == 0:
            erros_seguidos += 1
        else:
            erros_seguidos = 0
            novos = 0
            for p in resultado:
                if p not in visitados:
                    visitados.add(p)
                    urls_finais.append(p)
                    novos += 1
            if novos > 0:
                adicionar_log(f"-> Adicionados +{novos} URLs do mapa {i}")
    
    # Fase 3: Categorias
    cats = processar_sitemap_individual(f"{BASE_URL}/product_cat-sitemap.xml")
    if cats:
        for c in cats:
            if c not in visitados: urls_finais.append(c)

    return urls_finais

async def fetch_url(session, url):
    try:
        # Timeout curto para visitas (30s)
        async with session.get(url, headers=HEADERS_FAKE, timeout=30, ssl=False) as response:
            await response.read()
    except: pass

async def worker_logic(forcar_atualizacao=False):
    global status_global
    lista_urls = []
    
    # Tenta carregar cache primeiro
    if not forcar_atualizacao:
        lista_urls = carregar_do_cache()
    
    # Se nao tiver cache, somos OBRIGADOS a escanear
    if not lista_urls:
        adicionar_log("AVISO: Lista não encontrada. Precisamos escanear 1 vez para criar o arquivo.")
        loop = asyncio.get_running_loop()
        lista_urls = await loop.run_in_executor(None, scanner_inteligente)
        if lista_urls: salvar_no_cache(lista_urls)
    else:
        adicionar_log(f"Usando lista salva ({len(lista_urls)} URLs). Modo Rápido.")
    
    total = len(lista_urls)
    if total == 0:
        status_global = "PARADO"
        return

    semaphore = asyncio.Semaphore(50) 
    async with aiohttp.ClientSession() as session:
        async def bound_fetch(url):
            async with semaphore: await fetch_url(session, url)
        tarefas = []
        for i, url in enumerate(lista_urls):
            tarefas.append(bound_fetch(url))
            if i > 0 and i % 500 == 0:
                adicionar_log(f"Progresso: {i}/{total} visitas...")
        await asyncio.gather(*tarefas)
    
    adicionar_log("--- CONCLUÍDO ---")
    status_global = "CONCLUÍDO"

def run_background_thread(forcar=False):
    global status_global
    status_global = "RODANDO"
    logs_memoria.clear()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try: loop.run_until_complete(worker_logic(forcar))
    finally:
        loop.close()
        if status_global == "RODANDO": status_global = "PARADO"

@app.route('/')
def index(): return redirect(url_for('monitorar'))

@app.route('/iniciar')
def iniciar():
    global status_global
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background_thread, args=(False,))
        t.start()
    return redirect(url_for('monitorar'))

@app.route('/atualizar')
def atualizar():
    global status_global
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background_thread, args=(True,))
        t.start()
    return redirect(url_for('monitorar'))

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    global status_global
    token = request.args.get('token')
    if token != TOKEN_SECRETO: return jsonify({"status": "erro", "msg": "Senha incorreta"}), 403
    
    if status_global == "RODANDO":
        return jsonify({"status": "ocupado", "msg": "Ja esta rodando"}), 200
    
    # WEBHOOK SEMPRE TENTA O MODO RÁPIDO (False)
    t = threading.Thread(target=run_background_thread, args=(False,))
    t.start()
    
    return jsonify({"status": "sucesso", "msg": "Robo iniciado via Webhook"}), 200

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
            .container {{ max_width: 1000px; margin: 0 auto; }}
            .box {{ background: #333; padding: 20px; border: 1px solid #444; border-radius: 8px; height: 600px; overflow-y: scroll; display: flex; flex-direction: column-reverse; }}
            .btn {{ padding: 15px 30px; text-decoration: none; font-size: 18px; display: inline-block; margin-bottom: 20px; border-radius: 5px; margin-right: 10px; color:white; }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Gerador V10 (Timeout Ajustado)</h1>
            <p>Status: <b style="color:{cor}">{status_global}</b></p>
            <a href="/iniciar" class="btn" style="background:green">INICIAR</a>
            <a href="/atualizar" class="btn" style="background:orange">ATUALIZAR LISTA (Manual)</a>
            <div class="box"><div>{log_html}</div></div>
        </div>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
