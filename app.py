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
import queue
import sys
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO V17 (PRIORIDADE PRINCIPALES) ---
BASE_URL = "https://fullbai.com.ar"
TOKEN_SECRETO = "fullbai123"
DATA_DIR = "/app/data"
ARQUIVO_CACHE = os.path.join(DATA_DIR, "lista_urls.json")
# ------------------------

app = Flask(__name__)

status_global = "PARADO"
logs_memoria = []
fila_sniper = queue.Queue()

HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def adicionar_log(msg):
    timestamp = time.strftime("%H:%M:%S")
    linha = f"[{timestamp}] {msg}"
    print(linha, flush=True)
    logs_memoria.insert(0, linha)
    if len(logs_memoria) > 2000: logs_memoria.pop()

# Cria pasta se não existir
if not os.path.exists(DATA_DIR):
    try: os.makedirs(DATA_DIR)
    except: pass

def get_urls_via_regex(text_content):
    return re.findall(r'<loc>(.*?)</loc>', text_content)

def carregar_do_cache():
    if os.path.exists(ARQUIVO_CACHE):
        try:
            with open(ARQUIVO_CACHE, 'r') as f: return json.load(f)
        except Exception as e:
            adicionar_log(f"Erro leitura: {e}")
            return None
    return None

def salvar_no_cache(lista_urls):
    try:
        with open(ARQUIVO_CACHE, 'w') as f: json.dump(lista_urls, f)
        adicionar_log(f"LISTA SALVA: {len(lista_urls)} URLs (Ordem Personalizada).")
    except Exception as e:
        adicionar_log(f"❌ ERRO SALVAR: {e}")

def processar_sitemap_individual(url_sitemap, filtro=None):
    """Baixa sitemap e opcionalmente filtra URLs por uma palavra"""
    urls_encontradas = []
    try:
        # Timeout alto para garantir leitura
        r = requests.get(url_sitemap, headers=HEADERS_FAKE, timeout=60, verify=False)
        if r.status_code == 200:
            texto = r.content.decode('utf-8', errors='ignore')
            if url_sitemap.endswith('.gz'):
                try: texto = gzip.decompress(r.content).decode('utf-8')
                except: pass
            
            links = get_urls_via_regex(texto)
            
            for l in links: 
                l = l.strip()
                # Se tiver filtro, só adiciona se a URL conter a palavra
                if filtro:
                    if filtro in l:
                        urls_encontradas.append(l)
                else:
                    # Se não for link de sitemap aninhado
                    if not ('sitemap' in l and l.endswith('.xml')):
                        urls_encontradas.append(l)
    except: pass
    return urls_encontradas

def scanner_personalizado():
    """Lógica de Prioridade V17"""
    lista_final = []
    visitados = set()

    def adicionar_na_lista(novas_urls):
        count = 0
        for u in novas_urls:
            if u not in visitados:
                visitados.add(u)
                lista_final.append(u)
                count += 1
        return count

    # --- FASE 1: CATEGORIAS "PRINCIPALES" ---
    adicionar_log("1. Buscando 'Principales' no sitemap de categorias...")
    cats = processar_sitemap_individual(f"{BASE_URL}/product_cat-sitemap.xml", filtro="principales")
    qtd = adicionar_na_lista(cats)
    adicionar_log(f"-> Encontradas {qtd} categorias/subcategorias Principales.")

    # --- FASE 2: PÁGINAS (Institucionais) ---
    adicionar_log("2. Buscando Páginas...")
    pags = processar_sitemap_individual(f"{BASE_URL}/page-sitemap.xml")
    qtd = adicionar_na_lista(pags)
    adicionar_log(f"-> Encontradas {qtd} páginas.")

    # --- FASE 3: PRODUTOS (Todos) ---
    adicionar_log("3. Buscando Produtos (Do 1 ao 300)...")
    erros = 0
    total_produtos = 0
    for i in range(1, 300):
        if erros >= 3: 
            adicionar_log(f"Fim dos sitemaps de produtos (Sitemap {i}).")
            break
        
        url = f"{BASE_URL}/product-sitemap{i}.xml"
        prods = processar_sitemap_individual(url)
        
        if not prods: 
            erros += 1
        else:
            erros = 0
            qtd = adicionar_na_lista(prods)
            total_produtos += qtd
            # Aviso a cada 10 sitemaps lidos para não poluir
            if i % 10 == 0:
                adicionar_log(f"Lendo sitemap {i}... (Total produtos: {total_produtos})")

    return lista_final

async def fetch_url(session, url):
    try:
        async with session.get(url, headers=HEADERS_FAKE, timeout=30, ssl=False) as response:
            await response.read()
            return response.status
    except: return 0

# --- WORKER FILA ---
def processador_de_fila():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    async def visitar_batch():
        while True:
            try:
                url = fila_sniper.get()
                adicionar_log(f"🎯 SNIPER: {url}")
                async with aiohttp.ClientSession() as session:
                    await fetch_url(session, url)
                fila_sniper.task_done()
                await asyncio.sleep(0.05)
            except: pass
    loop.run_until_complete(visitar_batch())

t_fila = threading.Thread(target=processador_de_fila, daemon=True)
t_fila.start()

# --- WORKER GERAL ---
async def worker_logic(forcar_atualizacao=False):
    global status_global
    lista_urls = []
    
    if not forcar_atualizacao: lista_urls = carregar_do_cache()
    if not lista_urls:
        adicionar_log("Criando lista personalizada...")
        loop = asyncio.get_running_loop()
        lista_urls = await loop.run_in_executor(None, scanner_personalizado)
        if lista_urls: salvar_no_cache(lista_urls)
    
    if not lista_urls:
        status_global = "PARADO"
        adicionar_log("Nenhuma URL encontrada.")
        return

    total = len(lista_urls)
    adicionar_log(f"--- INICIANDO VISITA: {total} URLS (Ordem Prioritária) ---")
    
    semaphore = asyncio.Semaphore(50)
    contador = 0
    
    async with aiohttp.ClientSession() as session:
        async def visitador(url):
            nonlocal contador
            async with semaphore:
                await fetch_url(session, url)
                contador += 1
                if contador % 200 == 0:
                    adicionar_log(f"🚀 Progresso: {contador}/{total}...")

        tarefas = [visitador(url) for url in lista_urls]
        await asyncio.gather(*tarefas)
    
    adicionar_log("--- CICLO COMPLETO FINALIZADO ---")
    status_global = "CONCLUÍDO"

def run_background(forcar=False):
    global status_global
    status_global = "RODANDO"
    logs_memoria.clear()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try: loop.run_until_complete(worker_logic(forcar))
    finally:
        loop.close()
        if status_global == "RODANDO": status_global = "PARADO"

# --- ROTAS ---
@app.route('/')
def index(): return redirect(url_for('monitorar'))

@app.route('/iniciar')
def iniciar():
    global status_global
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background, args=(False,))
        t.start()
    return redirect(url_for('monitorar'))

@app.route('/atualizar')
def atualizar():
    global status_global
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background, args=(True,))
        t.start()
    return redirect(url_for('monitorar'))

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    if request.args.get('token') != TOKEN_SECRETO: return jsonify({"erro": "Token"}), 403
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background, args=(False,))
        t.start()
    return jsonify({"msg": "Rodando"}), 200

@app.route('/webhook_unitario', methods=['POST'])
def webhook_unitario():
    d = request.json
    if d.get('token') != TOKEN_SECRETO: return jsonify({"erro": "Token"}), 403
    if d.get('url'): fila_sniper.put(d.get('url'))
    return jsonify({"msg": "Ok"}), 200

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
            .box {{ background: #333; padding: 20px; border: 1px solid #444; border-radius: 8px; height: 600px; overflow-y: scroll; display: flex; flex-direction: column-reverse; }}
            .btn {{ padding: 15px 30px; text-decoration: none; font-size: 18px; display: inline-block; margin-bottom: 20px; border-radius: 5px; margin-right: 10px; color:white; font-weight:bold; cursor:pointer; }}
        </style>
    </head>
    <body>
        <h1>Gerador V17 (Prioridade: Principales)</h1>
        <p>Status: <b style="color:{cor}">{status_global}</b></p>
        <a href="/iniciar" class="btn" style="background:green">INICIAR (CACHE)</a>
        <a href="/atualizar" class="btn" style="background:orange">CRIAR LISTA (NOVA ORDEM)</a>
        <div class="box"><div>{log_html}</div></div>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
