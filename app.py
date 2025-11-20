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
import queue  # <--- NOVO: Para gerenciar a fila
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

# FILA DE SNIPER (Para aguentar atualizações em massa)
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
    if len(logs_memoria) > 2000:
        logs_memoria.pop()

# ... (Funções auxiliares iguais: regex, carregar_do_cache, salvar_no_cache) ...
def get_urls_via_regex(text_content):
    return re.findall(r'<loc>(.*?)</loc>', text_content)

def carregar_do_cache():
    if os.path.exists(ARQUIVO_CACHE):
        try:
            with open(ARQUIVO_CACHE, 'r') as f: return json.load(f)
        except: return None
    return None

def salvar_no_cache(lista_urls):
    try:
        with open(ARQUIVO_CACHE, 'w') as f: json.dump(lista_urls, f)
        adicionar_log("LISTA SALVA NO DISCO.")
    except: pass

def processar_sitemap_individual(url_sitemap):
    produtos = set()
    try:
        r = requests.get(url_sitemap, headers=HEADERS_FAKE, timeout=90, verify=False)
        if r.status_code == 200:
            texto = r.content.decode('utf-8', errors='ignore')
            if url_sitemap.endswith('.gz'):
                try: texto = gzip.decompress(r.content).decode('utf-8')
                except: pass
            links = get_urls_via_regex(texto)
            for l in links: 
                if not ('sitemap' in l): produtos.add(l.strip())
    except: pass
    return produtos

def scanner_inteligente():
    # Lógica de varredura igual a V11
    urls = []
    visitados = set()
    p = processar_sitemap_individual(f"{BASE_URL}/page-sitemap.xml")
    if p: urls.extend(list(p))
    
    erros = 0
    for i in range(1, 300):
        if erros >= 3: break
        res = processar_sitemap_individual(f"{BASE_URL}/product-sitemap{i}.xml")
        if not res: erros += 1
        else:
            erros = 0
            for x in res:
                if x not in visitados:
                    visitados.add(x)
                    urls.append(x)
    
    c = processar_sitemap_individual(f"{BASE_URL}/product_cat-sitemap.xml")
    if c:
        for x in c: 
            if x not in visitados: urls.append(x)
    return urls

async def fetch_url(session, url):
    try:
        async with session.get(url, headers=HEADERS_FAKE, timeout=30, ssl=False) as response:
            await response.read()
            return response.status
    except: return 0

# --- NOVO: WORKER DA FILA (SNIPER CONSTANTE) ---
def processador_de_fila():
    """Fica rodando eternamente esperando URLs na fila"""
    adicionar_log("Sistema de Fila Sniper: ATIVO")
    
    # Cria um loop async só para esse trabalhador
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    async def visitar_batch():
        while True:
            # Pega URL da fila (espera até aparecer se estiver vazia)
            # Se vierem 500 de uma vez, ele processa uma por uma aqui
            try:
                url = fila_sniper.get() # Bloqueia até ter item
                
                adicionar_log(f"🎯 SNIPER: Processando {url}...")
                async with aiohttp.ClientSession() as session:
                    await fetch_url(session, url)
                
                # Mostra quantos ainda faltam na fila
                pendentes = fila_sniper.qsize()
                if pendentes > 0:
                    adicionar_log(f"⏳ Fila: restam {pendentes} produtos...")
                else:
                    adicionar_log(f"✅ SNIPER: Fila limpa!")
                    
                fila_sniper.task_done()
                
                # Pequena pausa para respirar
                await asyncio.sleep(0.1)
                
            except Exception as e:
                adicionar_log(f"Erro na fila: {e}")

    loop.run_until_complete(visitar_batch())

# Inicia o processador de fila assim que o app liga
t_fila = threading.Thread(target=processador_de_fila, daemon=True)
t_fila.start()

# --- WORKER GERAL (BOTAO INICIAR) ---
async def worker_logic(forcar=False):
    global status_global
    lista = []
    if not forcar: lista = carregar_do_cache()
    if not lista:
        adicionar_log("Escaneando loja completa...")
        loop = asyncio.get_running_loop()
        lista = await loop.run_in_executor(None, scanner_inteligente)
        if lista: salvar_no_cache(lista)
    
    if not lista:
        status_global = "PARADO"
        return

    total = len(lista)
    semaphore = asyncio.Semaphore(50)
    async with aiohttp.ClientSession() as session:
        async def bound(url):
            async with semaphore: await fetch_url(session, url)
        tasks = [bound(u) for u in lista]
        for i, _ in enumerate(as_completed(tasks)):
             if i % 500 == 0: adicionar_log(f"Geral: {i}/{total}...")
        await asyncio.gather(*tasks)
    
    status_global = "CONCLUÍDO"

from asyncio import as_completed

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
    token = request.args.get('token')
    if token != TOKEN_SECRETO: return jsonify({"erro": "Token invalido"}), 403
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background, args=(False,))
        t.start()
    return jsonify({"msg": "Rodando Completo"}), 200

@app.route('/webhook_unitario', methods=['POST'])
def webhook_unitario():
    dados = request.json
    token = dados.get('token')
    url = dados.get('url')
    
    if token != TOKEN_SECRETO: return jsonify({"erro": "Token invalido"}), 403
    if not url: return jsonify({"erro": "Sem URL"}), 400
    
    # AGORA É SEGURO: Apenas adiciona na fila e retorna sucesso instantaneo
    fila_sniper.put(url)
    
    qtd = fila_sniper.qsize()
    return jsonify({"msg": "Adicionado na fila", "posicao": qtd}), 200

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
            .btn {{ padding: 15px 30px; text-decoration: none; font-size: 18px; display: inline-block; margin-bottom: 20px; border-radius: 5px; margin-right: 10px; color:white; }}
        </style>
    </head>
    <body>
        <h1>Gerador V12 (Fila Anti-Travamento)</h1>
        <p>Status Geral: <b style="color:{cor}">{status_global}</b></p>
        <p>Sniper: <b>ATIVO (Aguardando Webhooks)</b></p>
        <a href="/iniciar" class="btn" style="background:green">INICIAR COMPLETO</a>
        <a href="/atualizar" class="btn" style="background:orange">ATUALIZAR LISTA</a>
        <div class="box"><div>{log_html}</div></div>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
