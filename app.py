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
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO ---
BASE_URL = "https://fullbai.com.ar"
TOKEN_SECRETO = "fullbai123"
DATA_DIR = "/app/data"
ARQUIVO_CACHE = os.path.join(DATA_DIR, "lista_urls.json")

# Tenta criar a pasta se não existir
if not os.path.exists(DATA_DIR):
    try: os.makedirs(DATA_DIR)
    except: pass
# --------------------

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

# --- FERRAMENTA DE DIAGNÓSTICO (NOVO) ---
def testar_disco_agora():
    adicionar_log("--- INICIANDO TESTE DE DISCO ---")
    adicionar_log(f"Tentando escrever em: {DATA_DIR}")
    
    caminho_teste = os.path.join(DATA_DIR, "teste_permissao.txt")
    
    # Teste 1: Escrita
    try:
        with open(caminho_teste, "w") as f:
            f.write("Teste de escrita OK")
        adicionar_log("✅ SUCESSO: Arquivo criado.")
    except Exception as e:
        adicionar_log(f"❌ ERRO CRÍTICO DE ESCRITA: {e}")
        return

    # Teste 2: Leitura
    try:
        with open(caminho_teste, "r") as f:
            conteudo = f.read()
        if conteudo == "Teste de escrita OK":
            adicionar_log("✅ SUCESSO: Arquivo lido corretamente.")
        else:
            adicionar_log("❌ ERRO: Conteúdo lido incorreto.")
    except Exception as e:
        adicionar_log(f"❌ ERRO DE LEITURA: {e}")

    # Teste 3: Listar arquivos
    try:
        arquivos = os.listdir(DATA_DIR)
        adicionar_log(f"📂 Arquivos na pasta agora: {arquivos}")
    except:
        pass
        
    adicionar_log("--- FIM DO TESTE ---")

# ... (Resto das funções auxiliares iguais: regex, cache, scanner) ...
def get_urls_via_regex(text_content):
    return re.findall(r'<loc>(.*?)</loc>', text_content)

def carregar_do_cache():
    if os.path.exists(ARQUIVO_CACHE):
        try:
            with open(ARQUIVO_CACHE, 'r') as f: 
                dados = json.load(f)
                adicionar_log(f"MEMÓRIA CARREGADA: {len(dados)} URLs.")
                return dados
        except Exception as e: 
            adicionar_log(f"Erro ao ler cache: {e}")
            return None
    return None

def salvar_no_cache(lista_urls):
    try:
        with open(ARQUIVO_CACHE, 'w') as f: json.dump(lista_urls, f)
        adicionar_log(f"LISTA SALVA EM {ARQUIVO_CACHE}.")
    except Exception as e:
        adicionar_log(f"ERRO AO SALVAR NO DISCO: {e}")

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
    except: pass

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

async def worker_logic(forcar_atualizacao=False):
    global status_global
    lista_urls = []
    if not forcar_atualizacao: lista_urls = carregar_do_cache()
    if not lista_urls:
        adicionar_log("Arquivo nao existe. Iniciando Scanner...")
        loop = asyncio.get_running_loop()
        lista_urls = await loop.run_in_executor(None, scanner_inteligente)
        if lista_urls: salvar_no_cache(lista_urls)
    
    if not lista_urls:
        status_global = "PARADO"
        return
    
    # Salva de novo só pra garantir que a permissão tá ok
    salvar_no_cache(lista_urls) 

    semaphore = asyncio.Semaphore(50) 
    async with aiohttp.ClientSession() as session:
        async def bound(url):
            async with semaphore: await fetch_url(session, url)
        tarefas = []
        total = len(lista_urls)
        for i, url in enumerate(lista_urls):
            tarefas.append(bound(url))
            if i > 0 and i % 500 == 0: adicionar_log(f"Progresso: {i}/{total}...")
        await asyncio.gather(*tarefas)
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

# --- ROTA DE TESTE ---
@app.route('/testar_disco')
def testar_disco():
    testar_disco_agora()
    return redirect(url_for('monitorar'))

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    if request.args.get('token') != TOKEN_SECRETO: return jsonify({"erro": "Token invalido"}), 403
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background, args=(False,))
        t.start()
    return jsonify({"msg": "Rodando"}), 200

@app.route('/webhook_unitario', methods=['POST'])
def webhook_unitario():
    dados = request.json
    if dados.get('token') != TOKEN_SECRETO: return jsonify({"erro": "Token invalido"}), 403
    if dados.get('url'): fila_sniper.put(dados.get('url'))
    return jsonify({"msg": "Na fila"}), 200

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
            .btn {{ padding: 15px 30px; text-decoration: none; font-size: 18px; display: inline-block; margin-bottom: 20px; border-radius: 5px; margin-right: 10px; color:white; font-weight: bold; }}
        </style>
    </head>
    <body>
        <h1>Gerador V14 (Teste de Disco)</h1>
        <p>Status: <b style="color:{cor}">{status_global}</b></p>
        
        <a href="/testar_disco" class="btn" style="background:#007bff">1. TESTAR DISCO</a>
        <a href="/atualizar" class="btn" style="background:orange">2. CRIAR LISTA</a>
        <a href="/iniciar" class="btn" style="background:green">3. INICIAR</a>
        
        <div class="box"><div>{log_html}</div></div>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
