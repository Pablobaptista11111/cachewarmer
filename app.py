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
import datetime
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# --- CONFIGURAÇÃO V20 (LOG EM ARQUIVO) ---
BASE_URL = "https://fullbai.com.ar"
TOKEN_SECRETO = "fullbai123"
DATA_DIR = "/app/data"
ARQUIVO_CACHE = os.path.join(DATA_DIR, "lista_urls.json")
ARQUIVO_LOG = os.path.join(DATA_DIR, "log_visual.txt") # Log agora fica no disco
HORA_AGENDADA = "04:00" 
# ------------------------

app = Flask(__name__)

status_global = "PARADO"
fila_sniper = queue.Queue()

HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

# --- SISTEMA DE LOG NOVO (GRAVA NO DISCO) ---
def adicionar_log(msg):
    timestamp = time.strftime("%H:%M:%S")
    linha = f"[{timestamp}] {msg}"
    print(linha, flush=True) # Escreve na tela preta
    
    try:
        # Escreve no arquivo de texto (append)
        with open(ARQUIVO_LOG, "a") as f:
            f.write(linha + "\n")
    except:
        pass

def ler_logs_do_disco():
    """Lê as ultimas 100 linhas do arquivo de log"""
    if not os.path.exists(ARQUIVO_LOG):
        return "Aguardando logs..."
    try:
        with open(ARQUIVO_LOG, "r") as f:
            linhas = f.readlines()
            # Pega as ultimas 200 e inverte (mais recente no topo)
            return "<br>".join(reversed(linhas[-200:]))
    except:
        return "Erro ao ler logs."

def limpar_logs():
    try:
        with open(ARQUIVO_LOG, "w") as f:
            f.write(f"[{time.strftime('%H:%M:%S')}] --- LOG LIMPO ---\n")
    except: pass

# --- INICIALIZAÇÃO ---
if not os.path.exists(DATA_DIR):
    try: os.makedirs(DATA_DIR)
    except: pass

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
        adicionar_log(f"💾 LISTA SALVA: {len(lista_urls)} URLs no disco.")
    except Exception as e:
        adicionar_log(f"❌ ERRO SALVAR: {e}")

def processar_sitemap_individual(url_sitemap, filtro=None):
    produtos = set()
    try:
        adicionar_log(f"Lendo sitemap: {url_sitemap}...")
        r = requests.get(url_sitemap, headers=HEADERS_FAKE, timeout=90, verify=False)
        if r.status_code == 200:
            texto = r.content.decode('utf-8', errors='ignore')
            if url_sitemap.endswith('.gz'):
                try: texto = gzip.decompress(r.content).decode('utf-8')
                except: pass
            links = get_urls_via_regex(texto)
            for l in links: 
                l = l.strip()
                if filtro:
                    if filtro in l: produtos.add(l)
                else:
                    if not ('sitemap' in l and l.endswith('.xml')): produtos.add(l)
            adicionar_log(f"-> Encontrados {len(produtos)} itens.")
    except Exception as e:
        adicionar_log(f"Erro ao ler {url_sitemap}: {e}")
    return produtos

def scanner_inteligente():
    urls = []
    visitados = set()
    def add(novas):
        count = 0
        for u in novas:
            if u not in visitados:
                visitados.add(u)
                urls.append(u)
                count += 1
        return count

    adicionar_log("🔍 INICIANDO SCANNER...")
    
    # Principales
    qtd = add(processar_sitemap_individual(f"{BASE_URL}/product_cat-sitemap.xml", filtro="principales"))
    
    # Paginas
    qtd += add(processar_sitemap_individual(f"{BASE_URL}/page-sitemap.xml"))
    
    # Produtos
    erros = 0
    adicionar_log("🔍 Varrendo produtos (1 ao 300)...")
    for i in range(1, 300):
        if erros >= 3: 
            adicionar_log(f"Parece que acabou no sitemap {i}.")
            break
        
        # Log a cada 10 para saber que está vivo
        if i % 10 == 0: adicionar_log(f"Checando sitemap {i}...")
            
        res = processar_sitemap_individual(f"{BASE_URL}/product-sitemap{i}.xml")
        if not res: erros += 1
        else:
            erros = 0
            add(res)
            
    return urls

async def fetch_url(session, url):
    try:
        async with session.get(url, headers=HEADERS_FAKE, timeout=30, ssl=False) as response:
            await response.read()
            return response.status
    except: return 0

# --- WORKERS ---
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
    
    adicionar_log(f"⚙️ Worker Iniciado. Modo Forçar: {forcar_atualizacao}")
    
    if not forcar_atualizacao: 
        lista_urls = carregar_do_cache()
        if lista_urls: adicionar_log(f"📂 Cache carregado: {len(lista_urls)} URLs.")
    
    if not lista_urls:
        adicionar_log("⚠️ Cache vazio ou inválido. Iniciando Scanner Completo...")
        loop = asyncio.get_running_loop()
        lista_urls = await loop.run_in_executor(None, scanner_inteligente)
        if lista_urls: salvar_no_cache(lista_urls)
    
    if not lista_urls:
        status_global = "PARADO"
        adicionar_log("❌ Nenhuma URL encontrada para visitar.")
        return

    total = len(lista_urls)
    adicionar_log(f"🚀 DISPARANDO 50 ROBÔS PARA {total} URLS...")
    
    semaphore = asyncio.Semaphore(50) 
    contador = 0
    async with aiohttp.ClientSession() as session:
        async def bound(url):
            nonlocal contador
            async with semaphore: 
                await fetch_url(session, url)
                contador += 1
                if contador % 200 == 0: adicionar_log(f"⚡ Progresso: {contador}/{total}...")
        
        tarefas = [bound(u) for u in lista_urls]
        await asyncio.gather(*tarefas)
    
    status_global = "CONCLUÍDO"
    adicionar_log("🏁 CICLO FINALIZADO COM SUCESSO.")

def run_background(forcar=False):
    global status_global
    status_global = "RODANDO"
    # NÃO LIMPA MAIS O LOG AQUI
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try: loop.run_until_complete(worker_logic(forcar))
    except Exception as e: adicionar_log(f"❌ ERRO FATAL: {str(e)}")
    finally:
        loop.close()
        if status_global == "RODANDO": status_global = "PARADO"

def agendador_automatico():
    adicionar_log(f"⏰ Despertador ativado para {HORA_AGENDADA} UTC")
    while True:
        try:
            agora = datetime.datetime.utcnow().strftime("%H:%M")
            if agora == HORA_AGENDADA:
                if status_global != "RODANDO":
                    adicionar_log("⏰ HORÁRIO AGENDADO: Iniciando atualização total...")
                    threading.Thread(target=run_background, args=(True,)).start()
                    time.sleep(61)
            time.sleep(30)
        except: time.sleep(30)

t_relogio = threading.Thread(target=agendador_automatico, daemon=True)
t_relogio.start()

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
        # Limpa log apenas ao forçar atualização nova
        limpar_logs() 
        t = threading.Thread(target=run_background, args=(True,))
        t.start()
    return redirect(url_for('monitorar'))

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    if request.args.get('token') != TOKEN_SECRETO: return jsonify({"erro": "Token"}), 403
    forcar = request.args.get('forcar') == 'sim'
    if status_global != "RODANDO":
        if forcar: limpar_logs()
        t = threading.Thread(target=run_background, args=(forcar,))
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
    # LÊ DO ARQUIVO FÍSICO
    log_html = ler_logs_do_disco()
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
        <h1>Gerador V20 (Log em Arquivo)</h1>
        <p>Status: <b style="color:{cor}">{status_global}</b></p>
        <a href="/iniciar" class="btn" style="background:green">INICIAR (RÁPIDO)</a>
        <a href="/atualizar" class="btn" style="background:orange">ATUALIZAR TUDO (LENTO)</a>
        <div class="box">
            <div>{log_html}</div>
        </div>
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
