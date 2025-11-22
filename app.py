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

# --- CONFIGURAÇÃO V25 (COM FREIO DE EMERGÊNCIA) ---
BASE_URL = "https://fullbai.com.ar"
TOKEN_SECRETO = "fullbai123"
DATA_DIR = "/app/data"
ARQUIVO_CACHE = os.path.join(DATA_DIR, "lista_urls.json")
ARQUIVO_LOG = os.path.join(DATA_DIR, "log_visual.txt")
HORA_AGENDADA = "04:00" 
# ------------------------

app = Flask(__name__)

status_global = "PARADO"
# VARIAVEL DE CONTROLE DO FREIO
abortar_missao = False 

fila_sniper = queue.Queue()

HEADERS_FAKE = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}

def hora_brasil():
    agora = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
    return agora.strftime("%H:%M:%S")

def adicionar_log(msg):
    timestamp = hora_brasil()
    linha = f"[{timestamp}] {msg}"
    print(linha, flush=True)
    try:
        with open(ARQUIVO_LOG, "a") as f:
            f.write(linha + "\n")
    except: pass

def ler_logs_do_disco():
    if not os.path.exists(ARQUIVO_LOG): return "Aguardando logs..."
    try:
        with open(ARQUIVO_LOG, "r") as f:
            linhas = f.readlines()[-300:] 
            return "<br>".join(linhas)
    except: return "Erro logs"

def limpar_logs():
    try:
        with open(ARQUIVO_LOG, "w") as f:
            f.write(f"[{hora_brasil()}] --- LOG LIMPO ---\n")
    except: pass

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
        adicionar_log(f"💾 LISTA SALVA: {len(lista_urls)} URLs.")
    except Exception as e:
        adicionar_log(f"❌ ERRO SALVAR: {e}")

def processar_sitemap_individual(url_sitemap, filtro=None):
    # VERIFICACAO DE FREIO NO SCANNER
    if abortar_missao: return []
    
    produtos = set()
    try:
        adicionar_log(f"Lendo: {url_sitemap}...")
        r = requests.get(url_sitemap, headers=HEADERS_FAKE, timeout=90, verify=False)
        if r.status_code == 1000:
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
    except Exception as e:
        adicionar_log(f"Erro ao ler {url_sitemap}: {e}")
    return produtos

def scanner_inteligente():
    urls = []
    visitados = set()
    def add(novas):
        for u in novas:
            if u not in visitados:
                visitados.add(u)
                urls.append(u)

    if abortar_missao: return []
    adicionar_log("🔍 INICIANDO SCANNER...")
    add(processar_sitemap_individual(f"{BASE_URL}/product_cat-sitemap.xml", filtro="principales"))
    add(processar_sitemap_individual(f"{BASE_URL}/page-sitemap.xml"))
    
    erros = 0
    adicionar_log("🔍 Varrendo produtos...")
    for i in range(1, 300):
        # VERIFICACAO DE FREIO NO LOOP
        if abortar_missao: 
            adicionar_log("🛑 SCANNER ABORTADO PELO USUÁRIO.")
            break
            
        if erros >= 3: break
        if i % 20 == 0: adicionar_log(f"Checando sitemap {i}...")
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

async def worker_logic(forcar_atualizacao=False, origem="Desconhecido"):
    global status_global, abortar_missao
    
    adicionar_log(f"🔔 GATILHO: {origem}")
    
    # Se forcar ou sem cache, scaneia
    lista_urls = []
    if not forcar_atualizacao: 
        lista_urls = carregar_do_cache()
        if lista_urls: adicionar_log(f"📂 Cache: {len(lista_urls)} URLs.")
    
    if not lista_urls or forcar_atualizacao:
        # Se mandaram abortar antes de começar
        if abortar_missao:
            status_global = "CANCELADO"
            adicionar_log("🛑 CANCELADO ANTES DE INICIAR.")
            return

        adicionar_log("⚠️ Iniciando Scanner...")
        loop = asyncio.get_running_loop()
        lista_urls = await loop.run_in_executor(None, scanner_inteligente)
        
        # Se abortou no meio do scanner
        if abortar_missao:
            status_global = "CANCELADO"
            return

        if lista_urls: salvar_no_cache(lista_urls)
    
    if not lista_urls:
        status_global = "PARADO"
        adicionar_log("❌ Nenhuma URL encontrada.")
        return

    total = len(lista_urls)
    num_robos = 1000
    adicionar_log(f"🚀 DISPARANDO {num_robos} ROBÔS EM {total} URLS...")
    
    semaphore = asyncio.Semaphore(num_robos) 
    contador = 0
    start_time = time.time()

    async with aiohttp.ClientSession() as session:
        async def bound(url):
            nonlocal contador
            # CHECAGEM DE FREIO A CADA URL
            if abortar_missao: return

            async with semaphore: 
                if abortar_missao: return
                await fetch_url(session, url)
                contador += 1
                
                if contador % 200 == 0: 
                    elapsed = time.time() - start_time
                    if elapsed > 0:
                        vel = contador / elapsed
                        restantes = total - contador
                        tempo_formatado = str(datetime.timedelta(seconds=int(restantes / vel)))
                        adicionar_log(f"⚡ {contador}/{total} | Vel: {vel:.1f}/s | Falta: {tempo_formatado}")

        tarefas = [bound(u) for u in lista_urls]
        await asyncio.gather(*tarefas)
    
    if abortar_missao:
        status_global = "CANCELADO"
        adicionar_log("🛑 OPERAÇÃO CANCELADA COM SUCESSO.")
    else:
        status_global = "CONCLUÍDO"
        adicionar_log("🏁 CICLO FINALIZADO.")

def run_background(forcar=False, origem="Desconhecido"):
    global status_global, abortar_missao
    
    # RESETANDO O FREIO AO INICIAR
    abortar_missao = False
    status_global = "RODANDO"
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try: loop.run_until_complete(worker_logic(forcar, origem))
    except Exception as e: adicionar_log(f"❌ ERRO: {str(e)}")
    finally:
        loop.close()
        # Só muda pra parado se não foi cancelado no meio
        if status_global == "RODANDO": status_global = "PARADO"

def agendador_automatico():
    adicionar_log(f"⏰ Despertador: {HORA_AGENDADA} UTC")
    while True:
        try:
            agora_utc = datetime.datetime.utcnow().strftime("%H:%M")
            if agora_utc == HORA_AGENDADA:
                if status_global != "RODANDO":
                    threading.Thread(target=run_background, args=(True, "Agendador 01:00 AM")).start()
                    time.sleep(61)
            time.sleep(30)
        except: time.sleep(30)

t_relogio = threading.Thread(target=agendador_automatico, daemon=True)
t_relogio.start()

@app.route('/')
def index(): return redirect(url_for('monitorar'))

@app.route('/iniciar')
def iniciar():
    global status_global
    if status_global != "RODANDO":
        t = threading.Thread(target=run_background, args=(False, "Botão Manual"))
        t.start()
    return redirect(url_for('monitorar'))

@app.route('/atualizar')
def atualizar():
    global status_global
    if status_global != "RODANDO":
        limpar_logs() 
        t = threading.Thread(target=run_background, args=(True, "Botão Atualizar"))
        t.start()
    return redirect(url_for('monitorar'))

# --- ROTA DE PARAR ---
@app.route('/parar')
def parar():
    global abortar_missao, status_global
    if status_global == "RODANDO":
        adicionar_log("⚠️ SOLICITAÇÃO DE PARADA RECEBIDA...")
        abortar_missao = True
    return redirect(url_for('monitorar'))

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    if request.args.get('token') != TOKEN_SECRETO: return jsonify({"erro": "Token"}), 403
    forcar = request.args.get('forcar') == 'sim'
    if status_global != "RODANDO":
        if forcar: limpar_logs()
        t = threading.Thread(target=run_background, args=(forcar, "Webhook WordPress"))
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
    log_html = ler_logs_do_disco()
    
    cor = "gray"
    if status_global == "RODANDO": cor = "green"
    elif status_global == "CANCELADO": cor = "red"
    
    script_scroll = "<script>window.scrollTo(0, document.body.scrollHeight);</script>"
    
    return f"""
    <html>
    <head>
        <meta http-equiv="refresh" content="3">
        <style>
            body {{ font-family: monospace; padding: 20px; background: #222; color: #fff; }}
            .box {{ background: #333; padding: 20px; border: 1px solid #444; border-radius: 8px; min-height: 300px; }}
            .btn {{ padding: 15px 30px; text-decoration: none; font-size: 18px; display: inline-block; margin-bottom: 20px; border-radius: 5px; margin-right: 10px; color:white; font-weight:bold; cursor:pointer; }}
        </style>
    </head>
    <body>
        <h1>Gerador V25 (Botão Stop)</h1>
        <p>Status: <b style="color:{cor}">{status_global}</b></p>
        
        <a href="/iniciar" class="btn" style="background:green">INICIAR</a>
        <a href="/atualizar" class="btn" style="background:orange">ATUALIZAR</a>
        <a href="/parar" class="btn" style="background:red">PARAR TUDO</a>
        
        <div class="box">
            <div>{log_html}</div>
        </div>
        {script_scroll}
    </body>
    </html>
    """

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
