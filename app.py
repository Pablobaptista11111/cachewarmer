from flask import Flask
import asyncio
import aiohttp
import requests
import xml.etree.ElementTree as ET
import time
import gzip
import threading

# --- CONFIGURAÇÃO DA FULLBAI ---
SITEMAP_URL = "https://fullbai.com.ar/sitemap_index.xml"
# -------------------------------

app = Flask(__name__)

# Variável para controlar se o robô está trabalhando
is_running = False
log_mensagens = []  # Para guardar um histórico simples

async def fetch_url(session, url):
    try:
        # Timeout de 20s para dar tempo ao servidor
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; FullbaiWarmer/1.0; +http://fullbai.com.ar)',
            'Accept-Encoding': 'gzip, deflate' 
        }
        async with session.get(url, headers=headers, timeout=20) as response:
            # Lemos apenas o cabeçalho para confirmar o cache
            await response.read()
            cf_status = response.headers.get('cf-cache-status', 'MISS/UNKNOWN')
            return f"URL: {url} | Status: {response.status} | Cloudflare: {cf_status}"
    except Exception as e:
        return f"Erro em {url}: {str(e)}"

def get_sitemap_urls(sitemap_url):
    """Busca recursivamente todas as URLs do Sitemap da Fullbai"""
    urls = set()
    print(f"Lendo Sitemap: {sitemap_url}")
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (compatible; FullbaiWarmer/1.0)'}
        r = requests.get(sitemap_url, headers=headers, timeout=30)
        
        if r.status_code != 200:
            return urls

        # Tenta descompactar se for .gz
        if sitemap_url.endswith('.gz'):
            try:
                content = gzip.decompress(r.content)
            except:
                content = r.content # Pode ser que não esteja comprimido
        else:
            content = r.content

        root = ET.fromstring(content)
        # Namespaces padrão do sitemap
        ns = {'s': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        
        # Se for índice (sitemap de sitemaps)
        for sitemap in root.findall('s:sitemap', ns):
            loc = sitemap.find('s:loc', ns).text
            urls.update(get_sitemap_urls(loc))
            
        # Se forem URLs de produtos
        for url in root.findall('s:url', ns):
            loc = url.find('s:loc', ns).text
            urls.add(loc)
            
    except Exception as e:
        print(f"Erro ao ler sitemap: {e}")
        pass
    return urls

async def worker():
    global is_running
    is_running = True
    log_mensagens.clear()
    log_mensagens.append("Iniciando mapeamento de produtos...")
    
    # 1. Pega todas as URLs
    loop = asyncio.get_event_loop()
    urls = await loop.run_in_executor(None, get_sitemap_urls, SITEMAP_URL)
    total = len(urls)
    log_mensagens.append(f"Total de URLs encontradas na Fullbai: {total}")

    if total > 0:
        # 2. Visita 10 por vez (segurança para não derrubar a loja)
        semaphore = asyncio.Semaphore(10)
        
        async with aiohttp.ClientSession() as session:
            async def bound_fetch(url):
                async with semaphore:
                    res = await fetch_url(session, url)
                    # Opcional: imprimir no log do servidor
                    print(res) 

            tasks = [bound_fetch(url) for url in urls]
            await asyncio.gather(*tasks)
    
    log_mensagens.append("Ciclo finalizado com sucesso!")
    is_running = False

def run_async_background():
    asyncio.run(worker())

@app.route('/')
def index():
    status_texto = "RODANDO AGORA..." if is_running else "PARADO (Aguardando comando)"
    cor_status = "green" if is_running else "red"
    
    html = f"""
    <div style="font-family: sans-serif; text-align: center; padding: 50px;">
        <h1>Gerador de Cache - Fullbai</h1>
        <p>Alvo: <b>{SITEMAP_URL}</b></p>
        <div style="border: 1px solid #ccc; padding: 20px; margin: 20px auto; max-width: 600px; border-radius: 10px;">
            <p>Status: <b style="color: {cor_status};">{status_texto}</b></p>
            <br>
            <a href="/iniciar" style="text-decoration: none;">
                <button style="background-color: #007bff; color: white; padding: 15px 30px; font-size: 18px; border: none; border-radius: 5px; cursor: pointer;">
                    INICIAR CACHE AGORA
                </button>
            </a>
        </div>
        <h3>Últimas mensagens:</h3>
        <ul style="list-style: none; padding: 0;">
            {''.join([f'<li>{m}</li>' for m in log_mensagens])}
        </ul>
    </div>
    """
    return html

@app.route('/iniciar')
def iniciar():
    global is_running
    if is_running:
        return "O robô JÁ está rodando! Volte e aguarde."
    
    # Inicia em segundo plano
    t = threading.Thread(target=run_async_background)
    t.start()
    return "Iniciado! O robô está visitando suas páginas em segundo plano. <a href='/'>Voltar</a>"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80)
