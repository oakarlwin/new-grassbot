import shareithub
import threading
import asyncio
import ssl
import time
import uuid
import json
import random
import base64
from aiohttp import ClientSession, TCPConnector
from colorama import Fore, Style, init
from loguru import logger
from fake_useragent import UserAgent
from websockets_proxy import Proxy, proxy_connect

init(autoreset=True)

CONFIG_FILE = "config.json"
try:
    with open(CONFIG_FILE, "r") as cf:
        _cfg = json.load(cf)
        USER_IDS = _cfg.get("user_ids", [])
        BASE_PROXY = _cfg.get("base_proxy")
except Exception as e:
    print(Fore.RED + f"❌ Gagal load {CONFIG_FILE}: {e}")
    exit(1)

def setup_logger():
    logger.remove()
    logger.add("bot.log",
               format=" <level>{level}</level> | <cyan>{message}</cyan>",
               level="INFO", rotation="1 day")
    logger.add(lambda msg: print(msg, end=""),
               format=" <level>{level}</level> | <cyan>{message}</cyan>",
               level="INFO", colorize=True)

ua = UserAgent()

HEADERS = {
    "User-Agent": "GrassBot/5.1.1",  # akan di-override
    "Content-Type": "application/json",
    "Accept": "*/*",
    "Connection": "keep-alive"
}

ERROR_PATTERNS = [
    "Host unreachable",
    "[SSL: WRONG_VERSION_NUMBER]",
    "invalid length of packed IP address string",
    "Empty connect reply",
    "Device creation limit exceeded",
    "sent 1011 (internal error) keepalive ping timeout"
]

PING_INTERVAL = 30
CHECKIN_INTERVAL = 300
DIRECTOR_SERVER = "https://director.getgrass.io"

# Fungsi untuk mendapatkan IP yang sedang digunakan
async def get_current_ip(proxy_url: str):
    try:
        async with ClientSession() as session:
            async with session.get("http://ipinfo.io/ip", proxy=proxy_url) as response:
                if response.status == 200:
                    ip = await response.text()
                    logger.info(f"📡 IP yang digunakan: {ip.strip()}")
                    return ip.strip()
                else:
                    logger.error(f"❌ Gagal mendapatkan IP, status {response.status}")
                    return None
    except Exception as e:
        logger.error(f"Error saat mengambil IP: {e}")
        return None

# Fungsi untuk mendapatkan endpoint WebSocket
async def get_ws_endpoints(device_id: str, user_id: str, proxy_url: str):
    HEADERS["User-Agent"] = ua.random
    url = f"{DIRECTOR_SERVER}/checkin"
    payload = {
        "browserId": device_id,
        "userId": user_id,
        "version": "5.1.1",
        "extensionId": "lkbnfiajjmbhnfledhphioinpickokdi",
        "userAgent": HEADERS["User-Agent"],
        "deviceType": "extension"
    }
    connector = TCPConnector(ssl=False)
    async with ClientSession(connector=connector) as session:
        attempt = 0
        while attempt < 5:  
            try:
                async with session.post(url, json=payload, headers=HEADERS, proxy=proxy_url) as resp:
                    if resp.status == 429:  
                        logger.warning(f"⚠️ Rate Limiting Detected! Attempt {attempt + 1}")
                        wait_time = random.uniform(5, 15)  
                        logger.info(f"⏳ Menunggu {wait_time:.2f} detik sebelum mencoba lagi...")
                        time.sleep(wait_time)
                        attempt += 1
                        continue  
                    elif resp.status != 201:
                        logger.error(f"Check-in gagal (status {resp.status})")
                        return [], ""
                    data = await resp.json(content_type=None)
                    dests = [f"wss://{d}" for d in data.get("destinations", [])]
                    token = data.get("token", "")
                    return dests, token
            except Exception as e:
                logger.error(f"Check-in error: {e}")
                return [], ""
        logger.error("❌ Gagal melakukan check-in setelah 5 percakapan percobaan")
        return [], ""

class WebSocketClient:
    def __init__(self, device_id: str, user_id: str, proxy_url: str):
        self.device_id = device_id
        self.user_id = user_id
        self.proxy_url = proxy_url
        self.base_proxy = BASE_PROXY
        self.uri = None

    async def connect(self) -> None:
        logger.info(f"🖥️ Device {self.device_id} ➡️ Proxy Connected")
        # Menampilkan IP yang sedang digunakan
        ip = await get_current_ip(self.proxy_url)
        if ip:
            logger.info(f"🖥️ Device {self.device_id} menggunakan IP {ip}")

        while True:
            try:
                endpoints, token = await get_ws_endpoints(self.device_id, self.user_id, self.proxy_url)
                if not endpoints or not token:
                    raise Exception("Invalid endpoints/token")
                self.uri = f"{endpoints[0]}?token={token}"

                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode = ssl.CERT_NONE

                HEADERS["User-Agent"] = ua.random
                async with proxy_connect(self.uri,
                                         proxy=Proxy.from_url(self.proxy_url),
                                         ssl=ssl_ctx,
                                         extra_headers=HEADERS) as ws:
                    ping_task = asyncio.create_task(self._send_ping(ws))
                    checkin_task = asyncio.create_task(self._periodic_checkin())
                    await self._handle_messages(ws)
                    ping_task.cancel()
                    checkin_task.cancel()

            except Exception as e:
                logger.error(f"🚫 Koneksi error: {e}")
                if self.proxy_url != self.base_proxy:
                    logger.info("🔄 Fallback ke BASE_PROXY")
                else:
                    logger.info("❌ BASE_PROXY gagal, retry dalam 5 detik")
                    await asyncio.sleep(5)
                self.proxy_url = self.base_proxy

    async def _send_ping(self, ws):
        while True:
            try:
                msg = {
                    "id": str(uuid.uuid4()),
                    "version": "1.0.0",
                    "action": "PING",
                    "data": {}
                }
                await ws.send(json.dumps(msg))
                logger.info(f"💬 PING dikirim: {msg['id']}")
                await asyncio.sleep(PING_INTERVAL)
            except Exception as e:
                logger.error(f"Ping error: {e}")
                break

    async def _periodic_checkin(self):
        while True:
            await asyncio.sleep(CHECKIN_INTERVAL)
            await get_ws_endpoints(self.device_id, self.user_id, self.proxy_url)

    async def _handle_messages(self, ws):
        handlers = {
            "AUTH": self._handle_auth,
            "PONG": self._handle_pong,
            "HTTP_REQUEST": self._handle_http_request
        }
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            action = msg.get("action")
            if action == "PONG":
                logger.info(f"💬 PONG diterima: {msg['id']}")
            elif action in handlers:
                await handlers[action](ws, msg)
            else:
                logger.error(f"Tidak ada handler untuk action: {action}")

    async def _handle_auth(self, ws, msg):
        resp = {
            "id": msg["id"],
            "origin_action": "AUTH",
            "result": {
                "browser_id": self.device_id,
                "user_id": self.user_id,
                "user_agent": HEADERS["User-Agent"],
                "timestamp": int(time.time()),
                "device_type": "extension",
                "version": "5.1.1"
            }
        }
        await ws.send(json.dumps(resp))

    async def _handle_pong(self, ws, msg):
        resp = {"id": msg["id"], "origin_action": "PONG"}
        await ws.send(json.dumps(resp))

    async def _handle_http_request(self, ws, msg):
        d = msg.get("data", {})
        method = d.get("method", "GET").upper()
        url = d.get("url")
        hdrs = d.get("headers", {})
        hdrs["User-Agent"] = ua.random
        body = d.get("body")
        try:
            async with ClientSession() as session:
                async with session.request(method, url, headers=hdrs, data=body) as r:
                    if r.status == 429:
                        raise Exception("Rate limited")
                    res_headers = dict(r.headers)
                    res_bytes = await r.read()
        except Exception as e:
            logger.error(f"HTTP_REQUEST error: {e}")
            raise
        b64 = base64.b64encode(res_bytes).decode()
        result = {
            "url": url,
            "status": r.status,
            "status_text": "",
            "headers": res_headers,
            "body": b64
        }
        reply = {
            "id": msg["id"],
            "origin_action": "HTTP_REQUEST",
            "result": result
        }
        await ws.send(json.dumps(reply))

def start_client(device_id: str, user_id: str, proxy_url: str):
    asyncio.run(WebSocketClient(device_id, user_id, proxy_url).connect())

def main():
    setup_logger()

    threads_count = int(input("🔢 Jumlah thread per USER (max 10): "))

    if threads_count > 10:
        print(Fore.RED + "❌ Jumlah thread maksimal adalah 10.")
        return

    logger.info(f"🚀 Menjalankan {threads_count} threads untuk tiap USER")

    threads = []
    for uid in USER_IDS:
        for _ in range(threads_count):
            dev_id = str(uuid.uuid4())
            proxy_url = BASE_PROXY
            t = threading.Thread(
                target=start_client,
                args=(dev_id, uid, proxy_url),
                daemon=True
            )
            t.start()
            threads.append(t)
            time.sleep(0.1)

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("👋 Shutting down...")

if __name__ == "__main__":
    main()
