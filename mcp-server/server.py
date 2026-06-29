"""
MCP server: lakehouse'u (bronze / silver / gold) bir AI agent'a acar.

Claude Desktop bu server'a stdio uzerinden baglanir; server da Spark Thrift
(spark-thrift:10000) uzerinden Iceberg lakehouse'a SQL atar. Boylece dogal dille
sorulan bir soru ("gecen ayin en cok satan kategorisi ne?") agent tarafindan
once list_tables / describe_table ile sema kesfine, sonra run_query ile gercek
SQL'e cevrilir.

Tasarim notlari:
- run_query yalnizca SELECT calistirir (DDL/DML engellidir) -> agent yanlislikla
  veri bozamaz. Bu, "okuma-amacli analitik erisim" guvenlik sinirini uygular.
- Baglanti her sorguda acilip kapanir (basit ve saglam; bu olcekte yeterli).
- Sonuclar agent'in rahat okuyabilmesi icin sade metin tablo olarak donulur.
"""

import os
import re
import logging

# MCP stdio modunda stdout YALNIZCA protokol mesajlari icindir. PyHive ve diger
# kutuphaneler stdout'a/loglara satir basarsa (orn "USE default") protokol
# bozulabilir. Bu yuzden tum logging'i stderr'e/sustur: root logger'i WARNING'e
# cek ve PyHive'in gurultusunu kapat.
logging.basicConfig(level=logging.WARNING)
for noisy in ("pyhive", "thrift", "py4j", "urllib3"):
    logging.getLogger(noisy).setLevel(logging.ERROR)

from mcp.server.fastmcp import FastMCP
from pyhive import hive

THRIFT_HOST = os.environ.get("THRIFT_HOST", "spark-thrift")
THRIFT_PORT = int(os.environ.get("THRIFT_PORT", "10000"))
CATALOG = os.environ.get("LAKEHOUSE_CATALOG", "lakehouse")

mcp = FastMCP("lakehouse")


def _connect():
    return hive.Connection(host=THRIFT_HOST, port=THRIFT_PORT)


def _run(sql: str):
    """SQL calistirir, (kolonlar, satirlar) doner."""
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall() if cur.description else []
        return cols, rows
    finally:
        conn.close()


def _format(cols, rows, max_rows: int = 100) -> str:
    """Sonucu sade, okunabilir metin tablo olarak bicimlendirir."""
    if not cols:
        return "(sonuc yok)"
    out = [" | ".join(cols), "-" * 40]
    for r in rows[:max_rows]:
        out.append(" | ".join("NULL" if v is None else str(v) for v in r))
    if len(rows) > max_rows:
        out.append(f"... ({len(rows)} satirdan ilk {max_rows} gosterildi)")
    return "\n".join(out)


@mcp.tool()
def list_tables() -> str:
    """
    Lakehouse'daki tum namespace'leri (bronze/silver/gold) ve iclerindeki
    tablolari listeler. Agent neyi sorgulayabilecegini bilmek icin once bunu
    cagirmali.
    """
    cols, ns_rows = _run(f"SHOW NAMESPACES IN {CATALOG}")
    lines = []
    for ns in (r[0] for r in ns_rows):
        try:
            _, t_rows = _run(f"SHOW TABLES IN {CATALOG}.{ns}")
            for tr in t_rows:
                # SHOW TABLES: (namespace, tableName, isTemporary)
                tname = tr[1] if len(tr) > 1 else tr[0]
                lines.append(f"{CATALOG}.{ns}.{tname}")
        except Exception as e:
            lines.append(f"{CATALOG}.{ns} (okunamadi: {e})")
    return "\n".join(lines) if lines else "(tablo bulunamadi)"


@mcp.tool()
def describe_table(table: str) -> str:
    """
    Bir tablonun kolonlarini ve tiplerini doner. Agent dogru SQL yazabilmek
    icin sorgulamadan once semayi gormeli.

    table: tam ad, orn 'lakehouse.gold.mart_daily_revenue'
    """
    if not re.fullmatch(r"[A-Za-z0-9_.]+", table):
        return "Gecersiz tablo adi."
    cols, rows = _run(f"DESCRIBE {table}")
    return _format(cols, rows)


@mcp.tool()
def run_query(sql: str) -> str:
    """
    Lakehouse uzerinde bir SELECT sorgusu calistirir ve sonucu doner.
    Yalnizca SELECT/WITH/SHOW/DESCRIBE izinlidir; DDL/DML reddedilir.

    sql: tam SQL metni. Tablolar tam adla anilmali, orn
         'SELECT * FROM lakehouse.gold.mart_sales_by_category'
    """
    cleaned = sql.strip().rstrip(";").strip()
    low = cleaned.lower()
    # Yalnizca okuma: ilk anlamli kelime select/with/show/describe olmali.
    if not re.match(r"^(select|with|show|describe|desc)\b", low):
        return "Reddedildi: yalnizca SELECT/WITH/SHOW/DESCRIBE sorgulari calistirilabilir."
    # Ekstra guvenlik: tehlikeli anahtar kelimeleri engelle.
    forbidden = r"\b(insert|update|delete|drop|alter|create|truncate|merge|grant|revoke|call)\b"
    if re.search(forbidden, low):
        return "Reddedildi: sorgu yalnizca okuma amacli olmali (DDL/DML iceremez)."
    try:
        cols, rows = _run(cleaned)
        return _format(cols, rows)
    except Exception as e:
        return f"Sorgu hatasi: {e}"


if __name__ == "__main__":
    mcp.run()
