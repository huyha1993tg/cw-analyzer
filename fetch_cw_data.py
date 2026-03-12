"""
CW Data Fetcher v2 - Multiple API sources with fallback
"""
import json, sys, os, argparse
from datetime import datetime
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

def log(msg):
    print(f"  {msg}", flush=True)

def http_post(url, payload, headers=None):
    h = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    if headers:
        h.update(headers)
    data = json.dumps(payload).encode('utf-8')
    req = Request(url, data=data, headers=h, method='POST')
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))

def http_get(url, headers=None):
    h = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36", "Accept": "application/json"}
    if headers:
        h.update(headers)
    req = Request(url, headers=h)
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode('utf-8'))

# ============ SOURCE 1: VCI GraphQL ============
def fetch_vci():
    log("Trying VCI GraphQL...")
    url = "https://api.vietcap.com.vn/data-mt/graphql"
    queries = [
        ("CoveredWarrantSnapshot", "{ CoveredWarrantSnapshot { ticker issuerTicker underlyingTicker exercisePrice exerciseRatio exerciseRatioBase lastPrice referencePrice totalVolume status cwType underlyingLastPrice premium daysToMaturity maturityDate listingDate issuerName } }"),
        ("ListCoveredWarrant", "{ ListCoveredWarrant { ticker issuerTicker underlyingTicker exercisePrice exerciseRatio exerciseRatioBase lastPrice referencePrice totalVolume status cwType underlyingLastPrice premium daysToMaturity maturityDate listingDate issuerName } }"),
    ]
    for name, q in queries:
        try:
            data = http_post(url, {"query": q})
            items = data.get('data', {}).get(name, [])
            if items and len(items) > 0:
                log(f"VCI [{name}]: {len(items)} warrants")
                return [normalize_vci(x) for x in items if x], 'VCI'
        except Exception as e:
            log(f"VCI {name} failed: {e}")
    return None, None

def normalize_vci(item):
    try:
        return {
            'maCQ': item.get('ticker', ''),
            'tcph': item.get('issuerTicker', item.get('issuerName', '')),
            'tscs': item.get('underlyingTicker', ''),
            'giaHT_CQ': float(item.get('lastPrice', 0) or 0),
            'giaDC_CQ': float(item.get('referencePrice', 0) or 0),
            'giaTH': float(item.get('exercisePrice', 0) or 0),
            'giaHT_TSCS': float(item.get('underlyingLastPrice', 0) or 0),
            'tlcd_num': float(item.get('exerciseRatio', 1) or 1),
            'tlcd_den': float(item.get('exerciseRatioBase', 1) or 1),
            'thoiGianDH': int(item.get('daysToMaturity', 0) or 0),
            'premium': item.get('premium'),
            'volume': int(item.get('totalVolume', 0) or 0),
            'ngayCuoi': str(item.get('maturityDate', '')),
            'ngayDau': str(item.get('listingDate', '')),
            'loaiCQ': item.get('cwType', 'Call'),
        }
    except:
        return None

# ============ SOURCE 2: TCBS ============
def fetch_tcbs():
    log("Trying TCBS API...")
    urls = [
        "https://apipubaws.tcbs.com.vn/stock-insight/v2/stock/warrant-list",
        "https://apipubaws.tcbs.com.vn/stock-insight/v1/warrant/all",
    ]
    for url in urls:
        try:
            data = http_get(url)
            items = data if isinstance(data, list) else data.get('data', data.get('warrants', data.get('items', [])))
            if items and len(items) > 0:
                log(f"TCBS: {len(items)} warrants")
                return [normalize_tcbs(x) for x in items if x], 'TCBS'
        except Exception as e:
            log(f"TCBS failed: {e}")
    return None, None

def normalize_tcbs(item):
    try:
        return {
            'maCQ': item.get('ticker', item.get('symbol', '')),
            'tcph': item.get('issuer', item.get('issuerTicker', '')),
            'tscs': item.get('underlyingStock', item.get('underlying', item.get('underlyingTicker', ''))),
            'giaHT_CQ': float(item.get('price', item.get('lastPrice', item.get('close', 0))) or 0),
            'giaDC_CQ': float(item.get('refPrice', item.get('referencePrice', 0)) or 0),
            'giaTH': float(item.get('strikePrice', item.get('exercisePrice', 0)) or 0),
            'giaHT_TSCS': float(item.get('underlyingPrice', item.get('underlyingLastPrice', 0)) or 0),
            'tlcd_num': float(item.get('conversionRatioNum', item.get('exerciseRatio', 1)) or 1),
            'tlcd_den': float(item.get('conversionRatioDen', item.get('exerciseRatioBase', 1)) or 1),
            'thoiGianDH': int(item.get('daysToMaturity', item.get('remainDays', 0)) or 0),
            'premium': item.get('premium'),
            'volume': int(item.get('volume', item.get('totalVolume', 0)) or 0),
            'ngayCuoi': str(item.get('maturityDate', item.get('lastTradingDate', ''))),
            'loaiCQ': item.get('type', item.get('cwType', 'Call')),
        }
    except:
        return None

# ============ SOURCE 3: SSI (iBoard) ============
def fetch_ssi():
    log("Trying SSI iBoard API...")
    try:
        # SSI iBoard provides market data including CW
        url = "https://iboard-query.ssi.com.vn/v2/stock/type/w/hose"
        data = http_get(url)
        items = data.get('data', [])
        if items and len(items) > 0:
            log(f"SSI iBoard: {len(items)} warrants")
            return [normalize_ssi(x) for x in items if x], 'SSI'
    except Exception as e:
        log(f"SSI iBoard failed: {e}")

    # Alternative SSI endpoint
    try:
        url2 = "https://wgateway-iboard.ssi.com.vn/graphql"
        query = {"operationName": "coveredWarrant", "variables": {}, "query": "query coveredWarrant { coveredWarrant { ticker issuerTicker underlyingTicker exercisePrice exerciseRatio lastPrice referencePrice totalVolume underlyingLastPrice premium daysToMaturity maturityDate } }"}
        data2 = http_post(url2, query)
        items2 = data2.get('data', {}).get('coveredWarrant', [])
        if items2 and len(items2) > 0:
            log(f"SSI GraphQL: {len(items2)} warrants")
            return [normalize_ssi(x) for x in items2 if x], 'SSI'
    except Exception as e:
        log(f"SSI GraphQL failed: {e}")

    return None, None

def normalize_ssi(item):
    try:
        return {
            'maCQ': item.get('ticker', item.get('ss', item.get('stockSymbol', ''))),
            'tcph': item.get('issuerTicker', item.get('issuer', '')),
            'tscs': item.get('underlyingTicker', item.get('underlyingStock', item.get('bS', ''))),
            'giaHT_CQ': float(item.get('lastPrice', item.get('lP', item.get('close', 0))) or 0),
            'giaDC_CQ': float(item.get('referencePrice', item.get('rP', 0)) or 0),
            'giaTH': float(item.get('exercisePrice', item.get('eP', 0)) or 0),
            'giaHT_TSCS': float(item.get('underlyingLastPrice', item.get('uP', 0)) or 0),
            'tlcd_num': float(item.get('exerciseRatio', item.get('eR', 1)) or 1),
            'tlcd_den': float(item.get('exerciseRatioBase', 1) or 1),
            'thoiGianDH': int(item.get('daysToMaturity', item.get('dTM', 0)) or 0),
            'premium': item.get('premium'),
            'volume': int(item.get('totalVolume', item.get('tV', 0)) or 0),
            'ngayCuoi': str(item.get('maturityDate', '')),
            'loaiCQ': item.get('cwType', 'Call'),
        }
    except:
        return None

# ============ SOURCE 4: Wifeed / Simplize ============
def fetch_wifeed():
    log("Trying Wifeed/Simplize...")
    try:
        url = "https://api.simplize.vn/api/company/api/CompanyInfo/getListCoveredWarrant"
        data = http_get(url)
        items = data.get('data', []) if isinstance(data, dict) else data
        if items and len(items) > 0:
            log(f"Simplize: {len(items)} warrants")
            return [normalize_simplize(x) for x in items if x], 'Simplize'
    except Exception as e:
        log(f"Simplize failed: {e}")
    return None, None

def normalize_simplize(item):
    try:
        return {
            'maCQ': item.get('ticker', item.get('symbol', '')),
            'tcph': item.get('issuerTicker', item.get('issuer', '')),
            'tscs': item.get('underlyingTicker', item.get('underlying', '')),
            'giaHT_CQ': float(item.get('lastPrice', item.get('price', 0)) or 0),
            'giaDC_CQ': float(item.get('referencePrice', 0) or 0),
            'giaTH': float(item.get('exercisePrice', item.get('strikePrice', 0)) or 0),
            'giaHT_TSCS': float(item.get('underlyingLastPrice', item.get('underlyingPrice', 0)) or 0),
            'tlcd_num': float(item.get('exerciseRatio', item.get('conversionRatio', 1)) or 1),
            'tlcd_den': float(item.get('exerciseRatioBase', 1) or 1),
            'thoiGianDH': int(item.get('daysToMaturity', 0) or 0),
            'premium': item.get('premium'),
            'volume': int(item.get('totalVolume', item.get('volume', 0)) or 0),
            'ngayCuoi': str(item.get('maturityDate', '')),
            'loaiCQ': item.get('cwType', 'Call'),
        }
    except:
        return None

# ============ FINALIZE ============
def validate(items):
    valid = []
    for d in items:
        if d is None:
            continue
        if all([d.get('maCQ'), d.get('giaHT_CQ', 0) > 0, d.get('giaTH', 0) > 0,
                d.get('giaHT_TSCS', 0) > 0, d.get('thoiGianDH', 0) > 0]):
            d['trangThai'] = 'IN' if d['giaHT_TSCS'] > d['giaTH'] else 'OUT'
            valid.append(d)
    return valid

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='cw_data.json')
    parser.add_argument('--server', action='store_true')
    parser.add_argument('--port', type=int, default=8899)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)

    print("=" * 50)
    print("  CW Data Fetcher v2")
    print("=" * 50)

    sources = [fetch_vci, fetch_tcbs, fetch_ssi, fetch_wifeed]
    data, source = None, None

    for fn in sources:
        try:
            data, source = fn()
            if data:
                break
        except Exception as e:
            log(f"Source error: {e}")

    if not data:
        print("\nERROR: All API sources failed.")
        print("This may happen if APIs block non-Vietnam IPs.")
        print("Try running locally: python fetch_cw_data.py --server")
        sys.exit(1)

    valid = validate(data)
    log(f"Valid CW after filtering: {len(valid)}")

    if not valid:
        print("ERROR: No valid CW data.")
        sys.exit(1)

    output = {
        'timestamp': datetime.now().isoformat(),
        'source': source,
        'count': len(valid),
        'data': valid,
    }

    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    size = os.path.getsize(args.output) / 1024
    print(f"\n  OK: {len(valid)} CW saved to {args.output} ({size:.1f} KB)")

    if args.server:
        from http.server import HTTPServer, SimpleHTTPRequestHandler
        d = os.path.dirname(os.path.abspath(args.output)) or '.'
        class H(SimpleHTTPRequestHandler):
            def __init__(self, *a, **kw): super().__init__(*a, directory=d, **kw)
            def end_headers(self):
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Cache-Control', 'no-cache')
                super().end_headers()
        print(f"\n  Server: http://localhost:{args.port}")
        print(f"  Ctrl+C to stop\n")
        HTTPServer(('', args.port), H).serve_forever()

if __name__ == '__main__':
    main()
