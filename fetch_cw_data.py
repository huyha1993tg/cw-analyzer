"""
CW Data Fetcher - Lightweight version for GitHub Actions
=========================================================
Fetches covered warrant data directly from public APIs (VCI/TCBS).
No vnstock library or API key needed.

Usage:
  python fetch_cw_data.py --output data/cw_data.json
"""

import json, sys, os, argparse
from datetime import datetime, date

try:
    import requests
except ImportError:
    print("ERROR: pip install requests")
    sys.exit(1)


def fetch_vci():
    """Fetch CW data from VCI (Viet Capital) GraphQL API"""
    url = "https://api.vietcap.com.vn/data-mt/graphql"
    
    queries = [
        # Query 1: CoveredWarrantSnapshot
        {"query": "{ CoveredWarrantSnapshot { ticker issuerTicker underlyingTicker exercisePrice exerciseRatio exerciseRatioBase lastTradingDate maturityDate lastPrice referencePrice totalVolume status cwType underlyingLastPrice breakEvenPrice premium daysToMaturity listingDate issuerName } }"},
        # Query 2: Alternative name
        {"query": "{ ListCoveredWarrant { ticker issuerTicker underlyingTicker exercisePrice exerciseRatio exerciseRatioBase lastPrice referencePrice totalVolume status cwType underlyingLastPrice premium daysToMaturity maturityDate listingDate issuerName } }"},
    ]
    
    headers = {"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 CWAnalyzer/1.0"}
    
    for q in queries:
        try:
            r = requests.post(url, json=q, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json().get('data', {})
            for key in data:
                if isinstance(data[key], list) and len(data[key]) > 0:
                    print(f"  VCI [{key}]: {len(data[key])} warrants")
                    return data[key], 'VCI'
        except Exception as e:
            print(f"  VCI query failed: {e}")
    
    return None, None


def fetch_tcbs():
    """Fetch CW data from TCBS public API"""
    urls = [
        "https://apipubaws.tcbs.com.vn/stock-insight/v2/stock/warrant-list",
        "https://apipubaws.tcbs.com.vn/stock-insight/v1/warrant/all",
    ]
    headers = {"User-Agent": "Mozilla/5.0 CWAnalyzer/1.0", "Accept": "application/json"}
    
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
            items = data if isinstance(data, list) else data.get('data', data.get('warrants', data.get('items', [])))
            if items and len(items) > 0:
                print(f"  TCBS: {len(items)} warrants")
                return items, 'TCBS'
        except Exception as e:
            print(f"  TCBS {url.split('/')[-1]} failed: {e}")
    
    return None, None


def normalize(items, source):
    """Convert raw API data to unified format"""
    results = []
    
    for item in items:
        try:
            if source == 'VCI':
                d = {
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
                    'status': str(item.get('status', '')),
                }
            elif source == 'TCBS':
                d = {
                    'maCQ': item.get('ticker', item.get('symbol', '')),
                    'tcph': item.get('issuer', item.get('issuerTicker', '')),
                    'tscs': item.get('underlyingStock', item.get('underlying', item.get('underlyingTicker', ''))),
                    'giaHT_CQ': float(item.get('price', item.get('lastPrice', 0)) or 0),
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
                    'status': 'active',
                }
            else:
                continue
            
            # Validate
            if all([d['maCQ'], d['giaHT_CQ'] > 0, d['giaTH'] > 0, d['giaHT_TSCS'] > 0, d['thoiGianDH'] > 0]):
                d['trangThai'] = 'IN' if d['giaHT_TSCS'] > d['giaTH'] else 'OUT'
                results.append(d)
                
        except Exception:
            continue
    
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', default='cw_data.json')
    parser.add_argument('--server', action='store_true')
    parser.add_argument('--port', type=int, default=8899)
    args = parser.parse_args()
    
    os.makedirs(os.path.dirname(args.output) or '.', exist_ok=True)
    
    print("=" * 50)
    print("  CW Data Fetcher")
    print("=" * 50)
    
    # Try sources
    raw, source = fetch_vci()
    if not raw:
        raw, source = fetch_tcbs()
    
    if not raw:
        print("\nERROR: All sources failed.")
        sys.exit(1)
    
    data = normalize(raw, source)
    print(f"\n  Normalized: {len(data)} valid CW (from {source})")
    
    output = {
        'timestamp': datetime.now().isoformat(),
        'source': source,
        'count': len(data),
        'data': data,
    }
    
    with open(args.output, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    size = os.path.getsize(args.output) / 1024
    print(f"  Saved: {args.output} ({size:.1f} KB)")
    print("  Done!")
    
    if args.server:
        import http.server
        class H(http.server.SimpleHTTPRequestHandler):
            def __init__(self, *a, **kw):
                super().__init__(*a, directory=os.path.dirname(os.path.abspath(args.output)) or '.', **kw)
            def end_headers(self):
                self.send_header('Access-Control-Allow-Origin', '*')
                self.send_header('Cache-Control', 'no-cache')
                super().end_headers()
        
        print(f"\n  Server: http://localhost:{args.port}")
        print(f"  Data:   http://localhost:{args.port}/{os.path.basename(args.output)}")
        print(f"  Ctrl+C to stop\n")
        http.server.HTTPServer(('', args.port), H).serve_forever()


if __name__ == '__main__':
    main()
