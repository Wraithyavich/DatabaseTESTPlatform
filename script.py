import csv
import json
import os
import re
import urllib.parse
import urllib.request
from collections import defaultdict
from typing import Dict, Iterable, List, Optional, Set, Tuple

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ---------- Получение токена из переменной окружения ----------
API_TOKEN = os.environ.get('API_TOKEN')
if API_TOKEN is None:
    raise ValueError("❌ Переменная окружения API_TOKEN не задана!")

# ---------- Константы ----------
MIN_SEARCH_LENGTH = 4
MAX_LINES_PER_BLOCK = 25
MAX_VIN_CANDIDATES_TO_SEARCH = 30

DATA_FILE = 'data.csv'
JRONE_FILE = 'jronecross.csv'
OEM_FILE = 'oemcross.csv'
FLP_FILE = 'flp.csv'

# ---------- VIN + интернет-поиск ----------
# Локальную VIN-базу отключаем: бот ищет возможные номера турбины/OEM только через интернет.
# Поддерживаются Brave Search API или SerpAPI. Нужен хотя бы один ключ.
BRAVE_SEARCH_API_KEY = os.environ.get('BRAVE_SEARCH_API_KEY')
SERPAPI_API_KEY = os.environ.get('SERPAPI_API_KEY')
SEARCH_PROVIDER = os.environ.get('SEARCH_PROVIDER', '').lower().strip()  # brave / serpapi / auto
WEB_SEARCH_MAX_RESULTS = int(os.environ.get('WEB_SEARCH_MAX_RESULTS', '8'))
WEB_SEARCH_TIMEOUT = int(os.environ.get('WEB_SEARCH_TIMEOUT', '15'))

# Необязательно, но очень желательно: LLM извлекает из поисковой выдачи только номера турбин/OEM.
# Без OPENAI_API_KEY бот использует более грубый regex-фолбэк и будет менее точным.
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
OPENAI_MODEL = os.environ.get('OPENAI_MODEL', 'gpt-4.1-mini')
OPENAI_TIMEOUT = int(os.environ.get('OPENAI_TIMEOUT', '30'))

# Сколько найденных в интернете номеров проверять по локальной E&E базе.
MAX_VIN_CANDIDATES_TO_SEARCH = 30

# ---------- Очистка текста ----------
def clean_text(s: str) -> str:
    s = (s or '').strip()
    s = s.replace('\r', '').replace('\n', '').replace('\ufeff', '')
    return ' '.join(s.split())

# ---------- Замена кириллических букв, похожих на латиницу ----------
CYRILLIC_TO_LATIN = {
    'А': 'A', 'а': 'a',
    'В': 'B', 'в': 'b',
    'Е': 'E', 'е': 'e',
    'К': 'K', 'к': 'k',
    'М': 'M', 'м': 'm',
    'Н': 'H', 'н': 'h',
    'О': 'O', 'о': 'o',
    'Р': 'P', 'р': 'p',
    'С': 'C', 'с': 'c',
    'Т': 'T', 'т': 't',
    'У': 'Y', 'у': 'y',
    'Х': 'X', 'х': 'x',
}

def replace_cyrillic_like_latin(s: str) -> str:
    return ''.join(CYRILLIC_TO_LATIN.get(ch, ch) for ch in s)

def normalize(s: str) -> str:
    """Мягкая нормализация для поиска номеров: убираем все, кроме букв и цифр."""
    s = replace_cyrillic_like_latin(s or '')
    return re.sub(r'[^A-Za-z0-9]', '', s).lower()

def normalize_text(s: str) -> str:
    return normalize(s).upper()

def is_11_digit_number(s: str) -> bool:
    return re.fullmatch(r'\d{11}', s or '') is not None

VIN_RE = re.compile(r'^[A-HJ-NPR-Z0-9]{17}$')

def is_vin(s: str) -> bool:
    candidate = normalize_text(s)
    return VIN_RE.fullmatch(candidate) is not None

# ---------- Загрузка основной базы (data.csv) ----------
dict_by_col1 = defaultdict(list)   # Turbo P/N -> список E&E P/N
dict_by_col2 = defaultdict(list)   # E&E P/N -> список Turbo P/N
col1_norm_to_original = defaultdict(list)  # нормализованный Turbo -> оригиналы
col2_norm_to_original = defaultdict(list)  # нормализованный E&E -> оригиналы

try:
    with open(DATA_FILE, mode='r', encoding='utf-8-sig') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            if len(row) >= 2:
                col1 = clean_text(row[0])
                col2 = clean_text(row[1])
                if col1 and col2:
                    dict_by_col1[col1].append(col2)
                    dict_by_col2[col2].append(col1)
                    col1_norm_to_original[normalize(col1)].append(col1)
                    col2_norm_to_original[normalize(col2)].append(col2)
except FileNotFoundError:
    print("❌ Файл data.csv не найден! Поместите его в папку со скриптом.")
    exit(1)

print(f"✅ Основная база: {len(dict_by_col1)} Turbo P/N, {len(dict_by_col2)} E&E P/N.")

# ---------- Загрузка базы JRN-кроссов (jronecross.csv) ----------
jrone_norm_to_art = defaultdict(set)
try:
    with open(JRONE_FILE, mode='r', encoding='utf-8-sig') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            if len(row) >= 3:
                jrone = clean_text(row[0])
                our_art = clean_text(row[2])
                if jrone and our_art:
                    jrone_norm_to_art[normalize(jrone)].add(our_art)
except FileNotFoundError:
    print("⚠️ Файл jronecross.csv не найден, поиск по JRN-номерам недоступен.")
except Exception as e:
    print(f"❌ Ошибка загрузки {JRONE_FILE}: {e}")
print(f"✅ JRN-база: {len(jrone_norm_to_art)} уникальных нормализованных JRN-номеров.")

# ---------- Загрузка базы OEM-кроссов (oemcross.csv) ----------
oem_norm_to_art = defaultdict(set)
try:
    with open(OEM_FILE, mode='r', encoding='utf-8-sig') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            if len(row) >= 2:
                art = clean_text(row[0])
                oem = clean_text(row[1])
                if art and oem:
                    oem_norm_to_art[normalize(oem)].add(art)
except FileNotFoundError:
    print("⚠️ Файл oemcross.csv не найден, поиск по OEM-номерам недоступен.")
except Exception as e:
    print(f"❌ Ошибка загрузки {OEM_FILE}: {e}")
print(f"✅ OEM-база: {len(oem_norm_to_art)} уникальных нормализованных OEM-номеров.")

# ---------- Загрузка базы FLP-кроссов (flp.csv) ----------
flp_norm_to_art = defaultdict(set)
art_norm_to_flp = defaultdict(set)
try:
    with open(FLP_FILE, mode='r', encoding='utf-8-sig') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            if len(row) >= 2:
                art = clean_text(row[0])
                flp = clean_text(row[1])
                if art and flp:
                    flp_norm_to_art[normalize(flp)].add(art)
                    art_norm_to_flp[normalize(art)].add(flp)
except FileNotFoundError:
    print("⚠️ Файл flp.csv не найден, поиск по FLP-номерам недоступен.")
except Exception as e:
    print(f"❌ Ошибка загрузки {FLP_FILE}: {e}")
print(f"✅ FLP-база: {len(flp_norm_to_art)} уникальных FLP-номеров, {len(art_norm_to_flp)} уникальных артикулов.")

# ---------- Вспомогательная разбивка номеров ----------
def split_numbers(value: str) -> List[str]:
    raw = re.split(r'[,;|\s]+', value or '')
    return [clean_text(x) for x in raw if clean_text(x)]

print("ℹ️ Локальная VIN-база отключена. VIN-режим использует интернет-поиск.")

# ---------- Поиск в базах ----------
def partial_search_main(search_norm: str) -> Set[str]:
    results = set()
    for norm_key, original_keys in col1_norm_to_original.items():
        if search_norm in norm_key:
            for orig_key in original_keys:
                results.update(dict_by_col1[orig_key])
    for norm_key, original_keys in col2_norm_to_original.items():
        if search_norm in norm_key:
            for orig_key in original_keys:
                results.update(dict_by_col2[orig_key])
    return results

def exact_search_main(search_norm: str) -> Set[str]:
    results = set()
    if search_norm in col2_norm_to_original:
        for key in col2_norm_to_original[search_norm]:
            results.update(dict_by_col2[key])
    if search_norm in col1_norm_to_original:
        for key in col1_norm_to_original[search_norm]:
            results.update(dict_by_col1[key])
    return results

def search_all_sources(query: str, partial: bool = True) -> Dict[str, Set[str]]:
    q_norm = normalize(query)
    result = {
        'main': set(),
        'jrn': set(),
        'oem': set(),
        'flp_art': set(),
        'flp_num': set(),
    }
    if not q_norm:
        return result

    # Основная база: точный поиск всегда первым.
    result['main'].update(exact_search_main(q_norm))
    if partial and len(q_norm) >= MIN_SEARCH_LENGTH and not result['main']:
        result['main'].update(partial_search_main(q_norm))

    # Подстановка 970 для BorgWarner/KKK 11-значных номеров без дефисов.
    if not result['main'] and is_11_digit_number(q_norm):
        first4, middle3, last4 = q_norm[:4], q_norm[4:7], q_norm[7:]
        if middle3 != '970':
            alt_norm = first4 + '970' + last4
            result['main'].update(exact_search_main(alt_norm))
            if partial and not result['main']:
                result['main'].update(partial_search_main(alt_norm))

    # JRN
    if q_norm in jrone_norm_to_art:
        result['jrn'].update(jrone_norm_to_art[q_norm])
    elif partial and len(q_norm) >= MIN_SEARCH_LENGTH:
        for norm_key, arts in jrone_norm_to_art.items():
            if q_norm in norm_key:
                result['jrn'].update(arts)

    # OEM
    if q_norm in oem_norm_to_art:
        result['oem'].update(oem_norm_to_art[q_norm])
    elif partial and len(q_norm) >= MIN_SEARCH_LENGTH:
        for norm_key, arts in oem_norm_to_art.items():
            if q_norm in norm_key:
                result['oem'].update(arts)

    # FLP номер -> артикулы
    if q_norm in flp_norm_to_art:
        result['flp_art'].update(flp_norm_to_art[q_norm])
    elif partial and len(q_norm) >= MIN_SEARCH_LENGTH:
        for norm_key, arts in flp_norm_to_art.items():
            if q_norm in norm_key:
                result['flp_art'].update(arts)

    # Артикул -> FLP номера
    if q_norm in art_norm_to_flp:
        result['flp_num'].update(art_norm_to_flp[q_norm])
    elif partial and len(q_norm) >= MIN_SEARCH_LENGTH:
        for norm_key, nums in art_norm_to_flp.items():
            if q_norm in norm_key:
                result['flp_num'].update(nums)

    return result

def total_found(result: Dict[str, Set[str]]) -> int:
    return sum(len(v) for v in result.values())

def format_art_with_links(art: str) -> str:
    if art in dict_by_col1:
        eee_list = sorted(set(dict_by_col1[art]))
        return f"• {art} → {', '.join(eee_list[:8])}{' ...' if len(eee_list) > 8 else ''}"
    if art in dict_by_col2:
        turbo_list = sorted(set(dict_by_col2[art]))
        return f"• {art} → {', '.join(turbo_list[:8])}{' ...' if len(turbo_list) > 8 else ''}"
    return f"• {art}"

def format_search_result(query: str, result: Dict[str, Set[str]], title: Optional[str] = None) -> str:
    lines = []
    if title:
        lines.append(title)
    else:
        lines.append(f"🔎 Запрос: {query}")

    count = total_found(result)
    if count == 0:
        lines.append(f"❌ Ничего не найдено по запросу `{query}`.")
        return '\n'.join(lines)

    lines.append(f"✅ Найдено совпадений: {count}")

    def add_block(header: str, values: Iterable[str], formatter=lambda x: f"• {x}"):
        values = sorted(set(values))
        if not values:
            return
        lines.append("")
        lines.append(header)
        shown = values[:MAX_LINES_PER_BLOCK]
        lines.extend(formatter(v) for v in shown)
        if len(values) > MAX_LINES_PER_BLOCK:
            lines.append(f"…и ещё {len(values) - MAX_LINES_PER_BLOCK}. Уточните номер для более точного поиска.")

    add_block("📦 Основная база:", result['main'])
    add_block("🔁 JRN-кроссы:", result['jrn'], format_art_with_links)
    add_block("🏷 OEM-кроссы:", result['oem'])
    add_block("📌 FLP артикулы:", result['flp_art'], lambda x: f"• FLP артикул: {x}")
    add_block("📌 FLP номера:", result['flp_num'], lambda x: f"• FLP номер: {x}")
    return '\n'.join(lines)

# ---------- VIN-функции ----------
def http_json_request(url: str, *, method: str = 'GET', payload: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 12) -> dict:
    data = None
    req_headers = {'User-Agent': 'TurbonizerBot/1.0'}
    if headers:
        req_headers.update(headers)
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
        req_headers['Content-Type'] = 'application/json'
    request = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode('utf-8', errors='replace'))

def decode_vin_nhtsa(vin: str) -> Tuple[Optional[dict], Optional[str]]:
    vin = normalize_text(vin)
    url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValuesExtended/' + urllib.parse.quote(vin) + '?format=json'
    try:
        data = http_json_request(url)
        rows = data.get('Results') or []
        if not rows:
            return None, 'NHTSA не вернул данные по VIN.'
        row = rows[0]
        return {
            'vin': vin,
            'make': clean_text(row.get('Make', '')),
            'model': clean_text(row.get('Model', '')),
            'year': clean_text(row.get('ModelYear', '')),
            'trim': clean_text(row.get('Trim', '')),
            'vehicle_type': clean_text(row.get('VehicleType', '')),
            'engine_model': clean_text(row.get('EngineModel', '')),
            'engine_kw': clean_text(row.get('EngineKW', '')),
            'engine_hp': clean_text(row.get('EngineHP', '')),
            'displacement_l': clean_text(row.get('DisplacementL', '')),
            'fuel': clean_text(row.get('FuelTypePrimary', '')),
            'plant_country': clean_text(row.get('PlantCountry', '')),
            'error_code': clean_text(row.get('ErrorCode', '')),
            'error_text': clean_text(row.get('ErrorText', '')),
        }, None
    except Exception as e:
        return None, f'Ошибка VIN-декодера: {e}'

def choose_search_provider() -> Optional[str]:
    provider = SEARCH_PROVIDER
    if provider in ('brave', 'serpapi'):
        return provider
    if BRAVE_SEARCH_API_KEY:
        return 'brave'
    if SERPAPI_API_KEY:
        return 'serpapi'
    return None

def compact_vehicle_description(vehicle: dict) -> str:
    bits = []
    if vehicle.get('make'):
        bits.append(vehicle['make'])
    if vehicle.get('model'):
        bits.append(vehicle['model'])
    if vehicle.get('year'):
        bits.append(vehicle['year'])
    if vehicle.get('displacement_l'):
        bits.append(vehicle['displacement_l'] + 'L')
    if vehicle.get('engine_model'):
        bits.append(vehicle['engine_model'])
    if vehicle.get('fuel'):
        bits.append(vehicle['fuel'])
    return ' '.join(bits).strip()

def build_vin_web_queries(vin: str, vehicle: dict) -> List[str]:
    vin = normalize_text(vin)
    vehicle_desc = compact_vehicle_description(vehicle)
    prefix8 = vin[:8]
    prefix6 = vin[:6]

    queries = []
    # Сначала ищем по VIN / префиксу: полезно для европейских VIN, где внешний декодер вернул мало данных.
    queries.append(f'"{vin}" turbocharger turbo OEM')
    queries.append(f'"{prefix8}" turbocharger turbo OEM')
    queries.append(f'"{prefix8}" model turbocharger')

    if vehicle_desc:
        queries.append(f'{vehicle_desc} turbocharger OEM number')
        queries.append(f'{vehicle_desc} turbo part number Garrett BorgWarner KKK IHI MHI')
    else:
        queries.append(f'{prefix6} {prefix8} turbocharger OEM number')

    # Дедупликация с сохранением порядка
    seen = set()
    final = []
    for q in queries:
        q = clean_text(q)
        if q and q.lower() not in seen:
            seen.add(q.lower())
            final.append(q)
    return final[:5]

def search_web(query: str) -> Tuple[List[dict], Optional[str]]:
    provider = choose_search_provider()
    if not provider:
        return [], "Не задан ключ поискового API: BRAVE_SEARCH_API_KEY или SERPAPI_API_KEY."

    try:
        if provider == 'brave':
            url = 'https://api.search.brave.com/res/v1/web/search?' + urllib.parse.urlencode({
                'q': query,
                'count': min(max(WEB_SEARCH_MAX_RESULTS, 1), 20),
                'search_lang': 'en',
                'country': 'us',
                'safesearch': 'moderate',
            })
            data = http_json_request(url, headers={
                'Accept': 'application/json',
                'X-Subscription-Token': BRAVE_SEARCH_API_KEY,
            }, timeout=WEB_SEARCH_TIMEOUT)
            items = (data.get('web') or {}).get('results') or []
            return [
                {
                    'title': clean_text(i.get('title', '')),
                    'url': clean_text(i.get('url', '')),
                    'snippet': clean_text(i.get('description', '')),
                }
                for i in items
                if i.get('url')
            ], None

        if provider == 'serpapi':
            url = 'https://serpapi.com/search.json?' + urllib.parse.urlencode({
                'engine': 'google',
                'q': query,
                'api_key': SERPAPI_API_KEY,
                'num': min(max(WEB_SEARCH_MAX_RESULTS, 1), 10),
                'hl': 'en',
            })
            data = http_json_request(url, timeout=WEB_SEARCH_TIMEOUT)
            items = data.get('organic_results') or []
            return [
                {
                    'title': clean_text(i.get('title', '')),
                    'url': clean_text(i.get('link', '')),
                    'snippet': clean_text(i.get('snippet', '')),
                }
                for i in items
                if i.get('link')
            ], None
    except Exception as e:
        return [], f'Ошибка интернет-поиска ({provider}): {e}'

    return [], 'Неизвестный SEARCH_PROVIDER.'

def search_web_for_vin(vin: str, vehicle: dict) -> Tuple[List[dict], List[str], List[str]]:
    queries = build_vin_web_queries(vin, vehicle)
    all_results = []
    errors = []
    seen_urls = set()
    for q in queries:
        results, error = search_web(q)
        if error:
            errors.append(error)
            # если нет ключа, нет смысла повторять остальные запросы
            if 'Не задан ключ' in error:
                break
            continue
        for r in results:
            url = r.get('url')
            if url and url not in seen_urls:
                seen_urls.add(url)
                r['query'] = q
                all_results.append(r)
    return all_results[:40], queries, errors

def extract_response_text(data: dict) -> str:
    # Responses API обычно возвращает output_text, но оставим совместимый разбор.
    if isinstance(data.get('output_text'), str):
        return data['output_text']
    parts = []
    for item in data.get('output', []) or []:
        for content in item.get('content', []) or []:
            if content.get('type') in ('output_text', 'text') and content.get('text'):
                parts.append(content['text'])
    return '\n'.join(parts).strip()

def extract_turbo_numbers_with_openai(vin: str, vehicle: dict, results: List[dict]) -> Tuple[List[dict], Optional[str]]:
    if not OPENAI_API_KEY:
        return [], None
    if not results:
        return [], None

    compact_results = []
    for idx, r in enumerate(results[:25], start=1):
        compact_results.append({
            'id': idx,
            'title': r.get('title', ''),
            'url': r.get('url', ''),
            'snippet': r.get('snippet', ''),
        })

    schema = {
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'vehicle_guess': {'type': 'string'},
            'candidates': {
                'type': 'array',
                'items': {
                    'type': 'object',
                    'additionalProperties': False,
                    'properties': {
                        'number': {'type': 'string'},
                        'kind': {'type': 'string', 'enum': ['Turbo P/N', 'OEM P/N', 'Vehicle OE No', 'CHRA P/N', 'Other']},
                        'confidence': {'type': 'string', 'enum': ['low', 'medium', 'high']},
                        'why': {'type': 'string'},
                        'source_ids': {'type': 'array', 'items': {'type': 'integer'}},
                    },
                    'required': ['number', 'kind', 'confidence', 'why', 'source_ids'],
                },
            },
            'warning': {'type': 'string'},
        },
        'required': ['vehicle_guess', 'candidates', 'warning'],
    }

    prompt = {
        'vin': normalize_text(vin),
        'decoded_vehicle': vehicle,
        'search_results': compact_results,
        'task': (
            'Extract only turbocharger-related part numbers for this VIN/vehicle from the search results. '
            'Return possible turbocharger numbers, OEM turbo numbers, vehicle OE turbo numbers, or CHRA numbers. '
            'Do not include unrelated engine, filter, gasket, or random part numbers. '
            'If evidence is weak, mark confidence low. Prefer numbers explicitly near words like turbocharger, turbo, BorgWarner, KKK, Garrett, IHI, MHI, CHRA, actuator. '
            'This is for preliminary lookup only, not final fitment.'
        )
    }

    payload = {
        'model': OPENAI_MODEL,
        'instructions': 'You are an automotive turbocharger parts extraction assistant. Output only valid JSON matching the schema.',
        'input': json.dumps(prompt, ensure_ascii=False),
        'text': {
            'format': {
                'type': 'json_schema',
                'name': 'turbo_number_extraction',
                'schema': schema,
                'strict': True,
            }
        },
        'temperature': 0,
        'max_output_tokens': 1400,
    }

    try:
        data = http_json_request(
            'https://api.openai.com/v1/responses',
            method='POST',
            payload=payload,
            headers={'Authorization': f'Bearer {OPENAI_API_KEY}'},
            timeout=OPENAI_TIMEOUT,
        )
        text = extract_response_text(data)
        parsed = json.loads(text)
        candidates = parsed.get('candidates') or []
        cleaned = []
        seen = set()
        for c in candidates:
            number = clean_text(c.get('number', '')).upper()
            # Отсекаем VIN-подобные строки и слишком короткий мусор.
            if not number or is_vin(number) or len(normalize(number)) < 5:
                continue
            key = normalize(number)
            if key in seen:
                continue
            seen.add(key)
            c['number'] = number
            cleaned.append(c)
        return cleaned[:MAX_VIN_CANDIDATES_TO_SEARCH], None
    except Exception as e:
        return [], f'Ошибка LLM-анализа интернет-выдачи: {e}'

def regex_extract_candidate_numbers(results: List[dict]) -> List[dict]:
    """Грубый запасной вариант, если LLM не подключён. Лучше использовать только как подсказку."""
    patterns = [
        r'\b\d{4}[-\s]?(?:970|988|710|715|988|998)[-\s]?\d{4}\b',  # BorgWarner/KKK-like
        r'\b\d{6}[-\s]?\d{4}\b',                                    # Garrett-like short form
        r'\b[A-Z]\d{2}\s?\d{3}\s?\d{2}\s?\d{99}\b',                 # почти не сработает, оставлено без вреда
        r'\b0[0-9A-Z]{2}\s?253\s?0[0-9A-Z]{2,4}\b',                 # VAG turbo OE-like: 03L253...
        r'\b[0-9A-Z]{2,4}253[0-9A-Z]{2,6}\b',                        # VAG compact
    ]
    turbo_words = re.compile(r'\b(turbo|turbocharger|borgwarner|garrett|kkk|ihi|mhi|chra|actuator)\b', re.I)
    out = []
    seen = set()
    for idx, r in enumerate(results, start=1):
        text = ' '.join([r.get('title',''), r.get('snippet','')])
        if not turbo_words.search(text):
            continue
        for pat in patterns:
            for m in re.finditer(pat, text, re.I):
                number = clean_text(m.group(0)).upper().replace(' ', '-')
                key = normalize(number)
                if len(key) < 5 or key in seen or is_vin(number):
                    continue
                seen.add(key)
                out.append({
                    'number': number,
                    'kind': 'Other',
                    'confidence': 'low',
                    'why': 'Найдено regex-поиском рядом с turbo-словами; требуется проверка.',
                    'source_ids': [idx],
                })
    return out[:MAX_VIN_CANDIDATES_TO_SEARCH]

def candidates_to_numbers(candidates: List[dict]) -> List[str]:
    nums = []
    for c in candidates:
        n = clean_text(c.get('number', ''))
        if n:
            nums.append(n)
    return sorted(set(nums))

def source_lines_for_candidates(candidates: List[dict], results: List[dict], limit: int = 5) -> List[str]:
    ids = []
    for c in candidates:
        for sid in c.get('source_ids', []) or []:
            if isinstance(sid, int) and sid not in ids:
                ids.append(sid)
    lines = []
    for sid in ids[:limit]:
        if 1 <= sid <= len(results):
            r = results[sid - 1]
            title = r.get('title') or r.get('url') or 'source'
            url = r.get('url') or ''
            lines.append(f"• {title[:70]} — {url}")
    return lines

async def handle_vin(update: Update, vin: str):
    await update.message.reply_text("🔎 VIN распознан. Расшифровываю автомобиль и ищу возможные номера турбины в интернете…")

    vehicle, error = decode_vin_nhtsa(vin)
    if error or not vehicle:
        await update.message.reply_text(
            f"❌ Не удалось расшифровать VIN {vin}.\n{error or ''}\n\n"
            "Для точного подбора пришлите номер с шильдика турбины или OEM-номер."
        )
        return

    vehicle_lines = [
        f"🔎 VIN: {vehicle['vin']}",
        "",
        "🚗 Автомобиль по VIN-декодеру:",
        f"• Марка: {vehicle.get('make') or '—'}",
        f"• Модель: {vehicle.get('model') or '—'}",
        f"• Год: {vehicle.get('year') or '—'}",
    ]
    if vehicle.get('trim'):
        vehicle_lines.append(f"• Комплектация: {vehicle['trim']}")
    engine_bits = []
    if vehicle.get('displacement_l'):
        engine_bits.append(f"{vehicle['displacement_l']} L")
    if vehicle.get('engine_model'):
        engine_bits.append(vehicle['engine_model'])
    if vehicle.get('fuel'):
        engine_bits.append(vehicle['fuel'])
    if engine_bits:
        vehicle_lines.append(f"• Двигатель: {', '.join(engine_bits)}")

    web_results, queries, web_errors = search_web_for_vin(vehicle['vin'], vehicle)

    if web_errors and not web_results:
        vehicle_lines.extend([
            "",
            "⚠️ Интернет-поиск не выполнен.",
            *[f"• {e}" for e in sorted(set(web_errors))],
            "",
            "Чтобы включить режим интернет-подсказки, задайте BRAVE_SEARCH_API_KEY или SERPAPI_API_KEY.",
            "Для точного подбора пока пришлите фото шильдика турбины или номер турбины/OEM."
        ])
        await update.message.reply_text('\n'.join(vehicle_lines))
        return

    candidates, llm_error = extract_turbo_numbers_with_openai(vehicle['vin'], vehicle, web_results)
    extraction_note = None
    if not candidates:
        candidates = regex_extract_candidate_numbers(web_results)
        if candidates:
            extraction_note = "⚠️ OPENAI_API_KEY не задан или LLM не вернул кандидатов. Номера извлечены грубым regex-поиском, уверенность низкая."

    candidate_numbers = candidates_to_numbers(candidates)

    if not candidate_numbers:
        vehicle_lines.extend([
            "",
            "🌐 Интернет-поиск выполнен, но надёжные номера турбины/OEM не извлечены.",
            "",
            "Поисковые запросы:",
            *[f"• {q}" for q in queries[:5]],
        ])
        if llm_error:
            vehicle_lines.extend(["", f"⚠️ {llm_error}"])
        vehicle_lines.extend([
            "",
            "📌 Для точного подбора пришлите фото шильдика турбины или номер Garrett / BorgWarner / IHI / MHI / OEM."
        ])
        await update.message.reply_text('\n'.join(vehicle_lines))
        return

    vehicle_lines.extend([
        "",
        "🌐 Возможные номера турбины/OEM, найденные через интернет:",
    ])
    for c in candidates[:15]:
        vehicle_lines.append(f"• {c['number']} — {c.get('kind', 'номер')}, уверенность: {c.get('confidence', 'low')}")
    if len(candidates) > 15:
        vehicle_lines.append(f"…и ещё {len(candidates) - 15}")

    # Ищем найденные интернетом номера в твоей E&E базе.
    combined = {'main': set(), 'jrn': set(), 'oem': set(), 'flp_art': set(), 'flp_num': set()}
    matched_numbers = []
    for num in candidate_numbers[:MAX_VIN_CANDIDATES_TO_SEARCH]:
        res = search_all_sources(num, partial=False)
        if total_found(res) == 0:
            continue
        matched_numbers.append(num)
        for key in combined:
            combined[key].update(res[key])

    if total_found(combined) == 0:
        vehicle_lines.extend([
            "",
            "❌ По найденным в интернете номерам совпадений в E&E базе пока нет.",
            "📌 Проверьте номер по шильдику турбины или пришлите фото шильдика."
        ])
    else:
        vehicle_lines.extend([
            "",
            f"✅ Совпадения в E&E базе найдены по номерам: {', '.join(matched_numbers[:8])}{' ...' if len(matched_numbers) > 8 else ''}",
            "",
            format_search_result("VIN-интернет-кандидаты", combined, title="📦 Подходящие E&E артикулы:"),
        ])

    source_lines = source_lines_for_candidates(candidates, web_results, limit=5)
    if source_lines:
        vehicle_lines.extend(["", "🔗 Источники интернет-подсказки:", *source_lines])

    if extraction_note:
        vehicle_lines.extend(["", extraction_note])
    if llm_error:
        vehicle_lines.extend(["", f"⚠️ {llm_error}"])

    vehicle_lines.extend([
        "",
        "⚠️ Это предварительная интернет-подсказка, а не финальный подбор. Обязательно проверьте номер по шильдику турбины: на одной машине могут стоять разные турбины по году, рынку, мощности и замене."
    ])

    await update.message.reply_text('\n'.join(vehicle_lines))

# ---------- Обработчики ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    emoji_id = "5247029251940586192"
    welcome_text = (
        f"<tg-emoji emoji-id=\"{emoji_id}\">😊</tg-emoji> ТУРБОНАЙЗЕР бот приветствует!\n"
        "Введите E&E P/N, Turbo P/N, OEM номер, JRN-номер или VIN-код.\n\n"
        "Пример номера: CT-VNT11B или 17201-52010\n"
        "Пример VIN: WV1ZZZ2HZFH012345\n\n"
        f"🔍 Можно искать по части номера (минимум {MIN_SEARCH_LENGTH} символа).\n"
        "Дефисы можно не ставить – бот поймёт.\n"
        "Также бот понимает русские буквы, похожие на латинские (например, Е = E, Н = H)."
    )
    await update.message.reply_text(welcome_text, parse_mode='HTML')

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = clean_text(update.message.text)
    if not user_input:
        return

    if is_vin(user_input):
        await handle_vin(update, normalize_text(user_input))
        return

    result = search_all_sources(user_input, partial=True)
    await update.message.reply_text(format_search_result(user_input, result))

def main():
    app = Application.builder().token(API_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("🚀 ТУРБОНАЙЗЕР бот с VIN-режимом запущен...")
    app.run_polling()

if __name__ == '__main__':
    main()
