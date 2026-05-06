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

# Необязательный локальный файл для связки VIN/авто -> OEM/номера турбины.
# Формат с ; и заголовками:
# vin_prefix;make;model;year_from;year_to;engine;oem_numbers
# WV1ZZZ2H;VOLKSWAGEN;AMAROK;2010;2016;2.0 TDI;03L253016T, 03L253056G, 5304-970-0128
VIN_OEM_FILE = 'vin_oem.csv'

# Необязательный внешний API с данными по запчастям.
# Ожидаемый ответ JSON:
# {
#   "oem_numbers": ["03L253016T", "03L253056G"],
#   "turbo_numbers": ["5304-970-0128"],
#   "notes": "optional text"
# }
VIN_PARTS_API_URL = os.environ.get('VIN_PARTS_API_URL')
VIN_PARTS_API_KEY = os.environ.get('VIN_PARTS_API_KEY')

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

# ---------- Локальная VIN -> OEM база ----------
vin_oem_rows = []
try:
    with open(VIN_OEM_FILE, mode='r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=';')
        for row in reader:
            vin_prefix = clean_text(row.get('vin_prefix', '')).upper()
            oem_numbers = clean_text(row.get('oem_numbers', ''))
            if vin_prefix and oem_numbers:
                vin_oem_rows.append({
                    'vin_prefix': vin_prefix,
                    'make': clean_text(row.get('make', '')).upper(),
                    'model': clean_text(row.get('model', '')).upper(),
                    'year_from': clean_text(row.get('year_from', '')),
                    'year_to': clean_text(row.get('year_to', '')),
                    'engine': clean_text(row.get('engine', '')).upper(),
                    'oem_numbers': split_numbers(oem_numbers) if 'split_numbers' in globals() else []
                })
except FileNotFoundError:
    print("ℹ️ vin_oem.csv не найден. VIN будет расшифровываться, но без локальной привязки к OEM-номерам.")
except Exception as e:
    print(f"❌ Ошибка загрузки {VIN_OEM_FILE}: {e}")

# split_numbers нужен выше, поэтому если файл успел загрузиться до объявления функции — поправим ниже.
def split_numbers(value: str) -> List[str]:
    raw = re.split(r'[,;|\s]+', value or '')
    return [clean_text(x) for x in raw if clean_text(x)]

# Если vin_oem.csv был прочитан до объявления split_numbers, перечитаем корректно.
if vin_oem_rows and not vin_oem_rows[0].get('oem_numbers'):
    vin_oem_rows = []
    try:
        with open(VIN_OEM_FILE, mode='r', encoding='utf-8-sig') as file:
            reader = csv.DictReader(file, delimiter=';')
            for row in reader:
                vin_prefix = clean_text(row.get('vin_prefix', '')).upper()
                oem_numbers = clean_text(row.get('oem_numbers', ''))
                if vin_prefix and oem_numbers:
                    vin_oem_rows.append({
                        'vin_prefix': vin_prefix,
                        'make': clean_text(row.get('make', '')).upper(),
                        'model': clean_text(row.get('model', '')).upper(),
                        'year_from': clean_text(row.get('year_from', '')),
                        'year_to': clean_text(row.get('year_to', '')),
                        'engine': clean_text(row.get('engine', '')).upper(),
                        'oem_numbers': split_numbers(oem_numbers),
                    })
    except Exception:
        pass
print(f"✅ VIN-OEM база: {len(vin_oem_rows)} правил.")

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

def find_oem_numbers_for_vin_locally(vin: str, vehicle: dict) -> List[str]:
    vin = normalize_text(vin)
    make = normalize_text(vehicle.get('make', ''))
    model = normalize_text(vehicle.get('model', ''))
    year_raw = vehicle.get('year', '')
    try:
        year = int(year_raw)
    except Exception:
        year = None

    numbers = []
    for row in vin_oem_rows:
        if not vin.startswith(row['vin_prefix'].upper()):
            continue
        if row.get('make') and normalize_text(row['make']) not in make:
            continue
        if row.get('model') and normalize_text(row['model']) not in model:
            continue
        if year is not None:
            try:
                yf = int(row['year_from']) if row['year_from'] else None
                yt = int(row['year_to']) if row['year_to'] else None
                if yf and year < yf:
                    continue
                if yt and year > yt:
                    continue
            except Exception:
                pass
        numbers.extend(row.get('oem_numbers', []))
    return sorted(set(numbers))

def fetch_turbo_numbers_from_parts_api(vin: str, vehicle: dict) -> Tuple[List[str], Optional[str]]:
    if not VIN_PARTS_API_URL:
        return [], None
    headers = {}
    if VIN_PARTS_API_KEY:
        headers['Authorization'] = f'Bearer {VIN_PARTS_API_KEY}'
    try:
        data = http_json_request(
            VIN_PARTS_API_URL,
            method='POST',
            payload={'vin': normalize_text(vin), 'vehicle': vehicle, 'group': 'turbocharger'},
            headers=headers,
            timeout=20,
        )
        numbers = []
        for key in ('oem_numbers', 'turbo_numbers', 'numbers'):
            value = data.get(key, [])
            if isinstance(value, str):
                numbers.extend(split_numbers(value))
            elif isinstance(value, list):
                numbers.extend(str(x) for x in value if str(x).strip())
        note = clean_text(data.get('notes', '') or data.get('note', ''))
        return sorted(set(clean_text(n) for n in numbers if clean_text(n))), note or None
    except Exception as e:
        return [], f'⚠️ Ошибка внешнего VIN-parts API: {e}'

async def handle_vin(update: Update, vin: str):
    await update.message.reply_text("🔎 VIN распознан. Расшифровываю автомобиль и ищу возможные номера турбины…")

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
        "🚗 Автомобиль:",
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

    candidate_numbers = []
    candidate_numbers.extend(find_oem_numbers_for_vin_locally(vehicle['vin'], vehicle))
    api_numbers, api_note = fetch_turbo_numbers_from_parts_api(vehicle['vin'], vehicle)
    candidate_numbers.extend(api_numbers)
    candidate_numbers = sorted(set(candidate_numbers))[:MAX_VIN_CANDIDATES_TO_SEARCH]

    if not candidate_numbers:
        vehicle_lines.extend([
            "",
            "⚠️ Автомобиль расшифрован, но номера турбины/OEM не найдены.",
            "Чтобы VIN сразу давал E&E артикулы, нужно подключить внешний каталог запчастей через VIN_PARTS_API_URL или заполнить локальный vin_oem.csv.",
            "",
            "📌 Для точного подбора пришлите фото шильдика турбины или номер Garrett / BorgWarner / IHI / MHI / OEM."
        ])
        if api_note:
            vehicle_lines.append("")
            vehicle_lines.append(api_note)
        await update.message.reply_text('\n'.join(vehicle_lines))
        return

    vehicle_lines.append("")
    vehicle_lines.append("🧾 Возможные номера турбины/OEM по VIN:")
    vehicle_lines.extend(f"• {n}" for n in candidate_numbers[:15])
    if len(candidate_numbers) > 15:
        vehicle_lines.append(f"…и ещё {len(candidate_numbers) - 15}")

    # Ищем каждый найденный OEM/номер турбины в твоей E&E базе.
    combined = {'main': set(), 'jrn': set(), 'oem': set(), 'flp_art': set(), 'flp_num': set()}
    matched_numbers = []
    for num in candidate_numbers:
        res = search_all_sources(num, partial=False)
        if total_found(res) == 0:
            # иногда OEM в каталоге записан без дефисов/точек, partial=False уже ищет по нормализованному ключу;
            # если ничего нет, не расширяем частично, чтобы VIN не выдавал мусор.
            continue
        matched_numbers.append(num)
        for key in combined:
            combined[key].update(res[key])

    if total_found(combined) == 0:
        vehicle_lines.extend([
            "",
            "❌ Эти номера пока не дали совпадений в E&E базе.",
            "📌 Для точного подбора всё равно лучше запросить фото шильдика турбины."
        ])
    else:
        vehicle_lines.extend([
            "",
            f"✅ Совпадения в E&E базе найдены по номерам: {', '.join(matched_numbers[:8])}{' ...' if len(matched_numbers) > 8 else ''}",
        ])
        vehicle_lines.append("")
        vehicle_lines.append(format_search_result("VIN-кандидаты", combined, title="📦 Подходящие E&E артикулы:"))
        vehicle_lines.extend([
            "",
            "⚠️ VIN-подбор не должен быть финальным без проверки шильдика: на одной машине могут стоять разные турбины по году, рынку, мощности и замене." 
        ])
    if api_note:
        vehicle_lines.append("")
        vehicle_lines.append(api_note)

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
