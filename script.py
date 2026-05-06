import csv
import os
import re
from collections import defaultdict
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

# ---------- Получение токена из переменной окружения ----------
API_TOKEN = os.environ.get('API_TOKEN')
if API_TOKEN is None:
    raise ValueError("❌ Переменная окружения API_TOKEN не задана!")

# ---------- Константы ----------
MIN_SEARCH_LENGTH = 4
DATA_FILE = 'data.csv'
JRONE_FILE = 'jronecross.csv'
OEM_FILE = 'oemcross.csv'
FLP_FILE = 'flp.csv'

# ---------- Очистка текста ----------
def clean_text(s):
    s = s.strip()
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

def replace_cyrillic_like_latin(s):
    return ''.join(CYRILLIC_TO_LATIN.get(ch, ch) for ch in s)

def normalize(s):
    s = replace_cyrillic_like_latin(s)
    return s.replace('-', '').lower()

def is_11_digit_number(s):
    return re.fullmatch(r'\d{11}', s) is not None

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
jrone_norm_to_art = defaultdict(set)   # нормализованный JRN -> множество артикулов

try:
    with open(JRONE_FILE, mode='r', encoding='utf-8-sig') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            if len(row) >= 3:
                jrone = clean_text(row[0])
                our_art = clean_text(row[2])      # третья колонка — наш артикул
                if jrone and our_art:
                    norm = normalize(jrone)
                    jrone_norm_to_art[norm].add(our_art)
except FileNotFoundError:
    print("⚠️ Файл jronecross.csv не найден, поиск по JRN-номерам недоступен.")
except Exception as e:
    print(f"❌ Ошибка загрузки {JRONE_FILE}: {e}")

print(f"✅ JRN-база: {len(jrone_norm_to_art)} уникальных нормализованных JRN-номеров.")

# ---------- Загрузка базы OEM-кроссов (oemcross.csv) ----------
oem_norm_to_art = defaultdict(set)   # нормализованный OEM -> множество артикулов

try:
    with open(OEM_FILE, mode='r', encoding='utf-8-sig') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            if len(row) >= 2:
                art = clean_text(row[0])
                oem = clean_text(row[1])
                if art and oem:
                    norm = normalize(oem)
                    oem_norm_to_art[norm].add(art)
except FileNotFoundError:
    print("⚠️ Файл oemcross.csv не найден, поиск по OEM-номерам недоступен.")
except Exception as e:
    print(f"❌ Ошибка загрузки {OEM_FILE}: {e}")

print(f"✅ OEM-база: {len(oem_norm_to_art)} уникальных нормализованных OEM-номеров.")

# ---------- Загрузка базы FLP-кроссов (flp.csv) ----------
flp_norm_to_art = defaultdict(set)   # нормализованный FLP номер -> множество артикулов
art_norm_to_flp = defaultdict(set)   # нормализованный артикул -> множество FLP номеров

try:
    with open(FLP_FILE, mode='r', encoding='utf-8-sig') as file:
        reader = csv.reader(file, delimiter=';')
        for row in reader:
            if len(row) >= 2:
                art = clean_text(row[0])
                flp = clean_text(row[1])
                if art and flp:
                    norm_flp = normalize(flp)
                    norm_art = normalize(art)
                    flp_norm_to_art[norm_flp].add(art)
                    art_norm_to_flp[norm_art].add(flp)
except FileNotFoundError:
    print("⚠️ Файл flp.csv не найден, поиск по FLP-номерам недоступен.")
except Exception as e:
    print(f"❌ Ошибка загрузки {FLP_FILE}: {e}")

print(f"✅ FLP-база: {len(flp_norm_to_art)} уникальных FLP-номеров, {len(art_norm_to_flp)} уникальных артикулов.")

# ---------- Функция частичного поиска в основной базе ----------
def partial_search_main(search_norm):
    results = set()
    for norm_key, original_keys in col1_norm_to_original.items():
        if search_norm in norm_key:
            for orig_key in original_keys:
                for val in dict_by_col1[orig_key]:
                    results.add(val)
    for norm_key, original_keys in col2_norm_to_original.items():
        if search_norm in norm_key:
            for orig_key in original_keys:
                for val in dict_by_col2[orig_key]:
                    results.add(val)
    return results

# ---------- Вспомогательная функция для форматирования артикула со связями ----------
def format_art_with_links(art):
    if art in dict_by_col1:
        eee_list = sorted(set(dict_by_col1[art]))
        return f"• {art} → {', '.join(eee_list)}"
    elif art in dict_by_col2:
        turbo_list = sorted(set(dict_by_col2[art]))
        return f"• {art} → {', '.join(turbo_list)}"
    else:
        return f"• {art} (нет в основной базе)"

# ---------- Обработчики ----------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    emoji_id = "5247029251940586192"
    welcome_text = (
        f"<tg-emoji emoji-id=\"{emoji_id}\">😊</tg-emoji> ТУРБОНАЙЗЕР бот приветствует!\n"
        "Введите E&E P/N, Turbo P/N, OEM номер или JRN-номер\n\n"
        "Пример: CT-VNT11B или 17201-52010\n\n"
        f"🔍 Можно искать по части номера (минимум {MIN_SEARCH_LENGTH} символа).\n"
        "Дефисы можно не ставить – бот поймёт.\n"
        "Также бот понимает русские буквы, похожие на латинские (например, Е = E, Н = H)."
    )
    await update.message.reply_text(welcome_text, parse_mode='HTML')

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_input = clean_text(update.message.text)
    if not user_input:
        return

    user_input_norm = normalize(user_input)
    input_len = len(user_input_norm)

    # Списки для результатов из разных источников
    main_lines = []      # строки из основной базы data.csv
    jrone_lines = []     # строки из JRN (артикулы со связями)
    oem_lines = []       # строки из OEM (просто артикулы)
    flp_lines = []       # строки из FLP (артикулы или номера)

    # ------------------ ПОИСК В ОСНОВНОЙ БАЗЕ (data.csv) ------------------
    if input_len < MIN_SEARCH_LENGTH:
        # Точный поиск
        if user_input_norm in col2_norm_to_original:
            for key in col2_norm_to_original[user_input_norm]:
                for val in dict_by_col2[key]:
                    main_lines.append(f"• {val}")
        elif user_input_norm in col1_norm_to_original:
            for key in col1_norm_to_original[user_input_norm]:
                for val in dict_by_col1[key]:
                    main_lines.append(f"• {val}")
    else:
        # Частичный поиск
        results = partial_search_main(user_input_norm)
        for val in sorted(results):
            main_lines.append(f"• {val}")

        # Если ничего не найдено, пробуем заменить среднюю часть на 970 для 11-значных номеров
        if not results and is_11_digit_number(user_input_norm):
            first4 = user_input_norm[:4]
            middle3 = user_input_norm[4:7]
            last4 = user_input_norm[7:]
            if middle3 != '970':
                new_norm = first4 + '970' + last4
                results = partial_search_main(new_norm)
                for val in sorted(results):
                    main_lines.append(f"• {val}")

    # ------------------ ПОИСК В JRN ------------------
    jrone_arts = set()
    if input_len < MIN_SEARCH_LENGTH:
        if user_input_norm in jrone_norm_to_art:
            jrone_arts = jrone_norm_to_art[user_input_norm]
    else:
        for norm_key, arts in jrone_norm_to_art.items():
            if user_input_norm in norm_key:
                jrone_arts.update(arts)

    for art in sorted(jrone_arts):
        jrone_lines.append(format_art_with_links(art))

    # ------------------ ПОИСК В OEM ------------------
    oem_arts = set()
    if input_len < MIN_SEARCH_LENGTH:
        if user_input_norm in oem_norm_to_art:
            oem_arts = oem_norm_to_art[user_input_norm]
    else:
        for norm_key, arts in oem_norm_to_art.items():
            if user_input_norm in norm_key:
                oem_arts.update(arts)

    for art in sorted(oem_arts):
        oem_lines.append(f"• {art}")

    # ------------------ ПОИСК В FLP (двунаправленный) ------------------
    flp_arts = set()
    flp_nums = set()
    # Ищем как FLP номер -> артикулы
    if input_len < MIN_SEARCH_LENGTH:
        if user_input_norm in flp_norm_to_art:
            flp_arts = flp_norm_to_art[user_input_norm]
    else:
        for norm_key, arts in flp_norm_to_art.items():
            if user_input_norm in norm_key:
                flp_arts.update(arts)

    # Ищем как артикул -> FLP номера
    if input_len < MIN_SEARCH_LENGTH:
        if user_input_norm in art_norm_to_flp:
            flp_nums = art_norm_to_flp[user_input_norm]
    else:
        for norm_key, nums in art_norm_to_flp.items():
            if user_input_norm in norm_key:
                flp_nums.update(nums)

    # Собираем строки для FLP
    for art in sorted(flp_arts):
        flp_lines.append(f"• FLP артикул: {art}")
    for num in sorted(flp_nums):
        flp_lines.append(f"• FLP номер: {num}")

    # ------------------ ФОРМИРОВАНИЕ ОТВЕТА (без заголовков) ------------------
    answer_lines = []
    # Основная база всегда первой
    if main_lines:
        answer_lines.extend(main_lines)
    # Добавляем JRN, если есть
    if jrone_lines:
        if answer_lines:
            answer_lines.append("")  # пустая строка для разделения блоков
        answer_lines.extend(jrone_lines)
    # Добавляем OEM
    if oem_lines:
        if answer_lines and not (answer_lines[-1] == ""):
            answer_lines.append("")
        answer_lines.extend(oem_lines)
    # Добавляем FLP
    if flp_lines:
        if answer_lines and not (answer_lines[-1] == ""):
            answer_lines.append("")
        answer_lines.extend(flp_lines)

    # Если ничего не найдено
    if not answer_lines:
        answer_lines.append(f"❌ Ничего не найдено по запросу `{user_input}`.")

    await update.message.reply_text("\n".join(answer_lines))

def main():
    app = Application.builder().token(API_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    print("🚀 ТУРБОНАЙЗЕР бот с приоритетом data.csv и объединёнными результатами без заголовков запущен...")
    app.run_polling()

if __name__ == '__main__':
    main()
