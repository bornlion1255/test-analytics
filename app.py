import streamlit as st
import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px  
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# 1. КОНФИГУРАЦИЯ И БЕЗОПАСНОСТЬ
# ==========================================
st.set_page_config(page_title="SLA Dashboard Hybrid", layout="wide")

# Загружаем секреты напрямую. 
# Если их нет в st.secrets, программа выдаст ошибку — это безопаснее, чем утечка токена.
try:
    API_TOKEN = st.secrets["API_TOKEN"]
    SHEET_ID  = st.secrets["SHEET_ID"]
    GID       = st.secrets["GID"]
    SECRET_PASSWORD = st.secrets["PASSWORD"]
except KeyError as e:
    st.error(f"❌ Критическая ошибка: В секретах Streamlit не найдено поле {e}")
    st.stop()

# КОНСТАНТЫ (теперь они чистые)
BASE_URL = "https://api.chat2desk.com/v1"
HEADERS  = {"Authorization": API_TOKEN}
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

MAX_WORKERS = 20
TIME_OFFSET = 3

# СПРАВОЧНИКИ
OPERATORS_MAP = {310507: "Бот AI", 0: "Система"}
DEPARTMENT_MAPPING = {
    "Никита Приходько": "Concierge", 
    "Алина Федулова": "Тренер",
    "Илья Аврамов": "Appointment",
    "Виктория Суворова": "Appointment",
    "Кирилл Минаев": "Appointment",
    "Мария Попова": "Без отдела",
    "Станислав Басов": "Claims",
    "Милена Говорова": "Без отдела",
    "Надежда Смирнова": "Сопровождение",
    "Ирина Вережан": "Claims",
    "Наталья Половникова": "Claims",
    "Администратор": "Без отдела",
    "Владимир Асатрян": "Без отдела",
    "Екатерина Ермакова": "Без отдела",
    "Константин Гетман": "SMM",
    "Екатерина Анисимова": "Без отдела",
    "Оля Трущелева": "Без отдела",
    "Алина Новикова": "SALE",
    "Иван Савицкий": "SALE",
    "Анастасия Ванян": "SALE",
    "Павел Новиков": "SALE",
    "Александра Шаповал": "SMM",
    "Георгий Астапов": "Deep_support",
    "Елена Панова": "Deep_support",
    "Татьяна Сошникова": "SMM",
    "Виктория Вороняк": "SMM",
    "Анна Чернышова": "SMM",
    "Алина Ребрина": "Claims",
    "Алена Воронина": "Claims",
    "Ксения Бухонина": "Сопровождение",
    "Елизавета Давыденко": "Сопровождение",
    "Екатерина Кондратьева": "Сопровождение",
    "Ксения Гаврилова": "Claims",
    "Снежана Ефимова": "Сопровождение",
    "Анастасия Карпеева": "Claims",
    "Кристина Любина": "Сопровождение",
    "Наталья Серебрякова": "Сопровождение",
    "Константин Клишин": "Claims",
    "Наталья Баландина": "Claims",
    "Даниил Гусев": "Appointment",
    "Анна Власенкова": "SMM",
    "Регина Арендт": "Сопровождение",
    "Екатерина Щукина": "Сопровождение",
    "Ксения Кривко": "Claims",
    "Вероника Софронова": "SMM",
    "Юрий Кобелев": "Claims",
    "Арина Прохорова": "SMM"
}

CUSTOM_GROUPING = {
    "Cleaner_Payments": "Сопровождение",
    "Penalty": "Сопровождение",
    "Operations": "Сопровождение",
    "Storage": "Сопровождение"
}

# ==========================================
# 2. АВТОРИЗАЦИЯ
# ==========================================
def check_password():
    if "password_correct" not in st.session_state:
        st.session_state["password_correct"] = False
    
    if not st.session_state["password_correct"]:
        st.markdown("### 🔐 Вход в систему")
        with st.form("credentials"):
            password = st.text_input("Введите пароль доступа", type="password")
            submit = st.form_submit_button("Войти")
            
            if submit:
                if str(password).strip() == str(SECRET_PASSWORD).strip():
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("⛔ Неверный пароль")
        return False
    return True

if not check_password():
    st.stop()

# ==========================================
# 3. ФУНКЦИИ API И ОБРАБОТКИ
# ==========================================
def normalize_text(text):
    if not text: return ""
    return str(text).lower().strip().replace("ё", "е")

def find_department_smart(api_name_full):
    clean_api = normalize_text(api_name_full)
    for name, dept in DEPARTMENT_MAPPING.items():
        if normalize_text(name) == clean_api: return dept
    for name_key, dept in DEPARTMENT_MAPPING.items():
        parts = normalize_text(name_key).split()
        if not parts: continue
        if all(part in clean_api for part in parts): return dept
    return "Не определен"

def format_seconds(x):
    if pd.isna(x) or x is None: return "-"
    try:
        val = int(float(x))
        m, s = divmod(val, 60)
        h, m = divmod(m, 60)
        if h > 0: return f"{h}ч {m}м"
        return f"{m}м {s}с"
    except: return "-"

def process_single_dialog(item, target_start, target_end):
    req_id = item['req_id']
    try:
        r = requests.get(f"{BASE_URL}/requests/{req_id}/messages", headers=HEADERS, params={"limit": 300})
        if r.status_code != 200: return None
        json_data = r.json()
        msgs = json_data if isinstance(json_data, list) else json_data.get('data', [])
        msgs.sort(key=lambda x: x.get('created', 0))
        
        client_waiting_since = None
        stats = {
            'req_id': req_id,
            'participations': set(),
            'operator_speeds': {},
            'op_hours': {}, 
            'rating': item.get('rating')
        }
        
        for m in msgs:
            ts = m.get('created')
            if not ts: continue
            dt_utc = pd.to_datetime(ts, unit='s')
            dt_local = dt_utc + timedelta(hours=TIME_OFFSET)
            
            msg_type = m.get('type')
            op_id = m.get('operatorID') or m.get('operator_id')
            
            if msg_type == 'from_client' or msg_type == 'in':
                if client_waiting_since is None: client_waiting_since = dt_local
            
            # ИСПРАВЛЕНИЕ: Убрали ограничение на бота (310507), чтобы он тоже собирал статистику
            elif msg_type == 'out' and op_id and op_id != 0:
                 if target_start <= dt_local <= target_end:
                      stats['participations'].add(op_id)
                      
                      if op_id not in stats['op_hours']: stats['op_hours'][op_id] = set()
                      stats['op_hours'][op_id].add((dt_local.date(), dt_local.hour)) 
                      
                      if client_waiting_since:
                          diff = (dt_local - client_waiting_since).total_seconds()
                          if diff > 0:
                              if op_id not in stats['operator_speeds']: 
                                  stats['operator_speeds'][op_id] = []
                              stats['operator_speeds'][op_id].append(diff)
                          client_waiting_since = None
                      
        return stats
    except:
        return None

@st.cache_data(ttl=3600)
def load_api_data_range(start_date, end_date):
    try:
        r = requests.get(f"{BASE_URL}/operators", headers=HEADERS, params={"limit": 1000})
        for op in r.json().get('data', []):
            name = f"{op.get('first_name', '')} {op.get('last_name', '')}".strip()
            if not name: name = op.get('email', str(op['id']))
            OPERATORS_MAP[op['id']] = name
    except: pass
    
    all_active_requests = []
    date_list = pd.date_range(start_date, end_date).strftime("%Y-%m-%d").tolist()
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    for i, d_str in enumerate(date_list):
        status_text.text(f"Сбор списка чатов за {d_str}...")
        limit = 200; offset = 0
        while offset < 5000:
            try:
                params = {"report": "request_stats", "date": d_str, "limit": limit, "offset": offset}
                r = requests.get(f"{BASE_URL}/statistics", headers=HEADERS, params=params)
                data = r.json().get('data', [])
                if not data: break
                for row in data:
                    rating = row.get('rating_scale_score')
                    if rating == 0 or rating == '0': rating = None
                    all_active_requests.append({'req_id': row['request_id'], 'rating': rating})
                if len(data) < limit: break
                offset += limit
            except: break
        progress_bar.progress((i + 1) / (len(date_list) * 2))

    unique_requests = {v['req_id']: v for v in all_active_requests}.values()
    
    final_rows = []
    all_speeds = {}        
    all_first_speeds = {} 
    
    total = len(unique_requests)
    completed = 0
    
    target_start_global = pd.to_datetime(f"{start_date.strftime('%Y-%m-%d')} 00:00:00")
    target_end_global = pd.to_datetime(f"{end_date.strftime('%Y-%m-%d')} 23:59:59")
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_single_dialog, item, target_start_global, target_end_global): item for item in unique_requests}
        for future in as_completed(futures):
            res = future.result()
            if res and res['participations']:
                for op_id, speeds in res['operator_speeds'].items():
                    if op_id not in all_speeds: all_speeds[op_id] = []
                    all_speeds[op_id].extend(speeds)
                    
                    if speeds:
                        if op_id not in all_first_speeds: all_first_speeds[op_id] = []
                        all_first_speeds[op_id].append(speeds[0])

                for op_id in res['participations']:
                    # ИСПРАВЛЕНИЕ: Выделяем бота в отдельный отдел (с правильными отступами!)
                    if op_id == 310507:
                        op_name = "🤖 Бот AI"
                        dept = "Бот AI"
                    else:
                        op_name = OPERATORS_MAP.get(op_id, f"ID {op_id}")
                        dept = find_department_smart(op_name)
                        if dept in CUSTOM_GROUPING: dept = CUSTOM_GROUPING[dept]
                        
                    if dept == "Тренер": continue
                    
                    hours = res.get('op_hours', {}).get(op_id, set())
                    if not hours:
                        final_rows.append({
                            'req_id': res['req_id'],
                            'operator_id': op_id,
                            'Оператор': op_name,
                            'Отдел': dept,
                            'rating': res['rating'],
                            'Дата': None,
                            'Час': -1
                        })
                    else:
                        for d, h in hours: 
                            final_rows.append({
                                'req_id': res['req_id'],
                                'operator_id': op_id,
                                'Оператор': op_name,
                                'Отдел': dept,
                                'rating': res['rating'],
                                'Дата': d, 
                                'Час': h
                            })
            
            completed += 1
            if total > 0: 
                current_prog = 0.5 + (completed / total * 0.5)
                progress_bar.progress(min(current_prog, 1.0))
                status_text.text(f"Анализ диалогов: {completed}/{total}")
            
    progress_bar.empty(); status_text.empty()
    
    df = pd.DataFrame(final_rows)
    return df, all_speeds, all_first_speeds

def get_dynamics_stats(df, start_date, end_date):
    """Возвращает агрегированные данные: объем и % закрытия ботом"""
    mask = (df['Дата'].dt.date >= start_date) & (df['Дата'].dt.date <= end_date)
    period_df = df[mask].copy()
    
    if period_df.empty:
        return pd.DataFrame(columns=['Всего', 'Бот_%'])
    
    stats = period_df.groupby('Тип обращения').agg(
        Всего=('Дата', 'count'),
        Закрыто_ботом=('Статус', lambda x: (x == 'Закрыл').sum())
    )
    stats['Бот_%'] = (stats['Закрыто_ботом'] / stats['Всего'] * 100)
    return stats[['Всего', 'Бот_%']]

# ==========================================
# 4. GOOGLE SHEET
# ==========================================
@st.cache_data(ttl=600)
def load_gsheet_data():
    try:
        df = pd.read_csv(SHEET_URL)
        df['Дата'] = pd.to_datetime(df['Дата'], dayfirst=True, errors='coerce')
        df = df.dropna(subset=['Дата'])
        
        # ДОБАВЛЕНО: Убеждаемся, что колонка Продукт существует (защита от ошибок)
        if 'Продукт' not in df.columns:
            df['Продукт'] = '-'

        # ДОБАВЛЕНО: Чистим колонку Продукт вместе с остальными
        for col in ['Отдел', 'Статус', 'Тип обращения', 'Продукт']:
            if col in df.columns: 
                df[col] = df[col].astype(str).str.strip().replace(['nan', ''], '-')
        
        def fix_topic(row):
            topic = row['Тип обращения']
            dept = row['Отдел']
            if topic == '-' or topic == '' or topic == 'nan':
                return f"Прямая маршрутизация {dept}"
            return topic

        df['Тип обращения'] = df.apply(fix_topic, axis=1)
        
        df['Час'] = df['Дата'].dt.hour
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки Google Sheet: {e}"); return pd.DataFrame()

# ==========================================
# 5. ИНТЕРФЕЙС
# ==========================================
st.sidebar.title("Фильтры")

# 1. Загружаем все данные из GSheet
df_gsheet_all = load_gsheet_data()

# --- БЛОК БЕЗОПАСНЫХ ДАТ (Чтобы не было StreamlitAPIException) ---
today = datetime.now().date()

if not df_gsheet_all.empty:
    sheet_min = df_gsheet_all['Дата'].min().date()
    sheet_max = df_gsheet_all['Дата'].max().date()
else:
    sheet_min = today
    sheet_max = today

# Трюк: разрешаем календарю видеть +1 день от сегодня, 
# чтобы "утренние" данные из таблицы не конфликтовали с UTC временем сервера
absolute_max = max(today, sheet_max) + timedelta(days=1)
absolute_min = min(today, sheet_min)

# По умолчанию ставим последнюю дату из таблицы, но не выходя за границы
default_val = min(sheet_max, absolute_max)

date_range = st.sidebar.date_input(
    "Диапазон дат",
    value=(default_val, default_val),
    min_value=absolute_min,
    max_value=absolute_max
)
# -----------------------------------------------------------------

# Разбор выбранного диапазона
if isinstance(date_range, tuple) and len(date_range) == 2:
    sel_start, sel_end = date_range
elif isinstance(date_range, tuple) and len(date_range) == 1:
    sel_start = sel_end = date_range[0]
else:
    sel_start = sel_end = date_range

st.sidebar.caption(f"Выбрано: {sel_start} — {sel_end}")

# Кнопка запуска
if st.sidebar.button("Запустить анализ (API)"):
    st.session_state['run_analysis'] = True
    st.cache_data.clear()

# Если анализ еще не запускали — стопаем выполнение дальше
if 'run_analysis' not in st.session_state:
    st.info("👈 Выберите даты и нажмите 'Запустить анализ'"); st.stop()


# --- ТУТ НАЧИНАЕТСЯ ТВОЯ ЛОГИКА ГРАФИКОВ И KPI ---

# ЗАГРУЗКА ДАННЫХ ЧЕРЕЗ API
df_api, speeds_map, first_speeds_map = load_api_data_range(sel_start, sel_end)

# --- ДОБАВЛЕНО: Грузим прошлый период для динамики в отчетах ---
period_days = (sel_end - sel_start).days + 1
prev_end = sel_start - timedelta(days=1)
prev_start = prev_end - timedelta(days=period_days - 1)
df_api_prev, speeds_map_prev, first_speeds_map_prev = load_api_data_range(prev_start, prev_end)
# -----------------------------------------------------------------

# Фильтруем данные из таблицы под выбранные даты
mask_gsheet = (df_gsheet_all['Дата'].dt.date >= sel_start) & (df_gsheet_all['Дата'].dt.date <= sel_end)
df_gsheet = df_gsheet_all[mask_gsheet].copy()

# Расчет метрик KPI
if not df_api.empty: 
    count_human_chats = df_api['req_id'].nunique()
else: 
    count_human_chats = 0

bot_closed_mask = (df_gsheet['Статус'].str.lower() == 'закрыл')
count_bot_closed = len(df_gsheet[bot_closed_mask])

auth_mask = (df_gsheet['Тип обращения'].str.contains('Авторизация пройдена', case=False, na=False))
count_auth = len(df_gsheet[auth_mask])

total_chats_day = count_human_chats + count_bot_closed + count_auth

# --- ВЫВОД ТАБОВ ---
tabs = st.tabs(["KPI", "Нагрузка", "Анализ отдела", "Категории", "📈 Динамика", "База данных"])

# Дальше идут твои блоки with tabs[0], with tabs[1] и т.д.
# (Они остаются без изменений, как в твоем исходном коде)

# TAB 1: KPI
with tabs[0]:
    st.subheader("Сводная статистика")
    
    # --- 1. РАСЧЕТ МЕТРИК ---
    # Автоматика (общая)
    mask_bot_closed = (df_gsheet['Статус'] == 'Закрыл')
    mask_auth_success = df_gsheet['Тип обращения'].str.contains('Авторизация пройдена', case=False, na=False)
    mask_stub = (df_gsheet['Тип обращения'] == 'Заглушка на старый чат')
    
    count_bot_closed = len(df_gsheet[mask_bot_closed])
    count_auth_success = len(df_gsheet[mask_auth_success])
    count_stub = len(df_gsheet[mask_stub])
    
    total_automation = count_bot_closed + count_auth_success + count_stub

    # Участие человека (для фильтрации, если нужно в других табах)
    mask_confirm = df_gsheet['Статус'].isin(['Ручник: Позовите человека', 'Ручник: Обзвон и отмены'])
    mask_courier = (df_gsheet['Статус'] == 'Меню курьеров')
    mask_auth_fail = df_gsheet['Тип обращения'].str.startswith('Авторизация не пройдена', na=False)
    
    count_confirm = len(df_gsheet[mask_confirm])
    count_courier = len(df_gsheet[mask_courier])
    count_auth_fail = len(df_gsheet[mask_auth_fail])

    # Бот (участие и эффективность)
    bot_participated_df = df_gsheet[df_gsheet['Статус'].isin(['Закрыл', 'Перевод'])]
    participated_count = len(bot_participated_df)
    transferred_count = participated_count - count_bot_closed
    
    # Расчет общего итога
    pure_human_chats = max(0, count_human_chats - transferred_count)
    total_chats_all = count_human_chats + count_bot_closed + count_auth_success + count_stub
    
    # --- 2. KPI ПАНЕЛЬ (БЕЗ ДЕЛЬТЫ И ЛИШНИХ ПОЛЕЙ) ---
    cols = st.columns(6)
    cols[0].metric("Всего чатов", total_chats_all)
    cols[1].metric("Автоматика", total_automation, help="Бот закрыл + Авториз. ОК + Заглушки")
    cols[2].metric("Участие бота", participated_count)
    cols[3].metric("Бот (Закрыл)", count_bot_closed) # УДАЛЕНА ПРОЦЕНТОВКА
    cols[4].metric("Люди (Всего)", count_human_chats)
    cols[5].metric("Заглушка", count_stub)
    
    st.divider()
    
    # --- 3. ГРАФИКИ ---
    col_pies = st.columns(2)
    
    with col_pies[0]:
        st.subheader("Распределение нагрузки")
        if total_chats_all > 0:
            labels = ['Бот (Закрыл)', 'Бот (Перевел)', 'Люди (Без бота)', 'Заглушка', 'Авторизация']
            sizes = [count_bot_closed, transferred_count, pure_human_chats, count_stub, count_auth_success]
            colors = ['#ff9999', '#ffcc99', '#66b3ff', '#d3d3d3', '#99ff99'] 
            
            fig1, ax1 = plt.subplots(figsize=(5, 5))
            ax1.pie(
                sizes, 
                labels=labels, 
                autopct='%1.1f%%', 
                colors=colors, 
                startangle=90,
                pctdistance=0.85,
                explode=[0.05 if i == 0 else 0 for i in range(len(labels))]
            )
            
            # Donut-эффект
            centre_circle = plt.Circle((0,0), 0.70, fc='white')
            fig1.gca().add_artist(centre_circle)
            
            plt.tight_layout()
            st.pyplot(fig1, use_container_width=False)
            # ТЕКСТОВЫЕ ИНФО-ПАНЕЛИ УДАЛЕНЫ

    with col_pies[1]:
        # Визуальный отступ вниз
        st.write("##") 
        st.write("##")
        st.subheader("Эффективность бота")
        
        if participated_count > 0:
            st.caption(f"Из {participated_count} диалогов, где был бот:")
            fig2, ax2 = plt.subplots(figsize=(4, 4))
            ax2.pie([count_bot_closed, transferred_count], 
                    labels=['Справился', 'Перевел'], 
                    autopct='%1.1f%%', colors=['#ff9999', '#ffcc99'], startangle=90)
            
            centre_circle2 = plt.Circle((0,0), 0.70, fc='white')
            fig2.gca().add_artist(centre_circle2)
            
            st.pyplot(fig2, use_container_width=False)
            # ТЕКСТОВЫЙ БЛОК "РУЧНОЕ УЧАСТИЕ" УДАЛЕН
        else:
            st.write("Бот не участвовал в диалогах за выбранный период.")

# TAB 2: LOAD
with tabs[1]:
    st.subheader("Нагрузка по отделам (Данные скрипта)")
    if not df_api.empty:
        dept_load = df_api.groupby('Отдел')['req_id'].nunique().sort_values(ascending=False).reset_index()
        dept_load.columns = ['Отдел', 'Кол-во чатов']
        c_table, c_heat = st.columns([1, 2])
        with c_table: st.dataframe(dept_load, hide_index=True, use_container_width=True)
        with c_heat:
            st.write("**Тепловая карта: Отдел vs Час (Данные API)**")
            
            hm_df = df_api[df_api['Час'].between(0, 23)]
            
            if not hm_df.empty:
                hm_data = hm_df.groupby(['Отдел', 'Час'])['req_id'].nunique().unstack(fill_value=0)
                hm_data = hm_data.reindex(columns=range(24), fill_value=0)
                hm_data['Total'] = hm_data.sum(axis=1)
                hm_data = hm_data.sort_values('Total', ascending=False).drop(columns='Total')

                fig_hm, ax_hm = plt.subplots(figsize=(10, len(hm_data)*0.5+2))
                sns.heatmap(hm_data, annot=True, fmt="d", cmap="YlOrRd", cbar=False, ax=ax_hm)
                ax_hm.set_xlabel("Час дня")
                st.pyplot(fig_hm)
            else:
                st.warning("Нет данных по часам в API.")

    st.divider()
    st.subheader("Тематика обращений по времени")
    # Убираем только Авторизацию, "-" уже переименован
    topics_df = df_gsheet[~df_gsheet['Тип обращения'].str.contains('Авторизация', na=False)].copy()
    
    if not topics_df.empty:
        top_topics = topics_df['Тип обращения'].value_counts().nlargest(15).index
        topics_df_top = topics_df[topics_df['Тип обращения'].isin(top_topics)]
        hm_topic = topics_df_top.groupby(['Тип обращения', 'Час']).size().unstack(fill_value=0)
        hm_topic = hm_topic.reindex(columns=range(24), fill_value=0)
        hm_topic['Total'] = hm_topic.sum(axis=1)
        hm_topic = hm_topic.sort_values('Total', ascending=False).drop(columns='Total')
        
        fig2, ax2 = plt.subplots(figsize=(12, len(hm_topic)*0.6+2))
        sns.heatmap(hm_topic, annot=True, fmt="d", cmap="Blues", cbar=False, ax=ax2)
        st.pyplot(fig2)

# ==========================================
# TAB 3: DEPT ANALYSIS (С ИСПРАВЛЕННЫМ CSAT БОТА)
# ==========================================
with tabs[2]:
    st.subheader("Детальный анализ по отделу")
    if not df_api.empty:
        all_depts = sorted(df_api['Отдел'].unique())
        selected_dept = st.selectbox("Выберите отдел", all_depts, key="dept_analysis_v12")
        
        if selected_dept:
            # Базово берем данные API
            dept_data = df_api[df_api['Отдел'] == selected_dept].copy()
            
            # Защита: загружаем прошлый период, если он есть
            if 'df_api_prev' in globals() and not df_api_prev.empty:
                dept_data_prev = df_api_prev[df_api_prev['Отдел'] == selected_dept].copy()
                sm_prev, fsm_prev = speeds_map_prev, first_speeds_map_prev
            else:
                dept_data_prev, sm_prev, fsm_prev = pd.DataFrame(), {}, {}
            
            # Логика Тимлидов
            TL_ROOTS = ["черныш", "гетман", "власенков"]
            dept_data['is_tl'] = dept_data['Оператор'].apply(
                lambda x: any(normalize_text(tl) in normalize_text(x) for tl in TL_ROOTS)
            )
            if not dept_data_prev.empty:
                dept_data_prev['is_tl'] = dept_data_prev['Оператор'].apply(
                    lambda x: any(normalize_text(tl) in normalize_text(x) for tl in TL_ROOTS)
                )

            # --- ГЕНЕРАЦИЯ БАЗОВОГО ОТЧЕТА ПО API ---
            def calc_metrics_for_report(df, sm, fsm, dept_name):
                if df.empty and dept_name != "Бот AI": return None
                chats = df['req_id'].nunique() if not df.empty else 0
                ratings = pd.to_numeric(df.drop_duplicates('req_id')['rating'], errors='coerce').dropna() if not df.empty else pd.Series(dtype=float)
                
                d_speeds, d_first = [], []
                if not df.empty:
                    for o_id in df['operator_id'].unique():
                        if o_id in sm: d_speeds.extend(sm[o_id])
                        if o_id in fsm: d_first.extend(fsm[o_id])
                    
                avg_s = np.median(d_speeds) if d_speeds else 0
                avg_fs = np.median(d_first) if d_first else 0
                
                daily_c = df.groupby('Дата')['req_id'].nunique() if not df.empty else pd.Series(dtype=float)
                if dept_name == "Бот AI":
                    daily_o = pd.Series(1, index=daily_c.index) if not daily_c.empty else pd.Series(dtype=float)
                else:
                    daily_o = df[~df['is_tl']].groupby('Дата')['operator_id'].nunique() if not df.empty else pd.Series(dtype=float)
                    
                daily_stats = pd.DataFrame({'c': daily_c, 'o': daily_o}).fillna(0)
                avg_specs = daily_stats['o'].mean() if not daily_stats.empty else 0
                daily_stats['load'] = daily_stats.apply(lambda r: r['c']/r['o'] if r['o']>0 else r['c'], axis=1)
                avg_load = daily_stats['load'].mean() if not daily_stats.empty else 0
                
                return {
                    'chats': chats, 'ratings': len(ratings), 'csat': ratings.mean() if len(ratings)>0 else 0,
                    'specs': round(avg_specs) if dept_name != "Бот AI" else 1, 
                    'load': round(avg_load),
                    'speed': avg_s, 'first_speed': avg_fs
                }

            curr_m = calc_metrics_for_report(dept_data, speeds_map, first_speeds_map, selected_dept)
            prev_m = calc_metrics_for_report(dept_data_prev, sm_prev, fsm_prev, selected_dept)

            # --- ИЗОЛИРОВАННАЯ ЛОГИКА ДЛЯ БОТА (ЧЕСТНЫЙ CSAT) ---
            if selected_dept == "Бот AI":
                # Отфильтровываем участия бота из таблицы (Закрыл + Перевел)
                dept_gsheet = df_gsheet[df_gsheet['Статус'].isin(['Закрыл', 'Перевод'])].copy()
                d_chats_api = len(dept_gsheet) 
                
                # То же самое для прошлого периода (для динамики)
                mask_gsheet_prev = (df_gsheet_all['Дата'].dt.date >= prev_start) & (df_gsheet_all['Дата'].dt.date <= prev_end)
                df_gsheet_prev = df_gsheet_all[mask_gsheet_prev]
                dept_gsheet_prev = df_gsheet_prev[df_gsheet_prev['Статус'].isin(['Закрыл', 'Перевод'])].copy()
                
                # ---> ФИЛЬТРУЕМ CSAT: БЕРЕМ ОЦЕНКИ ТОЛЬКО ТАМ, ГДЕ БОТ "ЗАКРЫЛ" <---
                if 'ID обращения' in df_gsheet.columns:
                    # Чистим ID от возможных нулей на конце (если pandas прочитал их как float)
                    closed_ids_curr = dept_gsheet[dept_gsheet['Статус'] == 'Закрыл']['ID обращения'].astype(str).str.replace(r'\.0$', '', regex=True).tolist()
                    bot_ratings_curr = dept_data[dept_data['req_id'].astype(str).str.replace(r'\.0$', '', regex=True).isin(closed_ids_curr)].drop_duplicates('req_id')['rating']
                    bot_ratings_curr = pd.to_numeric(bot_ratings_curr, errors='coerce').dropna()
                else:
                    bot_ratings_curr = pd.Series(dtype=float)
                    
                if 'ID обращения' in df_gsheet_prev.columns:
                    closed_ids_prev = dept_gsheet_prev[dept_gsheet_prev['Статус'] == 'Закрыл']['ID обращения'].astype(str).str.replace(r'\.0$', '', regex=True).tolist()
                    bot_ratings_prev = dept_data_prev[dept_data_prev['req_id'].astype(str).str.replace(r'\.0$', '', regex=True).isin(closed_ids_prev)].drop_duplicates('req_id')['rating']
                    bot_ratings_prev = pd.to_numeric(bot_ratings_prev, errors='coerce').dropna()
                else:
                    bot_ratings_prev = pd.Series(dtype=float)

                # Подменяем объемы и честный CSAT в текущем отчете
                if curr_m:
                    curr_m['chats'] = len(dept_gsheet)
                    daily_c = dept_gsheet.groupby(dept_gsheet['Дата'].dt.date).size()
                    curr_m['load'] = round(daily_c.mean()) if not daily_c.empty else 0
                    curr_m['ratings'] = len(bot_ratings_curr)
                    curr_m['csat'] = bot_ratings_curr.mean() if len(bot_ratings_curr) > 0 else 0
                    
                # Подменяем объемы и честный CSAT в прошлом отчете
                if prev_m:
                    prev_m['chats'] = len(dept_gsheet_prev)
                    daily_c_p = dept_gsheet_prev.groupby(dept_gsheet_prev['Дата'].dt.date).size()
                    prev_m['load'] = round(daily_c_p.mean()) if not daily_c_p.empty else 0
                    prev_m['ratings'] = len(bot_ratings_prev)
                    prev_m['csat'] = bot_ratings_prev.mean() if len(bot_ratings_prev) > 0 else 0
            else:
                # Если это живые люди (SMM и т.д.), работаем по классике
                d_chats_api = dept_data['req_id'].nunique()
                dept_gsheet = df_gsheet[df_gsheet['Отдел'] == selected_dept].copy()

            # --- ВЫВОД МИКРО-ОТЧЕТА ---
            if curr_m:
                def fmt_trend(c_val, p_val, is_time=False, is_float=False):
                    if not prev_m or p_val == 0: return ""
                    if is_time: return f" (пред: {format_seconds(p_val)})"
                    diff = c_val - p_val
                    pct = (diff / p_val) * 100
                    sign = "+" if diff > 0 else ""
                    if is_float: return f" (пред: {p_val:.2f}, {sign}{pct:.1f}%)"
                    return f" (пред: {int(p_val)}, {sign}{pct:.1f}%)"

                report_text = f"""**{sel_start.strftime('%d.%m')} - {sel_end.strftime('%d.%m')}** **{selected_dept}**
Всего чатов: {curr_m['chats']}{fmt_trend(curr_m['chats'], prev_m['chats'] if prev_m else 0)}  
Всего оценок: {curr_m['ratings']}{fmt_trend(curr_m['ratings'], prev_m['ratings'] if prev_m else 0)}  
Кол-во специалистов в смену: {curr_m['specs']}{fmt_trend(curr_m['specs'], prev_m['specs'] if prev_m else 0)}  
Среднее кол-во чатов на специалиста: {curr_m['load']}{fmt_trend(curr_m['load'], prev_m['load'] if prev_m else 0)}  
Средний CSAT: {curr_m['csat']:.2f}{fmt_trend(curr_m['csat'], prev_m['csat'] if prev_m else 0, is_float=True)}  
Средняя 1-я скорость: {format_seconds(curr_m['first_speed'])}{fmt_trend(curr_m['first_speed'], prev_m['first_speed'] if prev_m else 0, is_time=True)}  
Средняя скорость: {format_seconds(curr_m['speed'])}{fmt_trend(curr_m['speed'], prev_m['speed'] if prev_m else 0, is_time=True)}"""

                st.info(report_text)

            # --- ИНФО-ПАНЕЛЬ: ЕСЛИ ВЫБРАН БОТ ---
            if selected_dept == "Бот AI":
                st.divider()
                st.write("#### 🤖 Конверсия бота")
                b_participated = len(dept_gsheet)
                b_closed = len(dept_gsheet[dept_gsheet['Статус'] == 'Закрыл'])
                b_transferred = b_participated - b_closed
                
                cb1, cb2, cb3 = st.columns(3)
                cb1.metric("Участие (Совпадает с KPI)", b_participated)
                cb2.metric("Справился (Закрыл)", b_closed, f"{b_closed/max(1, b_participated)*100:.1f}%" if b_participated > 0 else "0%")
                cb3.metric("Перевел на человека", b_transferred, f"-{b_transferred/max(1, b_participated)*100:.1f}%" if b_participated > 0 else "0%")
                st.caption("ℹ️ **Важно:** CSAT бота теперь считается *только* по диалогам, которые бот закрыл самостоятельно, чтобы исключить влияние операторов на оценку.")

            # --- ПОСУТОЧНАЯ НАГРУЗКА ---
            if selected_dept == "Бот AI":
                daily_chats = dept_gsheet.groupby(dept_gsheet['Дата'].dt.date).size()
                daily_ops = pd.Series(1, index=daily_chats.index)
            elif 'Дата' in dept_data.columns:
                daily_chats = dept_data.groupby('Дата')['req_id'].nunique()
                daily_ops = dept_data[~dept_data['is_tl']].groupby('Дата')['operator_id'].nunique()
            else:
                daily_chats, daily_ops = pd.Series(dtype=float), pd.Series(dtype=float)
                
            if not daily_chats.empty:
                daily_stats = pd.DataFrame({'Чатов': daily_chats, 'Спецов': daily_ops}).reset_index().fillna(0)
                daily_stats.rename(columns={'index': 'Дата', 'Дата': 'Дата'}, inplace=True)
                if 'Дата' not in daily_stats.columns: daily_stats['Дата'] = daily_stats.index
                daily_stats['Нагрузка'] = daily_stats.apply(lambda r: round(r['Чатов'] / r['Спецов'], 1) if r['Спецов'] > 0 else r['Чатов'], axis=1)
                
                st.write("#### Посуточная нагрузка отдела")
                st.dataframe(daily_stats.sort_values('Дата', ascending=False), use_container_width=True, hide_index=True)

            st.divider()

            # --- СТАТИСТИКА СПЕЦИАЛИСТОВ (СКРЫВАЕМ ДЛЯ БОТА) ---
            if selected_dept != "Бот AI":
                st.write("#### Статистика специалистов")
                op_list = dept_data.groupby(['operator_id', 'Оператор', 'is_tl']).agg(chats=('req_id', 'nunique')).reset_index().sort_values('chats', ascending=False)
                
                spec_rows = []
                for _, row in op_list.iterrows():
                    op_id = row['operator_id']
                    s_first_med = np.median(first_speeds_map.get(op_id, [])) if first_speeds_map.get(op_id) else None
                    s_avg = np.median(speeds_map.get(op_id, [])) if speeds_map.get(op_id) else None
                    op_ratings = pd.to_numeric(dept_data[dept_data['operator_id'] == op_id]['rating'], errors='coerce').dropna()
                    
                    spec_rows.append({
                        "Роль": "🤖 Автоматика" if op_id == 310507 else ("⭐ Team Lead" if row['is_tl'] else "Специалист"),
                        "Специалист": f"⭐ {row['Оператор'].upper()}" if row['is_tl'] else row['Оператор'],
                        "Чаты": row['chats'],
                        "1-я скор.": format_seconds(s_first_med),
                        "Ср. скор.": format_seconds(s_avg),
                        "Рейтинг": f"{op_ratings.mean():.2f}" if not op_ratings.empty else "-",
                        "Оценок": len(op_ratings)
                    })
                
                st.dataframe(
                    pd.DataFrame(spec_rows).style.apply(lambda r: ['background-color: #e3f2fd; font-weight: bold']*len(r) if "Team Lead" in r['Роль'] else ['']*len(r), axis=1),
                    use_container_width=True, hide_index=True
                )
                st.divider()

            # --- ТЕМАТИКИ С РАСЧЕТОМ РАЗНИЦЫ ---
            st.subheader("Тематика обращений (GSheet)")
            
            def get_universal_label(row):
                st_val = str(row.get('Статус', '-')).strip()
                tp_val = str(row.get('Тип обращения', '-')).strip()
                standard_statuses = ['закрыл', 'перевод', '-', 'none', 'nan']
                if st_val.lower() not in standard_statuses:
                    return f"{st_val} ({tp_val})"
                return tp_val

            if not dept_gsheet.empty:
                dept_gsheet['Категория_Финальная'] = dept_gsheet.apply(get_universal_label, axis=1)
                cat_counts = dept_gsheet['Категория_Финальная'].value_counts().reset_index()
                cat_counts.columns = ['Категория', 'Кол-во']
                
                total_sheet_found = len(dept_gsheet)
                
                # Для бота разница не считается (все данные и так из таблицы)
                if selected_dept == "Бот AI":
                    unknown_gap = 0
                else:
                    unknown_gap = max(0, d_chats_api - total_sheet_found)
                
                if unknown_gap > 0:
                    gap_row = pd.DataFrame([{'Категория': 'Разница (API > Sheet)', 'Кол-во': unknown_gap}])
                    cat_counts = pd.concat([cat_counts, gap_row], ignore_index=True)
                
                cat_counts = cat_counts.sort_values('Кол-во', ascending=False)
                cat_counts['Доля'] = (cat_counts['Кол-во'] / max(1, d_chats_api) * 100).map('{:.1f}%'.format)
                
                st.dataframe(cat_counts, use_container_width=True, hide_index=True)
            else:
                st.warning(f"В таблице GSheet нет данных для {selected_dept}. Разница: {d_chats_api}")
# ==========================================
# TAB 4: КАТЕГОРИИ (ДЕТАЛЬНАЯ АНАЛИТИКА)
# ==========================================
with tabs[3]:
    st.subheader("📊 Анализ типов обращений")
    
    # 3 вкладки, включая новую под продукты
    sub_tab1, sub_tab2, sub_tab3 = st.tabs(["📋 Полная детализация", "📈 Интерактивный ТОП-15", "📦 Отчет по продуктам"])

    if not df_gsheet.empty:
        import re 
        
        # --- 1. ПОДГОТОВКА ДАННЫХ ---
        def group_result_detailed(row):
            if row['Статус'] == 'Закрыл': return 'Бот справился'
            elif row['Статус'] == 'Перевод':
                reason = str(row.get('Причина перевода', 'Другое'))
                if reason in ['Требует сценарий', 'Не знает ответ', 'Лимит сообщений']: return f"Перевод: {reason}"
                return "Перевод: Прочее"
            return "Без статуса"

        # Функция для вытаскивания типа юзера из скобок
        def extract_user_type(topic):
            match = re.search(r'\(([^)]+)\)[^(]*$', str(topic))
            return match.group(1).strip() if match else 'Не определен'

        df_analysis = df_gsheet.copy()
        df_analysis['Результат'] = df_analysis.apply(group_result_detailed, axis=1)
        df_analysis['Тип юзера'] = df_analysis['Тип обращения'].apply(extract_user_type)

        # --- SUB-TAB 1: ПОЛНАЯ ТАБЛИЦА ---
        with sub_tab1:
            st.write("#### Полная статистика по всем категориям")
            stats = df_analysis.groupby(['Тип обращения', 'Статус']).size().unstack(fill_value=0)
            stats['Всего'] = stats.sum(axis=1)
            for c in ['Закрыл', 'Перевод']: 
                if c not in stats.columns: stats[c] = 0
                
            stats['Бот(✓)'] = (stats['Закрыл'] / stats['Всего'] * 100).map('{:.1f}%'.format)
            stats['Бот(→)'] = (stats['Перевод'] / stats['Всего'] * 100).map('{:.1f}%'.format)

            def get_cat_details(row):
                transferred = row.get('Перевод', 0)
                if transferred == 0: return "—"
                cat_reasons = df_analysis[(df_analysis['Тип обращения'] == row.name) & (df_analysis['Статус'] == 'Перевод')]
                r_counts = cat_reasons['Причина перевода'].value_counts()
                return "\n".join([f"• {r}: {(count/transferred*100):.0f}%" for r, count in r_counts.items() if count > 0])

            stats['Детализация перевода'] = stats.apply(get_cat_details, axis=1)
            final_table = stats[['Всего', 'Бот(✓)', 'Бот(→)', 'Детализация перевода']].sort_values('Всего', ascending=False).reset_index()
            st.dataframe(final_table, use_container_width=True, hide_index=True)

        # --- SUB-TAB 2: ГРАФИК ТОП-15 ---
        with sub_tab2:
            st.write("#### Топ-15 обращений в разрезе эффективности")
            top_names = df_gsheet['Тип обращения'].value_counts().nlargest(15).index
            df_plot = df_analysis[df_analysis['Тип обращения'].isin(top_names)]
            plot_data = df_plot.groupby(['Тип обращения', 'Результат']).size().reset_index(name='Количество')
            
            color_map = {
                'Бот справился': '#26A69A', 'Перевод: Не знает ответ': '#FF5252',
                'Перевод: Требует сценарий': '#FFAB40', 'Перевод: Лимит сообщений': '#7C4DFF',
                'Перевод: Прочее': '#90A4AE', 'Без статуса': '#CFD8DC'
            }
            fig = px.bar(plot_data, x="Количество", y="Тип обращения", color="Результат", orientation='h',
                         color_discrete_map=color_map, text_auto=True, category_orders={"Тип обращения": top_names.tolist()})
            fig.update_layout(barmode='stack', height=700, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), hovermode="y unified")
            fig.update_yaxes(title="")
            fig.update_xaxes(title="Количество диалогов")
            st.plotly_chart(fig, use_container_width=True)
            
        # --- SUB-TAB 3: ПРОДУКТЫ (НОВАЯ ЛОГИКА) ---
        with sub_tab3:
            # 1. Отсекаем все прочерки. Считаем только размеченные продукты.
            df_valid_prods = df_analysis[df_analysis['Продукт'] != '-'].copy()

            if not df_valid_prods.empty:
                total_valid_chats = len(df_valid_prods)
                
                c_head1, c_head2 = st.columns([2, 1])
                c_head1.write(f"### 🏢 Иерархия обращений")
                c_head2.metric("Учтено чатов с продуктом", total_valid_chats)

                # --- ЭТАЖИ (Текстовый отчет) ---
                for prod, prod_df in df_valid_prods.groupby('Продукт'):
                    prod_cnt = len(prod_df)
                    # Процент продукта от общей массы размеченных
                    prod_pct = (prod_cnt / total_valid_chats) * 100 
                    
                    with st.expander(f"📦 ПРОДУКТ: {prod} ({prod_pct:.1f}% | {prod_cnt} шт.)", expanded=True):
                        for usr, user_df in prod_df.groupby('Тип юзера'):
                            usr_cnt = len(user_df)
                            # Процент юзера внутри этого продукта
                            usr_pct = (usr_cnt / prod_cnt) * 100 
                            st.markdown(f"**👤 ЮЗЕР: {usr}** ({usr_pct:.1f}% | {usr_cnt} шт.)")

                            topic_counts = user_df['Тип обращения'].value_counts()
                            for top, top_cnt in topic_counts.items():
                                # Процент темы внутри этого юзера
                                top_pct = (top_cnt / usr_cnt) * 100 
                                st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;↳ 💬 *ТЕМА: {top}* ({top_pct:.1f}% | {top_cnt} шт.)")

                st.divider()

                # --- БЛОКИ (Визуализация как на схеме) ---
                st.write("### 🔲 Карта распределения (Кликабельно)")
                st.caption("Нажимай на блоки, чтобы провалиться вглубь продукта.")
                
                tree_df = df_valid_prods.groupby(['Продукт', 'Тип юзера', 'Тип обращения']).size().reset_index(name='Количество')
                fig_tree = px.treemap(
                    tree_df,
                    path=[px.Constant("Все продукты"), 'Продукт', 'Тип юзера', 'Тип обращения'],
                    values='Количество',
                    color='Продукт',
                    color_discrete_sequence=px.colors.qualitative.Pastel
                )
                fig_tree.update_traces(textinfo="label+value+percent parent")
                fig_tree.update_layout(margin=dict(t=10, l=10, r=10, b=10), height=500)
                st.plotly_chart(fig_tree, use_container_width=True)

                st.divider()

                # --- ОЧИЩЕННАЯ ТАБЛИЦА ---
                st.write("### 📊 Детализация конверсии бота по продуктам")
                
                # Добавили Тип обращения, чтобы таблица была максимально подробной
                prod_stats = df_valid_prods.groupby(['Продукт', 'Тип юзера', 'Тип обращения', 'Статус']).size().unstack(fill_value=0)
                prod_stats['Всего'] = prod_stats.sum(axis=1)
                
                for c in ['Закрыл', 'Перевод']: 
                    if c not in prod_stats.columns: prod_stats[c] = 0
                    
                prod_stats['Бот(✓)'] = (prod_stats['Закрыл'] / prod_stats['Всего'] * 100).map('{:.1f}%'.format)
                prod_stats['Бот(→)'] = (prod_stats['Перевод'] / prod_stats['Всего'] * 100).map('{:.1f}%'.format)

                def get_prod_details(row):
                    transferred = row.get('Перевод', 0)
                    if transferred == 0: return "—"
                    cat_reasons = df_valid_prods[(df_valid_prods['Продукт'] == row.name[0]) & 
                                              (df_valid_prods['Тип юзера'] == row.name[1]) & 
                                              (df_valid_prods['Тип обращения'] == row.name[2]) &
                                              (df_valid_prods['Статус'] == 'Перевод')]
                    r_counts = cat_reasons['Причина перевода'].value_counts()
                    return "\n".join([f"• {r}: {(count/transferred*100):.0f}%" for r, count in r_counts.items() if count > 0])

                prod_stats['Причины перевода'] = prod_stats.apply(get_prod_details, axis=1)
                
                final_prod_table = prod_stats[['Всего', 'Бот(✓)', 'Бот(→)', 'Причины перевода']].sort_values(['Продукт', 'Всего'], ascending=[True, False]).reset_index()
                st.dataframe(final_prod_table, use_container_width=True, hide_index=True)

            else:
                st.info("Нет обращений с размеченным 'Продуктом' за выбранный период (везде стоят прочерки).")
    else:
        st.info("Нет данных за выбранный период. Попробуйте изменить даты в фильтрах.")

# ==========================================
# TAB 5: ДИНАМИКА (ПЕРИОД Б -> ПЕРИОД А + ВИЗУАЛИЗАЦИЯ)
# ==========================================
with tabs[4]:
    st.subheader("📈 Сравнение динамики: Прошлое vs Настоящее")
    
    # 1. Легенда (Описание логики)
    with st.expander("ℹ️ Логика цветовой индикации", expanded=False):
        st.markdown("""
        | Метрика | Тренд | Цвет | Статус |
        | :--- | :--- | :--- | :--- |
        | **V (Volume)** | Рост (+) | 🔴 Red | Кол-во обращений выросло |
        | **V (Volume)** | Снижение (-) | 🟢 Green | Кол-во обращений упало |
        | **B (Bot)** | Рост (+) | 🟢 Green | Рост % закрытие чатов ботом |
        | **B (Bot)** | Снижение (-) | 🔴 Red | Падение % закрытие чатов ботом |
        """)

    # 2. Выбор периодов (Сначала ПРОШЛОЕ, потом ТЕКУЩЕЕ)
    st.write("#### 1. Настройте периоды для сравнения")
    col_past, col_curr = st.columns(2)
    
    today_dyn = datetime.now().date()
    
    with col_past:
        st.markdown("⏪ **Период Б (Прошлое)**")
        range_prev = st.date_input("Выберите прошлые даты", [today_dyn - timedelta(days=14), today_dyn - timedelta(days=8)], key="dyn_p_b")
        
    with col_curr:
        st.markdown("⏩ **Период А (Настоящее)**")
        range_curr = st.date_input("Выберите текущие даты", [today_dyn - timedelta(days=7), today_dyn], key="dyn_p_a")

    # Кнопка запуска
    if st.button("Просчитать динамику и визуализировать", use_container_width=True):
        if len(range_curr) == 2 and len(range_prev) == 2:
            p_s, p_e = range_prev
            c_s, c_e = range_curr
            
            # Расчет данных
            stats_p = get_dynamics_stats(df_gsheet_all, p_s, p_e)
            stats_c = get_dynamics_stats(df_gsheet_all, c_s, c_e)

            # Объединяем (Сортировка по текущему объему А)
            df_dyn = stats_c.join(stats_p, lsuffix='_curr', rsuffix='_prev', how='outer').fillna(0)
            df_dyn = df_dyn.sort_values('Всего_curr', ascending=False)
            
            # Функция подготовки данных для визуальной таблицы
            def prepare_visual_row(row):
                v_c, v_p = row['Всего_curr'], row['Всего_prev']
                b_c, b_p = row['Бот_%_curr'], row['Бот_%_prev']
                
                # Volume Change
                v_diff = ((v_c / v_p - 1) * 100) if v_p > 0 else (100.0 if v_c > 0 else 0.0)
                v_ico = "🔴" if v_diff > 0 else "🟢"
                
                # Bot Change
                b_diff = b_c - b_p
                b_ico = "🟢" if b_diff > 0 else ("🔴" if b_diff < 0 else "⚪")
                
                return pd.Series([
                    int(v_p), # Было чатов
                    int(v_c), # Стало чатов
                    f"{v_ico} {v_diff:+.1f}%", # Тренд V
                    v_diff, # Для полоски V
                    f"{b_p:.1f}% → {b_c:.1f}%", # Путь бота
                    f"{b_ico} {b_diff:+.1f}пп" # Тренд B
                ])

            if not df_dyn.empty:
                res_tab = df_dyn.apply(prepare_visual_row, axis=1)
                res_tab.columns = ['Было (Б)', 'Стало (А)', 'Изменение V', 'Шкала V', 'Эфф. бота (Б→А)', 'Тренд B']
                
                # --- ВИЗУАЛЬНОЕ ОТОБРАЖЕНИЕ ---
                st.write("#### 2. Анализ изменений")
                
                # Используем column_config для добавления полосок
                st.dataframe(
                    res_tab,
                    use_container_width=True,
                    height=600,
                    column_config={
                        "Шкала V": st.column_config.BarChartColumn(
                            "Визуальный рост V",
                            help="Красные полоски показывают относительный рост нагрузки",
                            y_min=-100, y_max=100
                        ),
                        "Было (Б)": st.column_config.NumberColumn(format="%d 🗨️"),
                        "Стало (А)": st.column_config.NumberColumn(format="%d 🗨️")
                    }
                )
                
                # Краткий итог
                t_v_c = df_dyn['Всего_curr'].sum()
                t_v_p = df_dyn['Всего_prev'].sum()
                t_diff = ((t_v_c / t_v_p - 1) * 100) if t_v_p > 0 else 0
                st.metric("Общее изменение входящего потока", f"{int(t_v_c)} чатов", f"{t_diff:+.1f}%", delta_color="inverse")
            else:
                st.warning("Нет данных в выбранных диапазонах.")
        else:
            st.error("Выберите полные диапазоны дат (начало и конец).")
# ==========================================
# TAB 6: БАЗА ДАННЫХ
# ==========================================
with tabs[5]: # 
    st.subheader("🗄️ База данных")
    if not df_gsheet.empty:
        st.write(f"Отображено записей: {len(df_gsheet)}")
        st.dataframe(df_gsheet, use_container_width=True)
    else:
        st.info("Нет данных для отображения за выбранный период.")