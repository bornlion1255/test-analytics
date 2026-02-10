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
    "Алина Новикова": "SMM",
    "Иван Савицкий": "SMM",
    "Анастасия Ванян": "SALE",
    "Павел Новиков": "SMM",
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
            'op_hours': {}, # Сюда будем сохранять кортежи (Дата, Час)
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
            
            elif msg_type == 'out' and op_id and op_id != 0 and op_id != 310507:
                 if target_start <= dt_local <= target_end:
                      stats['participations'].add(op_id)
                      
                      if op_id not in stats['op_hours']: stats['op_hours'][op_id] = set()
                      # ИСПРАВЛЕНО: Сохраняем и дату, и час для построения истории по дням
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
                        # ИСПРАВЛЕНО: Распаковываем кортеж (Дата, Час)
                        for d, h in hours: 
                            final_rows.append({
                                'req_id': res['req_id'],
                                'operator_id': op_id,
                                'Оператор': op_name,
                                'Отдел': dept,
                                'rating': res['rating'],
                                'Дата': d, # Теперь у каждой записи есть дата
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
        for col in ['Отдел', 'Статус', 'Тип обращения']:
            if col in df.columns: 
                df[col] = df[col].astype(str).str.strip().replace(['nan', ''], '-')
        
        # !!!!!!! ИСПРАВЛЕНИЕ ЗДЕСЬ !!!!!!!
        # Если Тип обращения "-" или пустой, то берем Отдел и пишем "Прямая маршрутизация [Отдел]"
        def fix_topic(row):
            topic = row['Тип обращения']
            dept = row['Отдел']
            if topic == '-' or topic == '' or topic == 'nan':
                return f"Прямая маршрутизация {dept}"
            return topic

        df['Тип обращения'] = df.apply(fix_topic, axis=1)
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        
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
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Всего чатов", total_chats_day)
    c2.metric("Люди (API)", count_human_chats)
    c3.metric("Бот (Закрыл)", count_bot_closed)
    c4.metric("Авторизация", count_auth)
    
    st.divider()
    col_pies = st.columns(2)
    with col_pies[0]:
        st.subheader("Распределение нагрузки")
        if total_chats_day > 0:
            fig1, ax1 = plt.subplots(figsize=(4, 4))
            ax1.pie([count_human_chats, count_bot_closed, count_auth], 
                    labels=['Люди', 'Бот (Закрыл)', 'Авторизация'], 
                    autopct='%1.1f%%', colors=['#66b3ff', '#ff9999', '#99ff99'], startangle=90)
            st.pyplot(fig1, use_container_width=False)
            
    with col_pies[1]:
        st.subheader("Конверсия бота (Там где принимал участие)")
        bot_participated_df = df_gsheet[df_gsheet['Статус'].isin(['Закрыл', 'Перевод'])]
        participated_count = len(bot_participated_df)
        transferred_count = participated_count - count_bot_closed
        
        if participated_count > 0:
            st.caption(f"Всего диалогов с ботом: {participated_count}")
            fig2, ax2 = plt.subplots(figsize=(4, 4))
            ax2.pie([count_bot_closed, transferred_count], 
                    labels=['Закрыл сам', 'Перевел на оператора'], 
                    autopct='%1.1f%%', colors=['#ff9999', '#ffcc99'], startangle=90)
            st.pyplot(fig2, use_container_width=False)
        else:
            st.write("Бот не участвовал в диалогах за этот период.")

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
# TAB 3: DEPT ANALYSIS (TL LOGIC + EVALUATIONS)
# ==========================================
with tabs[2]:
    st.subheader("Детальный анализ по отделу")
    if not df_api.empty:
        all_depts = sorted(df_api['Отдел'].unique())
        selected_dept = st.selectbox("Выберите отдел", all_depts, key="dept_analysis_v7")
        
        if selected_dept:
            dept_data = df_api[df_api['Отдел'] == selected_dept].copy()
            
            # 1. СЧИТАЕМ РЕЙТИНГ ОТДЕЛА (убираем дубли req_id, чтобы не задваивать оценки)
            unique_ratings = pd.to_numeric(dept_data.drop_duplicates('req_id')['rating'], errors='coerce').dropna()
            d_rate = unique_ratings.mean() if not unique_ratings.empty else 0.0
            d_rate_cnt = len(unique_ratings)
            d_rate_str = f"{d_rate:.2f}" if d_rate_cnt > 0 else "-"
            
            # 2. СЧИТАЕМ СКОРОСТЬ ОТДЕЛА
            dept_speeds = []
            operators_in_dept = dept_data['operator_id'].unique()
            for op_id in operators_in_dept:
                if op_id in speeds_map: dept_speeds.extend(speeds_map[op_id])
            d_med = np.median(dept_speeds) if dept_speeds else None

            # 3. ЛОГИКА ТИМЛИДОВ
            TL_ROOTS = ["черныш", "гетман", "власенков"]
            dept_data['is_tl'] = dept_data['Оператор'].apply(
                lambda x: any(normalize_text(tl) in normalize_text(x) for tl in TL_ROOTS)
            )

            # Заголовок с общими метриками
            st.markdown(f"""
            ### 📂 {selected_dept}: {dept_data['req_id'].nunique()} чатов
            **(По отделу: Ср. скорость: {format_seconds(d_med)} | Рейтинг: {d_rate_str} ({d_rate_cnt} шт.))**
            """)

            # 4. ПОСУТОЧНАЯ НАГРУЗКА
            if 'Дата' in dept_data.columns:
                daily_chats = dept_data.groupby('Дата')['req_id'].nunique()
                daily_ops = dept_data[~dept_data['is_tl']].groupby('Дата')['operator_id'].nunique()
                
                daily_stats = pd.DataFrame({'Чатов': daily_chats, 'Спецов': daily_ops}).reset_index().fillna(0)
                daily_stats['Нагрузка'] = daily_stats.apply(
                    lambda r: round(r['Чатов'] / r['Спецов'], 1) if r['Спецов'] > 0 else r['Чатов'], axis=1
                )
                
                st.write("#### Посуточная нагрузка отдела")
                st.dataframe(daily_stats.sort_values('Дата', ascending=False), use_container_width=True, hide_index=True)

            st.divider()

            # 5. СТАТИСТИКА СПЕЦИАЛИСТОВ (С ОЦЕНКАМИ)
            st.write("#### Статистика специалистов")
            op_list = dept_data.groupby(['operator_id', 'Оператор', 'is_tl']).agg(
                chats=('req_id', 'nunique')
            ).reset_index().sort_values('chats', ascending=False)
            
            spec_rows = []
            for _, row in op_list.iterrows():
                op_id = row['operator_id']
                
                # Медианные скорости
                s_first_speeds = first_speeds_map.get(op_id, [])
                s_first_med = np.median(s_first_speeds) if s_first_speeds else None
                s_avg = np.median(speeds_map.get(op_id, [])) if speeds_map.get(op_id) else None
                
                # Личный рейтинг оператора
                op_ratings = pd.to_numeric(
                    dept_data[dept_data['operator_id'] == op_id]['rating'], 
                    errors='coerce'
                ).dropna()
                s_rate_val = op_ratings.mean() if not op_ratings.empty else 0.0
                s_rate_cnt = len(op_ratings)
                
                spec_rows.append({
                    "Роль": "⭐ Team Lead" if row['is_tl'] else "Специалист",
                    "Специалист": f"⭐ {row['Оператор'].upper()}" if row['is_tl'] else row['Оператор'],
                    "Чаты": row['chats'],
                    "1-я скор.": format_seconds(s_first_med),
                    "Ср. скор.": format_seconds(s_avg),
                    "Рейтинг": f"{s_rate_val:.2f}" if s_rate_cnt > 0 else "-",
                    "Оценок": s_rate_cnt
                })
            
            df_spec = pd.DataFrame(spec_rows)
            st.dataframe(
                df_spec.style.apply(lambda r: ['background-color: #e3f2fd; font-weight: bold']*len(r) if "Team Lead" in r['Роль'] else ['']*len(r), axis=1),
                use_container_width=True, 
                hide_index=True
            )

            st.divider()
            
            # 6. ТЕМАТИКИ ИЗ GSHEET
            st.subheader("Тематика обращений (GSheet)")
            dept_gsheet = df_gsheet[df_gsheet['Отдел'] == selected_dept]
            if not dept_gsheet.empty:
                cat_counts = dept_gsheet['Тип обращения'].value_counts().reset_index()
                cat_counts.columns = ['Категория', 'Кол-во']
                
                total_chats_period = dept_data['req_id'].nunique()
                cat_counts['Доля'] = (cat_counts['Кол-во'] / total_chats_period * 100).map('{:.1f}%'.format)
                st.dataframe(cat_counts, use_container_width=True, hide_index=True)
    else:
        st.warning("Нет данных API. Запустите анализ.")

# ==========================================
# TAB 4: КАТЕГОРИИ (ДЕТАЛЬНАЯ АНАЛИТИКА)
# ==========================================
with tabs[3]:
    st.subheader("📊 Анализ типов обращений")
    
    # Создаем внутренние вкладки для чистоты интерфейса
    sub_tab1, sub_tab2 = st.tabs(["📋 Полная детализация", "📈 Интерактивный ТОП-15"])

    if not df_gsheet.empty:
        
        # --- 1. ПОДГОТОВКА ДАННЫХ ---
        def group_result_detailed(row):
            if row['Статус'] == 'Закрыл':
                return 'Бот справился'
            elif row['Статус'] == 'Перевод':
                reason = str(row.get('Причина перевода', 'Другое'))
                if reason in ['Требует сценарий', 'Не знает ответ', 'Лимит сообщений']:
                    return f"Перевод: {reason}"
                return "Перевод: Прочее"
            return "Без статуса"

        df_analysis = df_gsheet.copy()
        df_analysis['Результат'] = df_analysis.apply(group_result_detailed, axis=1)

        # --- 2. SUB-TAB 1: ТАБЛИЦА С ДЕТАЛИЗАЦИЕЙ ---
        with sub_tab1:
            st.write("#### Полная статистика по всем категориям")
            
            # Базовая группировка
            stats = df_gsheet.groupby(['Тип обращения', 'Статус']).size().unstack(fill_value=0)
            stats['Всего'] = stats.sum(axis=1)
            
            # Проверка наличия нужных колонок
            for c in ['Закрыл', 'Перевод']: 
                if c not in stats.columns: stats[c] = 0
                
            stats['Бот(✓)'] = (stats['Закрыл'] / stats['Всего'] * 100).map('{:.1f}%'.format)
            stats['Бот(→)'] = (stats['Перевод'] / stats['Всего'] * 100).map('{:.1f}%'.format)

            # Функция для формирования текстовой детализации (то, что ты хотел вернуть)
            def get_cat_details(row):
                transferred = row.get('Перевод', 0)
                if transferred == 0: return "—"
                cat_reasons = df_gsheet[(df_gsheet['Тип обращения'] == row.name) & (df_gsheet['Статус'] == 'Перевод')]
                r_counts = cat_reasons['Причина перевода'].value_counts()
                return "\n".join([f"• {r}: {(count/transferred*100):.0f}%" for r, count in r_counts.items() if count > 0])

            stats['Детализация перевода'] = stats.apply(get_cat_details, axis=1)
            
            # Финальная сборка таблицы
            final_table = stats[['Всего', 'Бот(✓)', 'Бот(→)', 'Детализация перевода']].sort_values('Всего', ascending=False).reset_index()
            
            st.dataframe(final_table, use_container_width=True, hide_index=True)

        # --- 3. SUB-TAB 2: ЦВЕТНОЙ ГРАФИК (ПРИЧИНЫ ПЕРЕВОДА) ---
        with sub_tab2:
            st.write("#### Топ-15 обращений в разрезе эффективности")
            
            top_names = df_gsheet['Тип обращения'].value_counts().nlargest(15).index
            df_plot = df_analysis[df_analysis['Тип обращения'].isin(top_names)]

            plot_data = df_plot.groupby(['Тип обращения', 'Результат']).size().reset_index(name='Количество')
            
            # Цветовая карта (твоя любимая контрастная схема)
            color_map = {
                'Бот справился': '#26A69A',           # Бирюзовый
                'Перевод: Не знает ответ': '#FF5252',  # Красный
                'Перевод: Требует сценарий': '#FFAB40', # Оранжевый
                'Перевод: Лимит сообщений': '#7C4DFF', # Фиолетовый
                'Перевод: Прочее': '#90A4AE',          # Серый
                'Без статуса': '#CFD8DC'                # Светло-серый
            }

            fig = px.bar(
                plot_data, 
                x="Количество", 
                y="Тип обращения", 
                color="Результат",
                orientation='h',
                color_discrete_map=color_map,
                text_auto=True,
                category_orders={"Тип обращения": top_names.tolist()} 
            )

            fig.update_layout(
                barmode='stack',
                height=700, 
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="y unified" 
            )
            
            fig.update_yaxes(title="")
            fig.update_xaxes(title="Количество диалогов")

            st.plotly_chart(fig, use_container_width=True)

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