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
# 1. КОНФИГУРАЦИЯ
# ==========================================
st.set_page_config(page_title="SLA Dashboard Hybrid", layout="wide")

st.markdown("""
    <style>
    .stDataFrame td { white-space: pre-wrap !important; vertical-align: top !important; }
    </style>
""", unsafe_allow_html=True)

# --- БЕЗОПАСНАЯ ЗАГРУЗКА СЕКРЕТОВ ---
# Значения по умолчанию для локального запуска (если secrets.toml нет)
API_TOKEN = "cb96240069dfaf99fee34e7bfb1c8b" # Вставьте сюда токен для локального теста
SHEET_ID = "123VexBVR3y9o6f6pnJKJAWV47PBpT0uhnCL9JSGwIBo"
GID = "465082032"
SECRET_PASSWORD = "123"

try:
    if "API_TOKEN" in st.secrets: API_TOKEN = st.secrets["API_TOKEN"]
    if "SHEET_ID" in st.secrets: SHEET_ID = st.secrets["SHEET_ID"]
    if "GID" in st.secrets: GID = st.secrets["GID"]
    if "PASSWORD" in st.secrets: SECRET_PASSWORD = st.secrets["PASSWORD"]
except Exception:
    # Игнорируем ошибки секретов при локальном запуске, если переменные заданы выше
    pass

# КОНСТАНТЫ
BASE_URL = "https://api.chat2desk.com/v1"
HEADERS = {"Authorization": API_TOKEN}
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
        st.subheader("Конверсия бота (Участие)")
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
    st.subheader("Тематика обращений по времени (ВСЕ обращения)")
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

# TAB 3: DEPT ANALYSIS
with tabs[2]:
    st.subheader("Детальный анализ по отделу")
    
    if not df_api.empty:
        all_depts = sorted(df_api['Отдел'].unique())
        selected_dept = st.selectbox("Выберите отдел", all_depts, key="dept_analysis_select")
        
        if selected_dept:
            # Фильтруем данные по отделу
            dept_data = df_api[df_api['Отдел'] == selected_dept].copy()
            
            # --- БЛОК 1: ОБЩАЯ СТАТИСТИКА (KPI) ---
            # Для корректного расчета истории нам нужна колонка с датой. 
            # Если её нет в df_api, динамика будет только по часам.
            
            total_chats = dept_data['req_id'].nunique()
            unique_ops = dept_data['operator_id'].nunique()
            avg_load = total_chats / unique_ops if unique_ops > 0 else 0
            
            c1, c2, c3 = st.columns(3)
            c1.metric("Всего чатов", total_chats)
            c2.metric("Спецов в смене", unique_ops)
            c3.metric("Нагрузка на 1 чел.", f"{avg_load:.1f}")

            # --- БЛОК 2: ДИНАМИКА ОБРАЩЕНИЙ ---
            # Если в данных есть дата, группируем по дням, если нет - по часам
            st.write("#### Динамика нагрузки")
            
            # Проверяем, есть ли колонка 'Дата' (добавь её в load_api_data_range как dt_local.date())
            group_col = 'Дата' if 'Дата' in dept_data.columns else 'Час'
            hist_data = dept_data.groupby(group_col)['req_id'].nunique().reset_index()
            hist_data.columns = [group_col, 'Чатов']
            
            fig_hist = px.line(hist_data, x=group_col, y='Чатов', 
                               markers=True, line_shape="spline",
                               color_discrete_sequence=['#636EFA'])
            fig_hist.update_layout(height=300, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_hist, use_container_width=True)

            st.divider()
            
            # --- БЛОК 3: ТАБЛИЦА СПЕЦИАЛИСТОВ (С ТИМЛИДАМИ) ---
            st.write("#### Статистика по специалистам")
            
            TL_NAMES = ["Чернышева", "Гетман", "Власенкова"]
            specialist_stats = []
            
            op_list = dept_data.groupby(['operator_id', 'Оператор']).agg(
                chats=('req_id', 'nunique')
            ).reset_index().sort_values('chats', ascending=False)
            
            for i, row in op_list.iterrows():
                op_id = row['operator_id']
                op_name = row['Оператор']
                cnt = row['chats']
                
                # Логика подсветки Тимлидов
                role = "Специалист"
                display_name = op_name
                for tl in TL_NAMES:
                    if tl.lower() in op_name.lower():
                        role = "⭐ Team Lead"
                        display_name = f"⭐ {op_name.upper()}"
                        break

                s_first_speeds = first_speeds_map.get(op_id, [])
                s_first_med = np.median(s_first_speeds) if s_first_speeds else None
                
                s_speeds = speeds_map.get(op_id, [])
                s_med = np.median(s_speeds) if s_speeds else None
                
                op_ratings = pd.to_numeric(
                    dept_data[dept_data['operator_id'] == op_id]['rating'], 
                    errors='coerce'
                ).dropna()
                
                s_rate_val = op_ratings.mean() if not op_ratings.empty else 0.0
                
                specialist_stats.append({
                    "Роль": role,
                    "Специалист": display_name,
                    "Чаты": cnt,
                    "1-я скорость": format_seconds(s_first_med),
                    "Ср. скорость": format_seconds(s_med),
                    "Рейтинг": f"{s_rate_val:.2f}" if not op_ratings.empty else "-"
                })
            
            df_spec = pd.DataFrame(specialist_stats)
            
            # Используем условное форматирование для выделения Тимлидов
            def highlight_tl(row):
                return ['background-color: #f0f7ff; font-weight: bold' if "Team Lead" in row['Роль'] else '' for _ in row]

            st.dataframe(
                df_spec.style.apply(highlight_tl, axis=1),
                use_container_width=True, 
                hide_index=True
            )

            st.divider()
            
            # --- БЛОК 4: ТЕМАТИКИ ---
            st.subheader("Тематика обращений (GSheet)")
            dept_gsheet = df_gsheet[df_gsheet['Отдел'] == selected_dept]
            if not dept_gsheet.empty:
                cat_counts = dept_gsheet['Тип обращения'].value_counts().reset_index()
                cat_counts.columns = ['Категория', 'Кол-во']
                cat_counts['Доля'] = (cat_counts['Кол-во'] / total_chats * 100).map('{:.1f}%'.format)
                st.dataframe(cat_counts, use_container_width=True, hide_index=True)
    else:
        st.warning("Нет данных API. Запустите анализ.")

import plotly.express as px # Не забудь добавить это в начало файла или в requirements.txt

# TAB 4: Анализ категорий (Интерактивный и широкий)
with tabs[3]:
    # Создаем внутренние вкладки для чистоты интерфейса
    sub_tab1, sub_tab2 = st.tabs(["📋 Полная детализация", "📈 Интерактивный ТОП-15"])

    if not df_gsheet.empty:
        
        # --- ПОДГОТОВКА ДАННЫХ (Общая для обоих под-табов) ---
        # 1. Группируем результат для аналитики
        def group_result_detailed(row):
            if row['Статус'] == 'Закрыл':
                return 'Бот справился'
            elif row['Статус'] == 'Перевод':
                reason = str(row.get('Причина перевода', 'Другое'))
                if reason in ['Требует сценарий', 'Не знает ответ', 'Лимит сообщений']:
                    return f"Перевод: {reason}"
                return "Перевод: Прочее"
            return "Без статуса"

        # Создаем временный DF для расчетов
        df_analysis = df_gsheet.copy()
        df_analysis['Результат'] = df_analysis.apply(group_result_detailed, axis=1)

        # --- SUB-TAB 1: ТАБЛИЦА (Теперь она первая) ---
        with sub_tab1:
            st.write("#### Полная статистика по всем категориям")
            
            stats = df_gsheet.groupby(['Тип обращения', 'Статус']).size().unstack(fill_value=0)
            stats['Всего'] = stats.sum(axis=1)
            
            for c in ['Закрыл', 'Перевод']: 
                if c not in stats.columns: stats[c] = 0
                
            stats['Бот(✓)'] = (stats['Закрыл'] / stats['Всего'] * 100).map('{:.1f}%'.format)
            stats['Бот(→)'] = (stats['Перевод'] / stats['Всего'] * 100).map('{:.1f}%'.format)

            def get_cat_details(row):
                transferred = row.get('Перевод', 0)
                if transferred == 0: return "—"
                cat_reasons = df_gsheet[(df_gsheet['Тип обращения'] == row.name) & (df_gsheet['Статус'] == 'Перевод')]
                r_counts = cat_reasons['Причина перевода'].value_counts()
                return "\n".join([f"• {r}: {(count/transferred*100):.0f}%" for r, count in r_counts.items() if count > 0])

            stats['Детализация перевода'] = stats.apply(get_cat_details, axis=1)
            final_table = stats[['Всего', 'Бот(✓)', 'Бот(→)', 'Детализация перевода']].sort_values('Всего', ascending=False).reset_index()
            
            st.dataframe(final_table, use_container_width=True, hide_index=True)

        # --- SUB-TAB 2: ГРАФИК (Широкий и интерактивный) ---
        with sub_tab2:
            st.write("#### Топ-15 обращений в разрезе эффективности")
            
            # Фильтруем ТОП-15
            top_names = df_gsheet['Тип обращения'].value_counts().nlargest(15).index
            df_plot = df_analysis[df_analysis['Тип обращения'].isin(top_names)]

            # Группируем для Plotly
            plot_data = df_plot.groupby(['Тип обращения', 'Результат']).size().reset_index(name='Количество')
            
            # Настройка контрастных цветов
            # Бот - зеленый, Переводы - яркие разные цвета, Прочее - серый
            color_map = {
                'Бот справился': '#26A69A',           # Бирюзово-зеленый
                'Перевод: Не знает ответ': '#FF5252',  # Ярко-красный
                'Перевод: Требует сценарий': '#FFAB40', # Насыщенный оранжевый
                'Перевод: Лимит сообщений': '#7C4DFF', # Фиолетовый
                'Перевод: Прочее': '#90A4AE',          # Сине-серый
                'Без статуса': '#CFD8DC'               # Светло-серый
            }

            # Создаем график Plotly
            fig = px.bar(
                plot_data, 
                x="Количество", 
                y="Тип обращения", 
                color="Результат",
                orientation='h',
                color_discrete_map=color_map,
                text_auto=True,
                category_orders={"Тип обращения": top_names.tolist()} # Сохраняем сортировку
            )

            # Настраиваем внешний вид и подсказки (hover)
            fig.update_layout(
                barmode='stack',
                height=700, # Делаем график высоким
                margin=dict(l=0, r=0, t=30, b=0),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                hovermode="y unified" # Красивая общая подсказка при наведении на строку
            )
            
            fig.update_yaxes(title="") # Убираем заголовок оси Y для красоты
            fig.update_xaxes(title="Количество диалогов")

            # Выводим график на всю ширину
            st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Нет данных за выбранный период.")

# ==========================================
# TAB 5: ДИНАМИКА (НОВЫЙ)
# ==========================================
with tabs[4]:
    st.subheader("Динамика обращений и эффективность бота")
    
    # 1. Выбор периода сравнения
    col_p1, col_p2 = st.columns(2)
    with col_p1:
        compare_mode = st.radio(
            "Выберите режим сравнения:",
            ["День к дню", "Неделя к неделе", "Месяц к месяцу", "Свой период"],
            horizontal=True
        )
    
    today = datetime.now().date()
    
    # Логика определения дат
    if compare_mode == "День к дню":
        curr_start, curr_end = today - timedelta(days=1), today - timedelta(days=1)
        prev_start, prev_end = today - timedelta(days=2), today - timedelta(days=2)
    elif compare_mode == "Неделя к неделе":
        curr_start = today - timedelta(days=today.weekday() + 7)
        curr_end = curr_start + timedelta(days=6)
        prev_start = curr_start - timedelta(days=7)
        prev_end = prev_start + timedelta(days=6)
    elif compare_mode == "Месяц к месяцу":
        curr_end = today.replace(day=1) - timedelta(days=1)
        curr_start = curr_end.replace(day=1)
        prev_end = curr_start - timedelta(days=1)
        prev_start = prev_end.replace(day=1)
    else:
        with col_p2:
            custom_dates = st.date_input("Выберите ТЕКУЩИЙ период:", [today - timedelta(days=7), today])
            if len(custom_dates) == 2:
                curr_start, curr_end = custom_dates
                delta = curr_end - curr_start
                prev_start = curr_start - delta - timedelta(days=1)
                prev_end = curr_start - timedelta(days=1)
            else: st.stop()

    st.info(f"📊 Сравнение: **{curr_start} — {curr_end}** (Текущий) vs **{prev_start} — {prev_end}** (Прошлый)")

    # 2. Расчет данных
    stats_curr = get_dynamics_stats(df_gsheet_all, curr_start, curr_end)
    stats_prev = get_dynamics_stats(df_gsheet_all, prev_start, prev_end)

    # Объединяем
    df_dyn = stats_curr.join(stats_prev, lsuffix='_curr', rsuffix='_prev', how='outer').fillna(0)
    
    # 3. Расчет метрик V и B
    def calculate_metrics(row):
        # V (Volume)
        v_curr = row['Всего_curr']
        v_prev = row['Всего_prev']
        v_diff_pct = ((v_curr / v_prev - 1) * 100) if v_prev > 0 else (100.0 if v_curr > 0 else 0.0)
        
        # B (Bot Efficiency)
        b_curr = row['Бот_%_curr']
        b_prev = row['Бот_%_prev']
        b_diff_pp = b_curr - b_prev
        
        # Форматирование для таблицы (как в твоем примере)
        v_color = "🔴" if v_diff_pct > 0 else "🟢"
        b_color = "🟢" if b_diff_pp > 0 else ("🔴" if b_diff_pp < 0 else "⚪")
        
        v_text = f"{v_color} V: {v_diff_pct:+.1f}%"
        b_text = f"{b_color} B: {b_diff_pp:+.1f}пп"
        
        return pd.Series([
            f"{int(v_curr)}\n({b_curr:.1f}%)",
            f"{int(v_prev)}\n({b_prev:.1f}%)",
            f"{v_text}\n{b_text}"
        ])

    if not df_dyn.empty:
        res_table = df_dyn.apply(calculate_metrics, axis=1)
        res_table.columns = ['Текущий период', 'Прошлый период', 'Динамика (V и B)']
        res_table = res_table.sort_values(by='Текущий период', ascending=False)
        
        # Добавляем строку ИТОГО
        total_curr_v = df_dyn['Всего_curr'].sum()
        total_prev_v = df_dyn['Всего_prev'].sum()
        total_curr_b = (df_gsheet_all[(df_gsheet_all['Дата'].dt.date >= curr_start) & 
                                     (df_gsheet_all['Дата'].dt.date <= curr_end)]['Статус'] == 'Закрыл').mean() * 100
        total_prev_b = (df_gsheet_all[(df_gsheet_all['Дата'].dt.date >= prev_start) & 
                                     (df_gsheet_all['Дата'].dt.date <= prev_end)]['Статус'] == 'Закрыл').mean() * 100
        
        v_total_pct = ((total_curr_v / total_prev_v - 1) * 100) if total_prev_v > 0 else 0
        b_total_pp = total_curr_b - total_prev_b
        
        total_row = pd.DataFrame([{
            'Текущий период': f"{int(total_curr_v)}\n({total_curr_b:.1f}%)",
            'Прошлый период': f"{int(total_prev_v)}\n({total_prev_b:.1f}%)",
            'Динамика (V и B)': f"{'🔴' if v_total_pct > 0 else '🟢'} V: {v_total_pct:+.1f}%\n{'🟢' if b_total_pp > 0 else '🔴'} B: {b_total_pp:+.1f}пп"
        }], index=['ИТОГО ПО ВСЕМ КАТЕГОРИЯМ'])
        
        res_table = pd.concat([res_table, total_row])

        # Вывод таблицы с поддержкой переносов строк
        st.dataframe(res_table, use_container_width=True, height=600)
        
        st.markdown("""
        > **Volume (V):** Рост (+) 🔴 — плохо, Снижение (-) 🟢 — хорошо.  
        > **Bot (B):** Рост (+) 🟢 — хорошо, Снижение (-) 🔴 — плохо.
        """)
    else:
        st.warning("Нет данных для сравнения в выбранных периодах.")

# ==========================================
# TAB 6: БАЗА ДАННЫХ
# ==========================================
with tabs[4]:
    st.subheader("🗄️ Полная база данных")
    if not df_gsheet.empty:
        st.write(f"Отображено записей: {len(df_gsheet)}")
        st.dataframe(df_gsheet, use_container_width=True)
    else:
        st.info("Нет данных для отображения за выбранный период.")