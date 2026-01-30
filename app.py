import streamlit as st
import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
import seaborn as sns
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
try:
    API_TOKEN = st.secrets["API_TOKEN"]
    SHEET_ID = st.secrets["SHEET_ID"]
    GID = st.secrets["GID"]
    # Пробуем загрузить пароль
    SECRET_PASSWORD = st.secrets["PASSWORD"]
except KeyError as e:
    st.error(f"❌ ОШИБКА БЕЗОПАСНОСТИ: В настройках Secrets не найден ключ {e}. Проверьте файл secrets.toml.")
    st.stop()
except Exception as e:
    st.error(f"❌ Критическая ошибка чтения секретов: {e}")
    st.stop()

# КОНСТАНТЫ
BASE_URL = "https://api.chat2desk.com/v1"
HEADERS = {"Authorization": API_TOKEN}
SHEET_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv&gid={GID}"

MAX_WORKERS = 20
TIME_OFFSET = 3

# СПРАВОЧНИКИ
OPERATORS_MAP = {310507: "Бот AI", 0: "Система"}
DEPARTMENT_MAPPING = {
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
    "Никита Приходько": "Concierge", 
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
# 2. АВТОРИЗАЦИЯ (СТРОГАЯ + ДИАГНОСТИКА)
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
                # 1. Приводим всё к строкам и убираем пробелы
                input_clean = str(password).strip()
                secret_clean = str(SECRET_PASSWORD).strip()
                
                # 2. Сравнение
                if input_clean == secret_clean:
                    st.session_state["password_correct"] = True
                    st.rerun()
                else:
                    st.error("⛔ Неверный пароль")
                    
                    # --- ДИАГНОСТИКА (ЕСЛИ НЕ РАБОТАЕТ - РАСКОММЕНТИРУЙТЕ СТРОКИ НИЖЕ) ---
                    # st.warning(f"Диагностика (покажите это админу):")
                    # st.write(f"Ожидаемая длина пароля: {len(secret_clean)}")
                    # st.write(f"Ваша длина пароля: {len(input_clean)}")
                    # st.write(f"Система видит первые символы пароля как: {secret_clean[:2]}***")
                    # ---------------------------------------------------------------------
                    
        return False
    return True

if not check_password():
    st.stop()

# ==========================================
# 3. ФУНКЦИИ API
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
            
            elif msg_type == 'out' and op_id and op_id != 0 and op_id != 310507:
                 if target_start <= dt_local <= target_end:
                     stats['participations'].add(op_id)
                     
                     if op_id not in stats['op_hours']: stats['op_hours'][op_id] = set()
                     stats['op_hours'][op_id].add(dt_local.hour)
                     
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
                # Сбор скоростей
                for op_id, speeds in res['operator_speeds'].items():
                    if op_id not in all_speeds: all_speeds[op_id] = []
                    all_speeds[op_id].extend(speeds)
                    
                    if speeds:
                        if op_id not in all_first_speeds: all_first_speeds[op_id] = []
                        all_first_speeds[op_id].append(speeds[0])

                # Строки для DF
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
                            'Час': -1
                        })
                    else:
                        for h in hours:
                            final_rows.append({
                                'req_id': res['req_id'],
                                'operator_id': op_id,
                                'Оператор': op_name,
                                'Отдел': dept,
                                'rating': res['rating'],
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
            if col in df.columns: df[col] = df[col].astype(str).str.strip()
        df['Час'] = df['Дата'].dt.hour
        return df
    except Exception as e:
        st.error(f"Ошибка загрузки Google Sheet: {e}"); return pd.DataFrame()

# ==========================================
# 5. UI & LOGIC
# ==========================================
st.sidebar.title("Фильтры")

df_gsheet_all = load_gsheet_data()
if not df_gsheet_all.empty:
    default_min = df_gsheet_all['Дата'].max().date()
    default_max = df_gsheet_all['Дата'].max().date()
else:
    default_min = datetime.now().date()
    default_max = datetime.now().date()

date_range = st.sidebar.date_input(
    "Диапазон дат",
    value=(default_min, default_max),
    min_value=df_gsheet_all['Дата'].min().date() if not df_gsheet_all.empty else None,
    max_value=datetime.now().date()
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    sel_start, sel_end = date_range
elif isinstance(date_range, tuple) and len(date_range) == 1:
    sel_start = sel_end = date_range[0]
else:
    sel_start = sel_end = date_range

st.sidebar.caption(f"Выбрано: {sel_start} — {sel_end}")

if st.sidebar.button("Запустить анализ (API)"):
    st.session_state['run_analysis'] = True
    st.cache_data.clear()

if 'run_analysis' not in st.session_state:
    st.info("👈 Выберите даты и нажмите 'Запустить анализ'"); st.stop()

# ЗАГРУЗКА
df_api, speeds_map, first_speeds_map = load_api_data_range(sel_start, sel_end)

mask_gsheet = (df_gsheet_all['Дата'].dt.date >= sel_start) & (df_gsheet_all['Дата'].dt.date <= sel_end)
df_gsheet = df_gsheet_all[mask_gsheet].copy()

# KPI
if not df_api.empty: count_human_chats = df_api['req_id'].nunique()
else: count_human_chats = 0

bot_closed_mask = (df_gsheet['Статус'].str.lower() == 'закрыл')
count_bot_closed = len(df_gsheet[bot_closed_mask])

auth_mask = (df_gsheet['Тип обращения'].str.contains('Авторизация пройдена', case=False, na=False))
count_auth = len(df_gsheet[auth_mask])

total_chats_day = count_human_chats + count_bot_closed + count_auth

# --- TABS ---
st.title(f"📊 Отчетность SLA ({sel_start} — {sel_end})")
tabs = st.tabs(["KPI", "Нагрузка", "Анализ отдела", "Категории", "База данных"])

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
            
            # Строим карту по данным API
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
    topics_df = df_gsheet[~df_gsheet['Тип обращения'].isin(['Авторизация пройдена'])].copy()
    topics_df['Тип обращения'] = topics_df['Тип обращения'].replace('-', 'Без темы')
    
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
        selected_dept = st.selectbox("Выберите отдел", all_depts)
        
        if selected_dept:
            dept_data = df_api[df_api['Отдел'] == selected_dept]
            
            unique_ratings = pd.to_numeric(dept_data.drop_duplicates('req_id')['rating'], errors='coerce').dropna()
            
            dept_speeds = []
            operators_in_dept = dept_data['operator_id'].unique()
            for op_id in operators_in_dept:
                if op_id in speeds_map: dept_speeds.extend(speeds_map[op_id])
            
            d_chats = dept_data['req_id'].nunique()
            d_med = np.median(dept_speeds) if dept_speeds else None
            
            d_rate = unique_ratings.mean() if not unique_ratings.empty else 0.0
            d_rate_cnt = len(unique_ratings)
            d_rate_str = f"{d_rate:.2f}" if d_rate_cnt > 0 else "-"
            
            st.markdown(f"""
            ### 📂 {selected_dept}: {d_chats} чатов
            **(По отделу: Ср. скорость: {format_seconds(d_med)} | Рейтинг: {d_rate_str} ({d_rate_cnt}))**
            """)
            
            st.divider()
            
            # --- ТАБЛИЦА СПЕЦИАЛИСТОВ ---
            st.write("#### Статистика по специалистам")
            
            specialist_stats = []
            
            op_list = dept_data.groupby(['operator_id', 'Оператор']).agg(
                chats=('req_id', 'nunique')
            ).reset_index().sort_values('chats', ascending=False)
            
            for i, row in op_list.iterrows():
                op_id = row['operator_id']
                op_name = row['Оператор']
                cnt = row['chats']
                
                s_first_speeds = first_speeds_map.get(op_id, [])
                s_first_med = np.median(s_first_speeds) if s_first_speeds else None
                s_first_str = format_seconds(s_first_med)
                
                s_speeds = speeds_map.get(op_id, [])
                s_med = np.median(s_speeds) if s_speeds else None
                s_time_str = format_seconds(s_med)
                
                op_ratings = pd.to_numeric(
                    dept_data[dept_data['operator_id'] == op_id]['rating'], 
                    errors='coerce'
                ).dropna()
                
                s_rate_val = op_ratings.mean() if not op_ratings.empty else 0.0
                s_rate_cnt = len(op_ratings)
                s_rate_str = f"{s_rate_val:.2f}" if s_rate_cnt > 0 else "-"
                
                specialist_stats.append({
                    "Оператор": op_name,
                    "Чаты": cnt,
                    "1-я скорость (мед)": s_first_str,
                    "Ср. скорость (мед)": s_time_str,
                    "Рейтинг": s_rate_str,
                    "Кол-во оценок": s_rate_cnt
                })
            
            df_spec = pd.DataFrame(specialist_stats)
            st.dataframe(
                df_spec, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Оператор": st.column_config.TextColumn("Специалист"),
                    "Чаты": st.column_config.NumberColumn("Чатов"),
                }
            )

            st.divider()
            
            # ТЕМАТИКИ
            st.subheader("Тематика обращений (GSheet)")
            dept_gsheet = df_gsheet[df_gsheet['Отдел'] == selected_dept]
            cat_counts = dept_gsheet['Тип обращения'].value_counts().reset_index()
            cat_counts.columns = ['Категория', 'Кол-во']
            
            known = len(dept_gsheet[dept_gsheet['Тип обращения'] != '-'])
            unknown = max(0, d_chats - known)
            
            if unknown > 0:
                new_row = pd.DataFrame([{'Категория': 'Неизвестные (разница)', 'Кол-во': unknown}])
                cat_counts = pd.concat([cat_counts, new_row], ignore_index=True)
            
            cat_counts['Доля'] = (cat_counts['Кол-во'] / d_chats * 100).map('{:.1f}%'.format)
            st.dataframe(cat_counts, use_container_width=True, hide_index=True)

# TAB 4 & 5
with tabs[3]:
    st.subheader("Категории (Бот)")
    ai_df = df_gsheet[df_gsheet['Статус'].isin(['Закрыл', 'Перевод'])]
    if not ai_df.empty:
        stats = ai_df.groupby('Тип обращения')['Статус'].value_counts().unstack(fill_value=0)
        for c in ['Закрыл', 'Перевод']: 
            if c not in stats.columns: stats[c] = 0
        stats['Total'] = stats['Закрыл'] + stats['Перевод']
        stats['Бот(✓)'] = (stats['Закрыл']/stats['Total']*100).map('{:.1f}%'.format)
        stats['Бот(→)'] = (stats['Перевод']/stats['Total']*100).map('{:.1f}%'.format)
        
        tr_df = ai_df[ai_df['Статус'] == 'Перевод']
        reasons = ['Требует сценарий', 'Не знает ответ', 'Лимит сообщений']
        r_counts = pd.DataFrame() if tr_df.empty else tr_df.groupby('Тип обращения')['Причина перевода'].value_counts().unstack(fill_value=0)
        for r in reasons: 
            if r not in r_counts.columns: r_counts[r] = 0
        stats = stats.join(r_counts, how='left').fillna(0)
        
        def fmt_r(row):
            tot = row['Перевод']
            if tot == 0: return "-"
            res = [f"• {r}: {(row.get(r,0)/tot*100):.0f}%" for r in reasons if row.get(r,0) > 0]
            return "\n".join(res) if res else "• Другая"
        
        stats['Причины'] = stats.apply(fmt_r, axis=1)
        final = stats[['Total', 'Бот(✓)', 'Бот(→)', 'Причины']].sort_values('Total', ascending=False).reset_index()
        st.dataframe(final, use_container_width=True, hide_index=True, height=600, column_config={"Причины": st.column_config.TextColumn(width="medium")})

with tabs[4]:
    st.dataframe(df_gsheet, use_container_width=True)