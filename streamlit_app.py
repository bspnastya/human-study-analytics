from __future__ import annotations
import streamlit as st, pandas as pd, plotly.express as px
import gspread, json, datetime
from oauth2client.service_account import ServiceAccountCredentials
from streamlit_autorefresh import st_autorefresh



st.set_page_config("Аналитика исследования", "📊", layout="wide")

REQ_ANS      = 40  
REFRESH_SEC  = 30
st_autorefresh(interval=REFRESH_SEC*1000, key="auto")


@st.cache_data(ttl=REFRESH_SEC, show_spinner="Обновляю данные…")
def load_sheet() -> pd.DataFrame:
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds  = ServiceAccountCredentials.from_json_keyfile_dict(
                dict(st.secrets["gsp"]), scopes)
    sh     = gspread.authorize(creds).open("human_study_results").sheet1
    raw    = sh.get_all_values()

    base_cols = ["timestamp","Пользователь","qnum","image_id","Алгоритм","Тип",
                 "Вопрос","Ответ","Правильный_ответ","time_ms","is_correct","session_id"]

    if not raw:
        return pd.DataFrame(columns=base_cols)

 
    if raw[0][:3] == base_cols[:3]:
        raw = raw[1:]

    cols = base_cols[:len(max(raw, key=len))]
    df   = pd.DataFrame(raw, columns=cols)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    df["time_ms"]   = pd.to_numeric(df["time_ms"], errors="coerce")
    df["Время_сек"] = df["time_ms"]/1000

    df["is_correct"] = df["is_correct"].astype(str).str.strip().str.upper().isin(["TRUE","1","YES"])

    full_users = (df.groupby("Пользователь")["qnum"]
                    .count().pipe(lambda s: s[s==REQ_ANS]).index)
    return df[df["Пользователь"].isin(full_users)]

try:
    df_raw = load_sheet()
except Exception as e:
    st.error(f"Ошибка при загрузке данных: {e}")
    st.stop()

if df_raw.empty:
    st.warning("Нет пользователей, прошедших тест полностью.")
    st.stop()


st.sidebar.header("Фильтры")

users_sel = st.sidebar.multiselect("Пользователь", sorted(df_raw["Пользователь"].unique()))
meth_sel  = st.sidebar.multiselect("Алгоритм",     sorted(df_raw["Алгоритм"].unique()))
quest_sel = st.sidebar.multiselect("Вопрос",       sorted(df_raw["Вопрос"].unique()))
pics_sel  = st.sidebar.multiselect("Изображение",  sorted(df_raw["image_id"].unique()))

date_min, date_max = df_raw["timestamp"].dt.date.agg(["min","max"])
date_from = st.sidebar.date_input("Дата от", date_min)
date_to   = st.sidebar.date_input("Дата до", date_max)

mask = pd.Series(True, index=df_raw.index)
if users_sel: mask &= df_raw["Пользователь"].isin(users_sel)
if meth_sel:  mask &= df_raw["Алгоритм"].isin(meth_sel)
if quest_sel: mask &= df_raw["Вопрос"].isin(quest_sel)
if pics_sel:  mask &= df_raw["image_id"].isin(pics_sel)
mask &= (df_raw["timestamp"].dt.date >= date_from) & \
        (df_raw["timestamp"].dt.date <= date_to)

df = df_raw[mask]


total   = len(df)
corr    = df["is_correct"].mean()*100 if total else 0
mean_t  = df["Время_сек"].mean()      if total else 0
med_t   = df["Время_сек"].median()    if total else 0
dont_k  = df["Ответ"].str.lower().str.startswith("затруд").sum()

a,b,c,d,e = st.columns(5)
a.metric("Всего ответов", f"{total:,}".replace(',',' '))
b.metric("Корректность", f"{corr:.1f}%")
c.metric("Среднее время", f"{mean_t:.2f} с")
d.metric("Медианное время", f"{med_t:.2f} с")
e.metric("«Затрудняюсь»", f"{dont_k:,}".replace(',',' '))

st.divider()

if not total:
    st.info("Нет данных под выбранные фильтры.")
    st.stop()


q99 = df["Время_сек"].quantile(0.99)
fig = px.histogram(df.query("Время_сек <= @q99"), x="Время_сек", nbins=20,
                   title="Распределение времени ответа",
                   labels={"Время_сек":"Время, с","count":"Количество"})
st.plotly_chart(fig, use_container_width=True)


st.subheader("Пользователи")
perf = (df.groupby("Пользователь")
          .agg(Ответов=("qnum","count"),
               Точность=("is_correct","mean"),
               Ср_время=("Время_сек","mean"),
               Затрудняюсь =("Ответ",lambda s:(s.str.lower().str.startswith("затруд")).sum()))
          .reset_index())
perf["Точность"] = (perf["Точность"]*100).round(1)
perf["Ср_время"] = perf["Ср_время"].round(2)
st.dataframe(perf, use_container_width=True)


st.subheader("Статистика по алгоритмам")
alg_stats = (df.groupby("Алгоритм")
               .agg(Ответов=("qnum","count"),
                    Точность=("is_correct","mean"),
                    Ср_время=("Время_сек","mean"),
                    Затрудняюсь =("Ответ",lambda s:(s.str.lower().str.startswith("затруд")).sum()))
               .reset_index())
alg_stats["Точность"] = (alg_stats["Точность"]*100).round(1)
alg_stats["Ср_время"] = alg_stats["Ср_время"].round(2)
fig_alg = px.bar(alg_stats, x="Алгоритм", y="Точность",
                 title="Точность ответов по алгоритмам",
                 labels={"Точность":"Точность, %"})
st.plotly_chart(fig_alg, use_container_width=True)
fig_dz = px.bar(alg_stats, x="Алгоритм", y="Затрудняюсь",
                title="«Затрудняюсь» по алгоритмам",
                labels={"Затрудняюсь":"Кол-во"})
st.plotly_chart(fig_dz, use_container_width=True)
st.dataframe(alg_stats, use_container_width=True)


st.subheader("Статистика по изображениям")
pic_stats = (df.groupby("image_id")
               .agg(Ответов=("qnum","count"),
                    Точность=("is_correct","mean"),
                    Ср_время=("Время_сек","mean"),
                    Затрудняюсь =("Ответ",lambda s:(s.str.lower().str.startswith("затруд")).sum()))
               .reset_index())
pic_stats["Точность"] = (pic_stats["Точность"]*100).round(1)
pic_stats["Ср_время"] = pic_stats["Ср_время"].round(2)
st.dataframe(pic_stats, use_container_width=True, height=350)



st.subheader("Буквенные вопросы: точность первого показа по алгоритмам")

letters = df[df["Тип"] == "letters"].copy()
if not letters.empty:
    letters["Правильный_ответ"] = letters["Правильный_ответ"].str.strip().str.lower()

    first = (letters.sort_values("timestamp")
                     .groupby(["Пользователь","Правильный_ответ"], as_index=False)
                     .first())

    stat = (first.groupby(["Правильный_ответ","Алгоритм"])
                   .agg(Пользователей=("Пользователь","count"),
                        Точность      =("is_correct","mean"))
                   .reset_index())
    stat["Точность"] = (stat["Точность"]*100).round(1)

    cats = sorted(stat["Правильный_ответ"].unique())
    sel  = st.radio("Категория букв", cats, horizontal=True, key="letter_cat")

    sub  = stat[stat["Правильный_ответ"] == sel].sort_values("Алгоритм")

    st.plotly_chart(
        px.bar(sub, x="Алгоритм", y="Точность", text="Пользователей",
               title=f"Первая встреча «{sel.upper()}»: средняя корректность",
               labels={"Точность":"Точность, %", "Пользователей":"Пользователей"}),
        use_container_width=True)

    st.dataframe(sub, use_container_width=True)
else:
    st.info("В данных нет вопросов с буквами.")




st.subheader("Данные")
csv = df.to_csv(index=False).encode("utf-8-sig")
st.download_button("💾 Скачать CSV", csv, "human_study_results.csv", "text/csv")
display_cols = ["timestamp","Пользователь","qnum","image_id","Алгоритм","Тип",
                "Вопрос","Ответ","Правильный_ответ","Время_сек","is_correct","session_id"]
present = [c for c in display_cols if c in df.columns]
st.dataframe(df[present], use_container_width=True, height=500)

st.caption(f"Данные обновляются каждые {REFRESH_SEC} секунд")
