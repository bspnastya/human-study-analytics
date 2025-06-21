from __future__ import annotations
import streamlit as st, pandas as pd, plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from streamlit_autorefresh import st_autorefresh


st.set_page_config("Аналитика исследования", "📊", layout="wide")
REQ_ANS     = 40     
REFRESH_SEC = 30
st_autorefresh(interval=REFRESH_SEC * 1000, key="auto")


@st.cache_data(ttl=REFRESH_SEC, show_spinner="Обновляю данные…")
def load_sheet() -> pd.DataFrame:
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]

    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        dict(st.secrets["gsp"]), scopes)
    sheet = gspread.authorize(creds).open("human_study_results").sheet1
    raw   = sheet.get_all_values()

    cols = ["timestamp","Пользователь","qnum","image_id","Алгоритм","Тип",
            "Вопрос","Ответ","Правильный_ответ","time_ms","is_correct"]

    if raw and [c.lower() for c in raw[0][:len(cols)]] == [c.lower() for c in cols]:
        raw = raw[1:]

    df = pd.DataFrame(raw, columns=cols)


    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    df["time_ms"]    = pd.to_numeric(df["time_ms"], errors="coerce")
    df["Время_сек"]  = df["time_ms"] / 1000
    df["is_correct"] = df["is_correct"].astype(str).str.upper().eq("TRUE")

   
    full_users = (
        df.groupby("Пользователь")["qnum"]
          .count()
          .pipe(lambda s: s[s == REQ_ANS])
          .index
    )
    df = df[df["Пользователь"].isin(full_users)]

    return df

df_raw = load_sheet()
if df_raw.empty:
    st.warning("Нет пользователей, прошедших тест полностью.")
    st.stop()


st.sidebar.header("Фильтры")

users_sel = st.sidebar.multiselect(
    "Пользователь", sorted(df_raw["Пользователь"].unique()))
meth_sel  = st.sidebar.multiselect(
    "Алгоритм",     sorted(df_raw["Алгоритм"].unique()))
quest_sel = st.sidebar.multiselect(
    "Вопрос",       sorted(df_raw["Вопрос"].unique()))

date_min, date_max = df_raw["timestamp"].dt.date.agg(["min", "max"])
date_from = st.sidebar.date_input("Дата от", date_min)
date_to   = st.sidebar.date_input("Дата до", date_max)

mask = pd.Series(True, index=df_raw.index)

if users_sel:
    mask &= df_raw["Пользователь"].isin(users_sel)
if meth_sel:
    mask &= df_raw["Алгоритм"].isin(meth_sel)
if quest_sel:
    mask &= df_raw["Вопрос"].isin(quest_sel)

mask &= (df_raw["timestamp"].dt.date >= date_from) & \
        (df_raw["timestamp"].dt.date <= date_to)

df = df_raw[mask]


a, b, c, d = st.columns(4)

total  = len(df)
corr   = df["is_correct"].mean()*100 if total else 0
mean_t = df["Время_сек"].mean()      if total else 0
med_t  = df["Время_сек"].median()    if total else 0

a.metric("Всего ответов",   f"{total:,}".replace(',', ' '))
b.metric("Корректность",    f"{corr:.1f}%")
c.metric("Среднее время",   f"{mean_t:.2f} с")
d.metric("Медианное время", f"{med_t:.2f} с")

st.divider()


if total:
    q99 = df["Время_сек"].quantile(0.99)
    fig = px.histogram(df.query("Время_сек <= @q99"),
                       x="Время_сек", nbins=20,
                       title="Распределение времени ответа",
                       labels={"Время_сек":"время, с","count":"Количество"})
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Пользователи")
    perf = (df.groupby("Пользователь")
              .agg(Ответов=("qnum","count"),
                   Точность=("is_correct","mean"),
                   Ср_время=("Время_сек","mean"))
              .reset_index())
    perf["Точность"] = perf["Точность"].mul(100).round(1)
    perf["Ср_время"] = perf["Ср_время"].round(2)
    st.dataframe(perf, use_container_width=True)
else:
    st.info("Нет данных под выбранные фильтры.")


st.subheader("Данные")
st.download_button("💾 Скачать CSV",
                   df.to_csv(index=False).encode(),
                   "human_study_results.csv",
                   "text/csv",
                   disabled=not total)
st.dataframe(df, use_container_width=True, height=500)

