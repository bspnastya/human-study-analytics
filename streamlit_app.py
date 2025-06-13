from __future__ import annotations
import streamlit as st, pandas as pd, plotly.express as px, time
import gspread
from oauth2client.service_account import ServiceAccountCredentials


st.set_page_config("Аналитика эксперимента", "📊", layout="wide")
REFRESH_SEC = 30                      

@st.cache_data(ttl=REFRESH_SEC, show_spinner="Обновляю данные…")
def load_sheet() -> pd.DataFrame:
    """Читаем Google Sheets и превращаем в чистый DataFrame."""
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

    df["time_ms"]   = pd.to_numeric(df["time_ms"], errors="coerce")
    df["Время_сек"] = df["time_ms"] / 1000
    df["is_correct"]= df["is_correct"].astype(str).str.upper().eq("TRUE")

    return df


df = load_sheet()
if df.empty:
    st.warning("В таблице пока нет корректных строк.")
    st.stop()


st.sidebar.header("Фильтры")

users   = st.sidebar.multiselect(
    "Пользователь", sorted(df["Пользователь"].unique()))
methods = st.sidebar.multiselect(
    "Алгоритм", sorted(df["Алгоритм"].unique()))
date1   = st.sidebar.date_input("Дата от", df["timestamp"].min().date())
date2   = st.sidebar.date_input("Дата до", df["timestamp"].max().date())

df = df.query(
    "(@users==[] or Пользователь in @users) and "
    "(@methods==[] or Алгоритм in @methods) and "
    "timestamp.dt.date >= @date1 and timestamp.dt.date <= @date2"
)


a, b, c, d = st.columns(4)
a.metric("Всего ответов", f"{len(df):,}".replace(',', ' '))
b.metric("Корректность",  f"{df['is_correct'].mean()*100:.1f}%")
c.metric("Среднее время", f"{df['Время_сек'].mean():.2f} с")
d.metric("Медианное время", f"{df['Время_сек'].median():.2f} с")

st.divider()


q99 = df["Время_сек"].quantile(0.99)
st.plotly_chart(
    px.histogram(
        df.query("Время_сек <= @q99"),
        x="Время_сек", nbins=20,
        title="Распределение времени ответа",
        labels={"Время_сек": "время, с", "count": "Кол-во"}),
    use_container_width=True)

perf = (df.groupby("Пользователь")
          .agg(Ответов=("qnum", "count"),
               Точность=("is_correct", "mean"),
               Ср_время=("Время_сек", "mean"))
          .reset_index())
perf["Точность"] = perf["Точность"].mul(100).round(1)
perf["Ср_время"] = perf["Ср_время"].round(2)

st.subheader("Пользователи")
st.dataframe(perf, use_container_width=True)


st.subheader("Данные")
st.download_button(
    "💾 Скачать CSV",
    df.to_csv(index=False).encode(),
    "human_study_results.csv",
    "text/csv")
st.dataframe(df, use_container_width=True, height=500)


time.sleep(REFRESH_SEC)
st.experimental_rerun()
