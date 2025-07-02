 from __future__ import annotations
import streamlit as st, pandas as pd, plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from streamlit_autorefresh import st_autorefresh

st.set_page_config("Аналитика исследования", "📊", layout="wide")
REQ_ANS, REFRESH_SEC = 40, 30
st_autorefresh(interval=REFRESH_SEC*1000, key="auto")

tab1, tab2 = st.tabs([
    "Этап 1: 40 вопросов",
    "Этап 2: 15 вопросов"
])


def highlight_max(v, top="#2ECC71", base="#1f77b4"):
    m = max(v) if len(v) else None
    return [top if x == m else base for x in v]


@st.cache_data(ttl=REFRESH_SEC, show_spinner="Обновляю данные…")
def load_sheet() -> pd.DataFrame:
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        dict(st.secrets["gsp"]), scopes)
    raw = gspread.authorize(creds).open("human_study_results").sheet1.get_all_values()
    base = ["timestamp","Пользователь","qnum","image_id","Алгоритм","Тип",
            "Вопрос","Ответ","Правильный_ответ","time_ms","is_correct","session_id"]
    if not raw:
        return pd.DataFrame(columns=base)
    if raw[0][:3] == base[:3]:
        raw = raw[1:]
    df = pd.DataFrame(raw, columns=base[:len(max(raw, key=len))])
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])
    df["time_ms"]  = pd.to_numeric(df["time_ms"], errors="coerce")
    df["Время_сек"] = df["time_ms"] / 1000
    df["is_correct"] = df["is_correct"].astype(str).str.strip() \
                           .str.upper().isin(["TRUE","1","YES"])
    full = df.groupby("Пользователь")["qnum"].count() \
             .pipe(lambda s: s[s == REQ_ANS]).index
    return df[df["Пользователь"].isin(full)]


@st.cache_data(ttl=REFRESH_SEC, show_spinner="Загружаю данные второго этапа…")
def load_stage2() -> pd.DataFrame:
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(
        dict(st.secrets["gsp"]), scopes)
    ws = gspread.authorize(creds) \
           .open("human_study_results") \
           .worksheet("stage2_log")
    raw = ws.get_all_records()
    df2 = pd.DataFrame(raw)
    df2["timestamp"]  = pd.to_datetime(df2["timestamp"], errors="coerce")
    df2["time_ms"]    = pd.to_numeric(df2["time_ms"], errors="coerce")
    df2["Время_сек"]  = df2["time_ms"] / 1000
    df2["is_correct"] = pd.Series(df2["is_correct"]).astype(str) \
                             .str.strip().str.upper().isin(["TRUE","1","YES"])
    return df2


with tab1:
    df_raw = load_sheet()
    if df_raw.empty:
        st.warning("Нет пользователей, прошедших тест полностью.")
        st.stop()


    st.sidebar.header("Фильтры")
    users = st.sidebar.multiselect("Пользователь", sorted(df_raw["Пользователь"].unique()))
    meth  = st.sidebar.multiselect("Алгоритм",     sorted(df_raw["Алгоритм"].unique()))
    ques  = st.sidebar.multiselect("Вопрос",       sorted(df_raw["Вопрос"].unique()))
    pics  = st.sidebar.multiselect("Изображение",  sorted(df_raw["image_id"].unique()))
    dmin, dmax = df_raw["timestamp"].dt.date.agg(["min","max"])
    d_from = st.sidebar.date_input("Дата от", dmin)
    d_to   = st.sidebar.date_input("Дата до", dmax)

    mask = pd.Series(True, index=df_raw.index)
    if users: mask &= df_raw["Пользователь"].isin(users)
    if meth:  mask &= df_raw["Алгоритм"].isin(meth)
    if ques:  mask &= df_raw["Вопрос"].isin(ques)
    if pics:  mask &= df_raw["image_id"].isin(pics)
    mask &= (df_raw["timestamp"].dt.date >= d_from) & (df_raw["timestamp"].dt.date <= d_to)
    df = df_raw[mask]


    tot = len(df)
    corr = df["is_correct"].mean()*100 if tot else 0
    mean_t = df["Время_сек"].mean() if tot else 0
    med_t  = df["Время_сек"].median() if tot else 0
    dont   = df["Ответ"].str.lower().str.startswith("затруд").sum()
    a,b,c,d,e = st.columns(5)
    a.metric("Всего ответов",   f"{tot:,}".replace(',',' '))
    b.metric("Корректность",    f"{corr:.1f}%")
    c.metric("Среднее время",   f"{mean_t:.2f} с")
    d.metric("Медианное время", f"{med_t:.2f} с")
    e.metric("«Затрудняюсь»",   f"{dont:,}".replace(',',' '))
    st.divider()

    if not tot:
        st.info("Нет данных под выбранные фильтры.")
        st.stop()


    q99 = df["Время_сек"].quantile(0.99)
    st.plotly_chart(
        px.histogram(df.query("Время_сек<=@q99"), x="Время_сек", nbins=20,
                     title="Распределение времени ответа",
                     labels={"Время_сек":"Время, с","count":"Количество"}),
        use_container_width=True
    )

    st.subheader("Буквенные вопросы: средняя точность первого показа по алгоритмам")
    letters = df[df["Тип"]=="letters"]
    stat_letters1 = pd.DataFrame()
    if not letters.empty:
        first = (letters.sort_values("timestamp")
                        .groupby(["Пользователь","image_id"], as_index=False)
                        .first())
        stat_letters1 = (first.groupby("Алгоритм")
                              .agg(Пользователей=("Пользователь","count"),
                                   Точность=("is_correct","mean"))
                              .reset_index())
        stat_letters1["Точность"]=(stat_letters1["Точность"]*100).round(1)

        fig_letters1 = px.bar(stat_letters1, x="Алгоритм", y="Точность",
                              text="Пользователей",
                              title="Этап 1",
                              labels={"Точность":"Точность, %","Пользователей":"Пользователей"})
        fig_letters1.update_traces(marker_color=highlight_max(stat_letters1["Точность"]))
        st.plotly_chart(fig_letters1, use_container_width=True)
        st.dataframe(stat_letters1, use_container_width=True)
    else:
        st.info("В данных нет вопросов типа «буквы».")


    df2_all = load_stage2()
    cnt_ok  = df2_all.groupby("user")["qnum"].count()
    df2     = df2_all[df2_all["user"].isin(cnt_ok[cnt_ok==15].index)]
    df_l2   = df2[df2["qtype"]=="letters"].sort_values("timestamp")
    df_l2   = df_l2.groupby(["user","group"], as_index=False).first()
    stat_letters2 = (df_l2.groupby("alg")
                           .agg(Пользователей=("user","nunique"),
                                Точность=("is_correct","mean"))
                           .reset_index())
    stat_letters2["Точность"] = (stat_letters2["Точность"]*100).round(1)

    if not stat_letters1.empty and not stat_letters2.empty:
        comb = pd.merge(stat_letters1[["Алгоритм","Точность"]],
                        stat_letters2[["alg","Точность"]].rename(columns={"alg":"Алгоритм","Точность":"Точность_2"}),
                        on="Алгоритм", how="outer") \
                   .rename(columns={"Точность":"Точность_1"})
        comb = comb.fillna(0)
        melt = comb.melt(id_vars="Алгоритм",
                         value_vars=["Точность_1","Точность_2"],
                         var_name="Этап", value_name="Точность")
        melt["Этап"] = melt["Этап"].map({"Точность_1":"Этап 1","Точность_2":"Этап 2"})
        fig_cmp = px.bar(melt, x="Алгоритм", y="Точность", color="Этап",
                         barmode="group", text="Точность",
                         title="Сравнение точности буквенных вопросов (Этап 1 vs Этап 2)",
                         labels={"Алгоритм":"Алгоритм","Точность":"Точность, %"})
        st.plotly_chart(fig_cmp, use_container_width=True)


with tab2:
    df2_all = load_stage2()
    if df2_all.empty:
        st.warning("Нет данных второго этапа.")
        st.stop()

    cnt2   = df2_all.groupby("user")["qnum"].count()
    users2 = cnt2[cnt2 == 15].index
    df2    = df2_all[df2_all["user"].isin(users2)]


    tot2   = len(df2)
    corr2  = df2["is_correct"].mean()*100 if tot2 else 0
    mean2  = df2["Время_сек"].mean() if tot2 else 0
    med2   = df2["Время_сек"].median() if tot2 else 0
    dont2  = df2["answer"].astype(str).str.lower().str.startswith("затруд").sum()
    a,b,c,d,e = st.columns(5)
    a.metric("Всего ответов",   f"{tot2:,}".replace(',',' '))
    b.metric("Корректность",    f"{corr2:.1f}%")
    c.metric("Среднее время",   f"{mean2:.2f} с")
    d.metric("Медианное время", f"{med2:.2f} с")
    e.metric("«Затрудняюсь»",   f"{dont2:,}".replace(',',' '))


    df_l2 = df2[df2["qtype"]=="letters"].sort_values("timestamp")
    df_l2 = df_l2.groupby(["user","group"], as_index=False).first()
    stat_l2 = (df_l2.groupby("alg")
                     .agg(Пользователей=("user","nunique"),
                          Точность=("is_correct","mean"))
                     .reset_index())
    stat_l2["Точность"] = (stat_l2["Точность"]*100).round(1)

    st.subheader("Буквенные вопросы: второй этап")
    fig_l2 = px.bar(stat_l2, x="alg", y="Точность", text="Пользователей",
                    title="Точность ответов на буквенные вопросы",
                    labels={"alg":"Алгоритм","Точность":"Точность, %","Пользователей":"Уникальных пользователей"})
    fig_l2.update_traces(marker_color=highlight_max(stat_l2["Точность"]))
    st.plotly_chart(fig_l2, use_container_width=True)
    st.dataframe(stat_l2, use_container_width=True)


    df_c2 = df2[df2["qtype"]=="corners"]
    df_c2 = df_c2[df_c2["alg"].isin(["socolov_lab_result","socolov_rgb_result"])]
    stat_c2 = (df_c2.groupby("alg")
                     .agg(Ответов=("user","count"),
                          Точность=("is_correct","mean"))
                     .reset_index())
    stat_c2["Точность"] = (stat_c2["Точность"]*100).round(1)

    st.subheader("Вопросы про углы: второй этап")
    fig_c2 = px.bar(stat_c2, x="alg", y="Точность", text="Ответов",
                    title="Точность ответов по угловым вопросам",
                    labels={"alg":"Алгоритм","Точность":"Точность, %","Ответов":"Количество ответов"})
    fig_c2.update_traces(marker_color=highlight_max(stat_c2["Точность"]))
    st.plotly_chart(fig_c2, use_container_width=True)
    st.dataframe(stat_c2, use_container_width=True)


