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

with tab1:
    @st.cache_data(ttl=REFRESH_SEC, show_spinner="Обновляю данные…")
    def load_sheet() -> pd.DataFrame:
        scopes = ["https://spreadsheets.google.com/feeds",
                  "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_dict(
            dict(st.secrets["gsp"]), scopes)
        raw = gspread.authorize(creds) \
                 .open("human_study_results") \
                 .sheet1.get_all_values()
        base = ["timestamp","Пользователь","qnum","image_id","Алгоритм","Тип",
                "Вопрос","Ответ","Правильный_ответ","time_ms","is_correct","session_id"]
        if not raw:
            return pd.DataFrame(columns=base)
        if raw[0][:3] == base[:3]:
            raw = raw[1:]
        df = pd.DataFrame(raw, columns=base[:len(max(raw, key=len))])
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        df = df.dropna(subset=["timestamp"])
        df["time_ms"] = pd.to_numeric(df["time_ms"], errors="coerce")
        df["Время_сек"] = df["time_ms"] / 1000
        df["is_correct"] = df["is_correct"].astype(str).str.strip()\
                              .str.upper().isin(["TRUE","1","YES"])
        full = df.groupby("Пользователь")["qnum"].count()\
                 .pipe(lambda s: s[s == REQ_ANS]).index
        return df[df["Пользователь"].isin(full)]

    def highlight_max(v, top="#2ECC71", base="#1f77b4"):
        m = max(v) if len(v) else None
        return [top if x == m else base for x in v]

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
    corr = df["is_correct"].mean() * 100 if tot else 0
    mean_t = df["Время_сек"].mean() if tot else 0
    med_t  = df["Время_сек"].median() if tot else 0
    dont   = df["Ответ"].str.lower().str.startswith("затруд").sum()
    a,b,c,d,e = st.columns(5)
    a.metric("Всего ответов",      f"{tot:,}".replace(',',' '))
    b.metric("Корректность",       f"{corr:.1f}%")
    c.metric("Среднее время",      f"{mean_t:.2f} с")
    d.metric("Медианное время",    f"{med_t:.2f} с")
    e.metric("«Затрудняюсь»",      f"{dont:,}".replace(',',' '))
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

    st.subheader("Пользователи")
    perf = (
        df.groupby("Пользователь")
          .agg(Ответов=("qnum","count"),Точность=("is_correct","mean"),
               Ср_время=("Время_сек","mean"),
               Затрудняюсь=("Ответ",lambda s:(s.str.lower().str.startswith("затруд")).sum()))
          .reset_index()
    )
    perf["Точность"]   = (perf["Точность"] * 100).round(1)
    perf["Ср_время"]   = perf["Ср_время"].round(2)
    st.dataframe(perf, use_container_width=True)

    st.subheader("Статистика по алгоритмам")
    alg = (
        df.groupby("Алгоритм")
          .agg(Ответов=("qnum","count"),Точность=("is_correct","mean"),
               Ср_время=("Время_сек","mean"),
               Затрудняюсь=("Ответ",lambda s:(s.str.lower().str.startswith("затруд")).sum()))
          .reset_index()
    )
    alg["Точность"] = (alg["Точность"] * 100).round(1)
    alg["Ср_время"] = alg["Ср_время"].round(2)

    fig_alg = px.bar(alg, x="Алгоритм", y="Точность",
                     title="Точность ответов по алгоритмам",
                     labels={"Точность":"Точность, %"})
    fig_alg.update_traces(marker_color=highlight_max(alg["Точность"]))
    st.plotly_chart(fig_alg, use_container_width=True)

    fig_dz = px.bar(alg, x="Алгоритм", y="Затрудняюсь",
                    title="«Затрудняюсь» по алгоритмам",
                    labels={"Затрудняюсь":"Кол-во"})
    fig_dz.update_traces(marker_color=highlight_max(alg["Затрудняюсь"]))
    st.plotly_chart(fig_dz, use_container_width=True)
    st.dataframe(alg, use_container_width=True)

    st.subheader("Статистика по изображениям")
    pic = (
        df.groupby("image_id")
          .agg(Ответов=("qnum","count"),Точность=("is_correct","mean"),
               Ср_время=("Время_сек","mean"),
               Затрудняюсь=("Ответ",lambda s:(s.str.lower().str.startswith("затруд")).sum()))
          .reset_index()
    )
    pic["Точность"] = (pic["Точность"] * 100).round(1)
    pic["Ср_время"] = pic["Ср_время"].round(2)
    st.dataframe(pic, use_container_width=True, height=350)

    st.subheader("Буквенные вопросы: средняя точность первого показа по алгоритмам")
    letters = df[df["Тип"] == "letters"]
    if not letters.empty:
        letters = letters.assign(
            Правильный_ответ=lambda d: d["Правильный_ответ"].str.strip().str.lower()
        )
        first = (
            letters.sort_values("timestamp")
                   .groupby(["Пользователь","Правильный_ответ"], as_index=False)
                   .first()
        )
        stat = (
            first.groupby("Алгоритм")
                 .agg(Пользователей=("Пользователь","count"),
                      Точность=("is_correct","mean"))
                 .reset_index()
        )
        stat["Точность"] = (stat["Точность"] * 100).round(1)
        fig_letters = px.bar(stat, x="Алгоритм", y="Точность", text="Пользователей",
                             title="Средняя точность по первому вхождению",
                             labels={"Точность":"Точность, %","Пользователей":"Пользователей"})
        fig_letters.update_traces(marker_color=highlight_max(stat["Точность"]))
        st.plotly_chart(fig_letters, use_container_width=True)
        st.dataframe(stat, use_container_width=True)
    else:
        st.info("В данных нет вопросов типа «буквы».")

    st.subheader("Данные")
    csv = df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("💾 Скачать CSV", csv, "human_study_results.csv", "text/csv")
    cols = ["timestamp","Пользователь","qnum","image_id","Алгоритм","Тип",
            "Вопрос","Ответ","Правильный_ответ","Время_сек","is_correct","session_id"]
    st.dataframe(df[[c for c in cols if c in df.columns]],
                 use_container_width=True, height=500)

    st.caption(f"Данные обновляются каждые {REFRESH_SEC} секунд")

with tab2:
    st.header("Этап 2: 15 вопросов")

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
        df2["is_correct"] = pd.Series(df2["is_correct"]).astype(str)\
                                 .str.strip().str.upper().isin(["TRUE","1","YES"])
        return df2

    df2 = load_stage2()
    if df2.empty:
        st.warning("Нет данных второго этапа.")
        st.stop()


    cnt2   = df2.groupby("user")["qnum"].count()
    users2 = cnt2[cnt2 == 15].index
    df2    = df2[df2["user"].isin(users2)]

 
    df_l2 = df2[df2["qtype"] == "letters"].sort_values("timestamp")
    df_l2 = df_l2.groupby(["user","group"], as_index=False).first()
    stat_l2 = (
        df_l2.groupby("alg")
             .agg(Пользователей=("user","nunique"),
                  Точность=("is_correct","mean"))
             .reset_index()
    )
    stat_l2["Точность"] = (stat_l2["Точность"] * 100).round(1)

    st.subheader("Буквенные вопросы: второй этап")
    fig_l2 = px.bar(stat_l2, x="alg", y="Точность", text="Пользователей",
                    title="Точность ответов на буквенные вопросы",
                    labels={"alg":"Алгоритм","Точность":"Точность, %","Пользователей":"Уникальных пользователей"})
    fig_l2.update_traces(marker_color=highlight_max(stat_l2["Точность"]))
    st.plotly_chart(fig_l2, use_container_width=True)
    st.dataframe(stat_l2, use_container_width=True)


    df_c2 = df2[df2["qtype"] == "corners"]
    df_c2 = df_c2[df_c2["alg"].isin(["socolov_lab_result","socolov_rgb_result"])]
    stat_c2 = (
        df_c2.groupby("alg")
             .agg(Ответов=("user","count"),
                  Точность=("is_correct","mean"))
             .reset_index()
    )
    stat_c2["Точность"] = (stat_c2["Точность"] * 100).round(1)

    st.subheader("Вопросы про углы: второй этап")
    fig_c2 = px.bar(stat_c2, x="alg", y="Точность", text="Ответов",
                    title="Точность ответов по угловым вопросам",
                    labels={"alg":"Алгоритм","Точность":"Точность, %","Ответов":"Количество ответов"})
    fig_c2.update_traces(marker_color=highlight_max(stat_c2["Точность"]))
    st.plotly_chart(fig_c2, use_container_width=True)
    st.dataframe(stat_c2, use_container_width=True)

