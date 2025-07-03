from __future__ import annotations
import streamlit as st, pandas as pd, plotly.express as px
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from streamlit_autorefresh import st_autorefresh

st.set_page_config("Аналитика исследования", "📊", layout="wide")
REQ_ANS, REFRESH_SEC = 40, 30
st_autorefresh(interval=REFRESH_SEC*1000, key="auto")

tab1, tab2 = st.tabs(["Этап 1: 40 вопросов", "Этап 2: 15 вопросов"])


def highlight_max(v, top="#2ECC71", base="#1f77b4"):
    m = max(v) if len(v) else None
    return [top if x == m else base for x in v]


@st.cache_data(ttl=REFRESH_SEC, show_spinner="Обновляю данные…")
def load_stage1() -> pd.DataFrame:
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(st.secrets["gsp"]), scopes)
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
    df["time_ms"] = pd.to_numeric(df["time_ms"], errors="coerce")
    df["Время_сек"] = df["time_ms"] / 1000
    df["is_correct"] = df["is_correct"].astype(str).str.strip().str.upper().isin(["TRUE","1","YES"])
    full = df.groupby("Пользователь")["qnum"].count().pipe(lambda s: s[s == REQ_ANS]).index
    return df[df["Пользователь"].isin(full)]


@st.cache_data(ttl=REFRESH_SEC, show_spinner="Загружаю данные второго этапа…")
def load_stage2() -> pd.DataFrame:
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(dict(st.secrets["gsp"]), scopes)
    raw = gspread.authorize(creds).open("human_study_results").worksheet("stage2_log").get_all_records()
    df = pd.DataFrame(raw)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["time_ms"] = pd.to_numeric(df["time_ms"], errors="coerce")
    df["Время_сек"] = df["time_ms"] / 1000
    df["is_correct"] = df["is_correct"].astype(str).str.strip().str.upper().isin(["TRUE","1","YES"])
    return df


with tab1:
    df_raw = load_stage1()
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
    med_t = df["Время_сек"].median() if tot else 0
    dont = df["Ответ"].astype(str).str.lower().str.startswith("затруд").sum()
    a,b,c,d,e = st.columns(5)
    a.metric("Всего ответов", f"{tot:,}".replace(',',' '))
    b.metric("Корректность", f"{corr:.1f}%")
    c.metric("Среднее время", f"{mean_t:.2f} с")
    d.metric("Медианное время", f"{med_t:.2f} с")
    e.metric("«Затрудняюсь»", f"{dont:,}".replace(',',' '))
    st.divider()

    q99 = df["Время_сек"].quantile(0.99)
    st.plotly_chart(px.histogram(df.query("Время_сек<=@q99"), x="Время_сек", nbins=20,
                                 title="Распределение времени ответа",
                                 labels={"Время_сек":"Время, с","count":"Количество"}),
                    use_container_width=True)

    st.subheader("Пользователи")
    perf = (df.groupby("Пользователь")
              .agg(Ответов=("qnum","count"), Точность=("is_correct","mean"),
                   Ср_время=("Время_сек","mean"),
                   Затрудняюсь=("Ответ", lambda s:(s.str.lower().str.startswith("затруд")).sum()))
              .reset_index())
    perf["Точность"] = (perf["Точность"]*100).round(1)
    perf["Ср_время"] = perf["Ср_время"].round(2)
    st.dataframe(perf, use_container_width=True)

    st.subheader("Статистика по алгоритмам")
    alg = (df.groupby("Алгоритм")
             .agg(Ответов=("qnum","count"), Точность=("is_correct","mean"),
                  Ср_время=("Время_сек","mean"),
                  Затрудняюсь=("Ответ", lambda s:(s.str.lower().str.startswith("затруд")).sum()))
             .reset_index())
    alg["Точность"] = (alg["Точность"]*100).round(1)
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
    pic = (df.groupby("image_id")
             .agg(Ответов=("qnum","count"), Точность=("is_correct","mean"),
                  Ср_время=("Время_сек","mean"),
                  Затрудняюсь=("Ответ", lambda s:(s.str.lower().str.startswith("затруд")).sum()))
             .reset_index())
    pic["Точность"] = (pic["Точность"]*100).round(1)
    pic["Ср_время"] = pic["Ср_время"].round(2)
    st.dataframe(pic, use_container_width=True, height=350)

    st.subheader("Буквенные вопросы: средняя точность первого показа по алгоритмам")
    letters1 = df[df["Тип"]=="letters"]
    if not letters1.empty:
        first1 = (letters1.sort_values("timestamp")
                           .groupby(["Пользователь","image_id"], as_index=False)
                           .first())
        stat1 = (first1.groupby("Алгоритм")
                        .agg(Пользователей=("Пользователь","count"),
                             Точность=("is_correct","mean"))
                        .reset_index())
        stat1["Точность"] = (stat1["Точность"]*100).round(1)
        fig_letters1 = px.bar(stat1, x="Алгоритм", y="Точность", text="Пользователей",
                              title="Средняя точность (Этап 1)",
                              labels={"Точность":"Точность, %","Пользователей":"Пользователей"})
        fig_letters1.update_traces(marker_color=highlight_max(stat1["Точность"]))
        st.plotly_chart(fig_letters1, use_container_width=True)
        st.dataframe(stat1, use_container_width=True)
    else:
        stat1 = pd.DataFrame()
        st.info("В данных нет вопросов типа «буквы» для этапа 1.")

    df2_all = load_stage2()
    full2 = df2_all.groupby("user")["qnum"].count()
    df2 = df2_all[df2_all["user"].isin(full2[full2==15].index)]
    letters2 = df2[df2["qtype"]=="letters"].sort_values("timestamp") \
                                           .groupby(["user","group"], as_index=False) \
                                           .first()
    stat2 = (letters2.groupby("alg")
                     .agg(Пользователей=("user","nunique"),
                          Точность=("is_correct","mean"))
                     .reset_index())
    stat2["Точность"] = (stat2["Точность"]*100).round(1)

    if not stat1.empty and not stat2.empty:
        cmp = (pd.merge(stat1[["Алгоритм","Точность"]],
                        stat2.rename(columns={"alg":"Алгоритм"})[["Алгоритм","Точность"]],
                        on="Алгоритм", how="outer", suffixes=("_1","_2"))
                .fillna(0))
        melt = cmp.melt(id_vars="Алгоритм", var_name="Этап", value_name="Точность")
        melt["Этап"] = melt["Этап"].map({"Точность_1":"Этап 1","Точность_2":"Этап 2"})
        fig_cmp = px.bar(melt, x="Алгоритм", y="Точность", color="Этап",
                         barmode="group", text="Точность",
                         title="Буквенные вопросы: сравнение точности (Этап 1 vs Этап 2)",
                         labels={"Алгоритм":"Алгоритм","Точность":"Точность, %"})
        st.plotly_chart(fig_cmp, use_container_width=True)

        comb_letters = pd.concat([
            first1.rename(columns={"Алгоритм":"alg"})[["alg","is_correct"]],
            letters2[["alg","is_correct"]]
        ], ignore_index=True).rename(columns={"alg":"Алгоритм"})
        comb_stat = (comb_letters.groupby("Алгоритм")
                                 .agg(Экспозиций=("is_correct","count"),
                                      Точность=("is_correct","mean"))
                                 .reset_index())
        comb_stat["Точность"] = (comb_stat["Точность"]*100).round(1)
        st.subheader("Буквенные вопросы: суммарная точность двух этапов")
        fig_tot = px.bar(comb_stat, x="Алгоритм", y="Точность", text="Экспозиций",
                         title="Суммарная точность (Этап 1 + Этап 2)",
                         labels={"Точность":"Точность, %","Экспозиций":"Экспозиций"})
        fig_tot.update_traces(marker_color=highlight_max(comb_stat["Точность"]))
        st.plotly_chart(fig_tot, use_container_width=True)
        st.dataframe(comb_stat, use_container_width=True)

    csv = df.to_csv(index=False).encode("utf-8-sig")
    st.subheader("Данные")
    st.download_button("💾 Скачать CSV", csv, "human_study_results.csv", "text/csv")
    cols = ["timestamp","Пользователь","qnum","image_id","Алгоритм","Тип",
            "Вопрос","Ответ","Правильный_ответ","Время_сек","is_correct","session_id"]
    st.dataframe(df[[c for c in cols if c in df.columns]], use_container_width=True, height=500)
    st.caption(f"Данные обновляются каждые {REFRESH_SEC} секунд")


with tab2:
    df2_all = load_stage2()
    if df2_all.empty:
        st.warning("Нет данных второго этапа.")
        st.stop()

    full2 = df2_all.groupby("user")["qnum"].count()
    df2 = df2_all[df2_all["user"].isin(full2[full2==15].index)]

    tot2 = len(df2)
    corr2 = df2["is_correct"].mean()*100 if tot2 else 0
    mean2 = df2["Время_сек"].mean() if tot2 else 0
    med2 = df2["Время_сек"].median() if tot2 else 0
    dont2 = df2["answer"].astype(str).str.lower().str.startswith("затруд").sum()
    a,b,c,d,e = st.columns(5)
    a.metric("Всего ответов", f"{tot2:,}".replace(',',' '))
    b.metric("Корректность", f"{corr2:.1f}%")
    c.metric("Среднее время", f"{mean2:.2f} с")
    d.metric("Медианное время", f"{med2:.2f} с")
    e.metric("«Затрудняюсь»", f"{dont2:,}".replace(',',' '))
    st.divider()

    letters2 = df2[df2["qtype"]=="letters"].sort_values("timestamp") \
                                           .groupby(["user","group"], as_index=False) \
                                           .first()
    stat_l2 = (letters2.groupby("alg")
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


