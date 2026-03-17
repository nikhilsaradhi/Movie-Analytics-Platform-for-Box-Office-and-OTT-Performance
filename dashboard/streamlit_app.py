import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.express as px
import os
from dotenv import load_dotenv


# --------------------------------------------------
# PAGE CONFIG
# --------------------------------------------------

st.set_page_config(
    page_title="Movie Analytics Platform",
    layout="wide"
)

# --------------------------------------------------
# CUSTOM CSS
# --------------------------------------------------

st.markdown("""
<style>

/* ---------------- GLOBAL COLORS ---------------- */

:root{
--sidebar-bg: linear-gradient(180deg,#1b3b6f 0%,#0f2748 50%,#071427 100%);
--main-bg: radial-gradient(circle at top,#0f2d57 0%,#071a33 60%,#030c19 100%);
--panel-bg: rgba(255,255,255,0.06);
--panel-border: rgba(255,255,255,0.12);
--text-main:#f3f8ff;
--text-muted:#9bb5d4;
--accent:#4fb3ff;
}

/* ---------------- MAIN BACKGROUND ---------------- */

[data-testid="stAppViewContainer"]{
background:var(--main-bg);
}

/* ---------------- SIDEBAR ---------------- */

[data-testid="stSidebar"]{
background:var(--sidebar-bg);
border-right:1px solid rgba(255,255,255,0.08);
padding-top:15px;
}

/* Sidebar Text */

[data-testid="stSidebar"] *{
color:#e7f3ff;
}

/* Sidebar Header */

[data-testid="stSidebar"] h2{
font-size:22px;
font-weight:700;
margin-bottom:20px;
}

/* ---------------- FILTER LABELS ---------------- */

[data-testid="stSidebar"] label{
font-weight:800;
font-size:40px;
color:#cfe6ff;
margin-top:6px;
}

/* ---------------- FILTER DROPDOWNS ---------------- */

[data-testid="stSidebar"] [data-baseweb="select"]{

background:rgba(0,0,0,0.45);
border-radius:12px;
border:1px solid rgba(255,255,255,0.08);
padding:4px;

transition: all 0.25s ease;

}

/* Hover Effect */

[data-testid="stSidebar"] [data-baseweb="select"]:hover{

border:1px solid var(--accent);
box-shadow:0 0 10px rgba(79,179,255,0.5);

}

/* Selected Value */

[data-testid="stSidebar"] [data-baseweb="select"] div{
color:#eaf4ff;
}

/* Dropdown menu */

[data-baseweb="popover"]{

background:#0c203f;
border-radius:10px;

}

/* ---------------- PAGE SPACING ---------------- */

.block-container{
padding-top:2rem;
}

/* ---------------- TEXT COLORS ---------------- */

h1,h2,h3,h4,p{
color:var(--text-main);
}

/* ---------------- KPI CARDS ---------------- */

[data-testid="stMetric"]{

background:linear-gradient(
180deg,
rgba(255,255,255,0.12),
rgba(255,255,255,0.04)
);

border:1px solid var(--panel-border);

border-radius:18px;

padding:18px;

box-shadow:0 10px 35px rgba(0,0,0,0.35);

}

/* ---------------- CHART PANELS ---------------- */

[data-testid="stPlotlyChart"]{

background:var(--panel-bg);

border:1px solid var(--panel-border);

border-radius:20px;

padding:8px;

box-shadow:0 12px 35px rgba(0,0,0,0.35);

}

/* ---------------- SIDEBAR SCROLLBAR ---------------- */

[data-testid="stSidebar"]::-webkit-scrollbar{
width:6px;
}

[data-testid="stSidebar"]::-webkit-scrollbar-thumb{
background:#4fb3ff;
border-radius:10px;
}
            
/* KPI CARD CENTER ALIGN */
[data-testid="stMetric"]{
text-align:center;
}

[data-testid="stMetricLabel"]{
font-size:18px;
font-weight:600;
}

[data-testid="stMetricValue"]{
font-size:36px;
font-weight:800;
text-align:center;
}

</style>
""", unsafe_allow_html=True)

# --------------------------------------------------
# HEADER
# --------------------------------------------------

st.markdown("""
<h1 style='text-align:center;font-size:48px;font-weight:800'>
 MOVIES ANALYTICS PLATFORM
</h1>

<h4 style='text-align:center;color:#9bb5d4'>
BOX OFFICE AND OTT PERFORMANCE INSIGHTS
</h4>
""", unsafe_allow_html=True)

load_dotenv()

conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse="SNOWFLAKE_LEARNING_WH",
        database="MOVIE_ANALYTICS",
        schema="GOLD"
    )

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

@st.cache_data
def load_data():

    

    query = "SELECT * FROM VW_MOVIE_ANALYTICS"

    df = pd.read_sql(query, conn)

    

    return df


df = load_data()
df.columns = df.columns.str.lower()

# --------------------------------------------------
# DATA CLEANING
# --------------------------------------------------

df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
df["worldwide_revenue"] = pd.to_numeric(df["worldwide_revenue"], errors="coerce")
df["imdb_rating"] = pd.to_numeric(df["imdb_rating"], errors="coerce")
df["popularity"] = pd.to_numeric(df["popularity"], errors="coerce")

df = df.dropna(subset=["release_year"])
df["release_year"] = df["release_year"].astype(int)

df["genres"] = df["genres"].fillna("").str.strip()

filtered = df.copy()

# --------------------------------------------------
# SIDEBAR FILTERS
# --------------------------------------------------

st.sidebar.markdown("## 🔎 Analytics Filters")

# Genre
genres_list = (
    df["genres"].str.split(",")
    .explode()
    .str.strip()
)

genres_list = genres_list[genres_list!=""]
genres_list = sorted(genres_list.unique())

genre = st.sidebar.selectbox("GENRES",["All"] + genres_list)

# Country
countries = sorted(df["country"].dropna().unique())
country = st.sidebar.selectbox("COUNTRY",["All"] + countries)

# Studio
studios = sorted(df["studio_name"].dropna().unique())
studio = st.sidebar.selectbox("STUDIOS",["All"] + studios)

# OTT
ott_platform = st.sidebar.selectbox(
"OTT PLATFORM",
["All","Netflix","Hulu","Prime Video","Disney+"]
)

# Year
years = sorted(df["release_year"].unique())

year_from = st.sidebar.selectbox("From Year",years)
year_to = st.sidebar.selectbox("To Year",years,index=len(years)-1)

# Movie Search
movie_list = sorted(df["title"].dropna().unique())

movie_search = st.sidebar.selectbox(
"SEARCH MOVIES",
["All"] + movie_list
)

# --------------------------------------------------
# APPLY FILTERS
# --------------------------------------------------

if genre!="All":
    filtered = filtered[
        filtered["genres"].str.contains(genre,case=False,na=False)
    ]

if country!="All":
    filtered = filtered[filtered["country"]==country]

if studio!="All":
    filtered = filtered[filtered["studio_name"]==studio]

if ott_platform=="Netflix":
    filtered = filtered[filtered["netflix"]==1]

elif ott_platform=="Hulu":
    filtered = filtered[filtered["hulu"]==1]

elif ott_platform=="Prime Video":
    filtered = filtered[filtered["prime_video"]==1]

elif ott_platform=="Disney+":
    filtered = filtered[filtered["disney_plus"]==1]

filtered = filtered[
    (filtered["release_year"]>=year_from) &
    (filtered["release_year"]<=year_to)
]

if movie_search!="All":
    filtered = filtered[filtered["title"]==movie_search]

# Remove duplicates

movie_df=filtered.drop_duplicates("movie_id")

# --------------------------------------------------
# KPI CALCULATIONS
# --------------------------------------------------

total_movies = movie_df["movie_id"].nunique()
total_revenue = movie_df["worldwide_revenue"].fillna(0).sum()

ott_movies = movie_df[
(movie_df["netflix"]==1) |
(movie_df["hulu"]==1) |
(movie_df["prime_video"]==1) |
(movie_df["disney_plus"]==1)
]

ott_coverage = (ott_movies["movie_id"].nunique()/total_movies)*100

avg_popularity = movie_df["popularity"].mean()

# --------------------------------------------------
# KPI CARDS
# --------------------------------------------------

col1,col2,col3,col4 = st.columns(4)

col1.metric("TOTAL MOVIES",f"{total_movies:,}")
col2.metric("GLOBAL REVENUE",f"${total_revenue/1e9:.2f}B")
col3.metric("OTT COVERAGE",f"{ott_coverage:.1f}%")
col4.metric("POPULARITY SCORE",f"{avg_popularity:.2f}")

st.divider()

# ==================================================
# PREP DATA
# ==================================================

movie_df = filtered.drop_duplicates("movie_id")


# ==================================================
# SECTION 1 — EXECUTIVE OVERVIEW
# Revenue Trend | Movies per Year
# ==================================================

col1, col2 = st.columns(2)

with col1:

    revenue_year = (
        movie_df.groupby("release_year")["worldwide_revenue"]
        .sum()
        .reset_index()
    )

    fig = px.line(
        revenue_year,
        x="release_year",
        y="worldwide_revenue",
        markers=True,
        title="GLOBAL BOX OFFICE REVENUE TRENDS",
        color_discrete_sequence=["#4FB3FF"]
    )

    fig.update_layout(template="plotly_dark",title_font=dict(size=22),title_x=0.2)

    st.plotly_chart(fig,use_container_width=True)


with col2:

    movies_year = (
        movie_df.groupby("release_year")
        .size()
        .reset_index(name="movies")
    )

    fig = px.bar(
        movies_year,
        x="release_year",
        y="movies",
        title="MOVIES RELEASED PER YEAR",
        color="movies",
        color_continuous_scale="Cividis"
    )

    fig.update_layout(template="plotly_dark",title_font=dict(size=22),title_x=0.2)

    st.plotly_chart(fig,use_container_width=True)


# ==================================================
# SECTION 1 CONTINUED
# OTT Distribution | Studio Market Share
# ==================================================

col1, col2 = st.columns(2)

with col1:

    ott_counts = {
    "Netflix": movie_df["netflix"].sum(),
    "Hulu": movie_df["hulu"].sum(),
    "Prime Video": movie_df["prime_video"].sum(),
    "Disney+": movie_df["disney_plus"].sum()
}

ott_df = pd.DataFrame(list(ott_counts.items()), columns=["platform","movies"])

fig1 = px.pie(
    ott_df,
    names="platform",
    values="movies",
    hole=0.55,
    title="OTT PLATFORM DISTRIBUTION",
    color_discrete_sequence=px.colors.qualitative.Set2
)

fig1.update_traces(
    textposition="outside",
    textinfo="percent+label",
    textfont_size=14
)

fig1.update_layout(
    template="plotly_dark",
    showlegend=True,
    title_font=dict(size=22),
    title_x=0.2
)

st.plotly_chart(fig1, use_container_width=True)


with col2:

    studio_market = (
    movie_df.groupby("studio_name")["worldwide_revenue"]
    .sum()
    .reset_index()
    .sort_values("worldwide_revenue",ascending=False)
    .head(10)
)

fig2 = px.pie(
    studio_market,
    names="studio_name",
    values="worldwide_revenue",
    title="STUDIO MARKET SHARES",
    color_discrete_sequence=px.colors.qualitative.Set3
)

fig2.update_traces(
    textposition="outside",
    textinfo="percent+label",
    textfont_size=14
)

fig2.update_layout(
    template="plotly_dark",
    title_font=dict(size=22),
    showlegend=True,
    title_x=0.2

)

st.plotly_chart(fig2, use_container_width=True)

# ==================================================
# SECTION 2 — MARKET INSIGHTS
# Genre Popularity | Revenue by Genre
# ==================================================

col1, col2 = st.columns(2)

with col1:

    genre_df = (
        movie_df.assign(genres=movie_df["genres"].str.split(","))
        .explode("genres")
        .groupby("genres")
        .size()
        .reset_index(name="movies")
        .sort_values("movies",ascending=False)
        .head(10)
    )

    fig = px.bar(
        genre_df,
        x="movies",
        y="genres",
        orientation="h",
        title="GENRES POPULARITY",
        color="movies",
        color_continuous_scale="Blues"
    )

    fig.update_layout(template="plotly_dark",title_font=dict(size=22),title_x=0.3)

    st.plotly_chart(fig,use_container_width=True)


with col2:

    genre_revenue = (
        movie_df.assign(genres=movie_df["genres"].str.split(","))
        .explode("genres")
        .groupby("genres")["worldwide_revenue"]
        .sum()
        .reset_index()
        .sort_values("worldwide_revenue",ascending=False)
        .head(10)
    )

    fig = px.bar(
        genre_revenue,
        x="worldwide_revenue",
        y="genres",
        orientation="h",
        title="REVENUE BY GENRES",
        color="worldwide_revenue",
        color_continuous_scale="Magma"
    )

    fig.update_layout(template="plotly_dark",title_font=dict(size=22),title_x=0.3)

    st.plotly_chart(fig,use_container_width=True)


# ==================================================
# SECTION 2 CONTINUED
# Movies by Country | Global Revenue Map
# ==================================================

col1, col2 = st.columns(2)

with col1:

    country_df = (
        movie_df.groupby("country")
        .size()
        .reset_index(name="movies")
        .sort_values("movies",ascending=False)
        .head(10)
    )

    fig = px.bar(
        country_df,
        x="movies",
        y="country",
        orientation="h",
        title="MOVIES BY COUNTRY",
        color="movies",
        color_continuous_scale="Viridis"
    )

    fig.update_layout(template="plotly_dark",title_font=dict(size=22),title_x=0.3)

    st.plotly_chart(fig,use_container_width=True)


with col2:

    map_df = (
        movie_df.groupby("country")["worldwide_revenue"]
        .sum()
        .reset_index()
    )

    fig = px.choropleth(
        map_df,
        locations="country",
        locationmode="country names",
        color="worldwide_revenue",
        title="GLOBAL REVENUE MAP",
        color_continuous_scale="Blues"
    )

    fig.update_layout(template="plotly_dark",title_font=dict(size=22),title_x=0.3)

    st.plotly_chart(fig,use_container_width=True)


# ==================================================
# SECTION 3 — BUSINESS PERFORMANCE
# Studio Revenue | Top Movies
# ==================================================

col1, col2 = st.columns(2)

with col1:

    studio_perf = (
        movie_df.groupby("studio_name")
        .agg(
            movies=("movie_id","count"),
            revenue=("worldwide_revenue","sum")
        )
        .reset_index()
        .sort_values("revenue",ascending=False)
        .head(20)
    )

    fig = px.scatter(
        studio_perf,
        x="movies",
        y="revenue",
        size="revenue",
        color="revenue",
        hover_name="studio_name",
        title="STUDIO PERFORMANCE ANALYSIS",
        color_continuous_scale="Turbo"
    )

    fig.update_layout(template="plotly_dark",title_font=dict(size=22),title_x=0.2)

    st.plotly_chart(fig,use_container_width=True)

with col2:

    top_movies = (
        movie_df.sort_values("worldwide_revenue",ascending=False)
        .head(10)
    )

    fig = px.bar(
        top_movies,
        x="worldwide_revenue",
        y="title",
        orientation="h",
        title="TOP MOVIES BY REVENUE",
        color="worldwide_revenue",
        color_continuous_scale="Sunset"
    )

    fig.update_layout(template="plotly_dark",title_font=dict(size=22),title_x=0.3)

    st.plotly_chart(fig,use_container_width=True)


# ==================================================
# SECTION 3 CONTINUED
# Rating vs Popularity | Studio Bubble
# ==================================================

col1, col2 = st.columns(2)

with col1:

    scatter_df = movie_df.dropna(
    subset=["popularity","worldwide_revenue"]
)

scatter_df = scatter_df[scatter_df["worldwide_revenue"] > 0]

scatter_df["revenue_billion"] = scatter_df["worldwide_revenue"] / 1e9

fig = px.scatter(
    scatter_df,
    x="popularity",
    y="worldwide_revenue",
    size="revenue_billion",
    color="revenue_billion",
    hover_name="title",
    title="POPULARITY VS REVENUE ANALYSIS",
    color_continuous_scale="Turbo",
    size_max=45
)

fig.update_layout(
    template="plotly_dark",
    xaxis_title="Popularity Score",
    yaxis_title="Revenue",
    title_font=dict(size=22),
    title_x=0.4

)

st.plotly_chart(fig, use_container_width=True)

with col2:

    studio_df = (
    movie_df.groupby("studio_name")["worldwide_revenue"]
    .sum()
    .reset_index()
    .sort_values("worldwide_revenue",ascending=False)
    .head(10)
)
studio_df["revenue_billion"] = studio_df["worldwide_revenue"] / 1e9

fig = px.treemap(
    studio_df,
    path=["studio_name"],
    values="revenue_billion",
    title="STUDIO REVENUE PERFORMANCE",
    color="revenue_billion",
    color_continuous_scale="Turbo"
)

fig.update_traces(
    textposition="middle center",
    texttemplate="%{label}<br>$%{value:.0f}B",
    hovertemplate="<b>%{label}</b><br>Revenue: $%{value:.2f}B"
)

fig.update_layout(
    template="plotly_dark",
    coloraxis_colorbar_title="Revenue (Billion $)",
    title_font=dict(size=22),
    title_x=0.4
)

st.plotly_chart(fig, use_container_width=True)


# ==================================================
# SECTION 4 — ADVANCED INSIGHT
# Genre Heatmap
# ==================================================

st.subheader("MOVIE PRODUCTION HEATMAP")

heat_df = (
    movie_df.assign(genres=movie_df["genres"].str.split(","))
    .explode("genres")
)

heat_df = (
    heat_df.groupby(["release_year","genres"])
    .size()
    .reset_index(name="movies")
)

fig = px.density_heatmap(
    heat_df,
    x="release_year",
    y="genres",
    z="movies",
    color_continuous_scale="Viridis",
    title="GENRE PRODUCTION HEATMAP"
)

fig.update_layout(template="plotly_dark",title_font=dict(size=22),title_x=0.4)

st.plotly_chart(fig,use_container_width=True)


# ==================================================
# SECTION 5 — DATA TABLE
# Top Movies Leaderboard
# ==================================================

st.subheader("TOP MOVIES LEADERBOARD")

leaderboard = (
    movie_df[
        ["title","genres","studio_name","country","popularity",
         "worldwide_revenue"]
    ]
    .sort_values("worldwide_revenue",ascending=False)
    .head(20)
)

leaderboard["worldwide_revenue"] = leaderboard["worldwide_revenue"].apply(
    lambda x: f"${x/1e9:.2f}B"
)

leaderboard["popularity"] = leaderboard["popularity"].round(2)

st.dataframe(leaderboard,use_container_width=True)


    