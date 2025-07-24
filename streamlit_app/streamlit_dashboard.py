import streamlit as st
import pandas as pd
from pyathena import connect
import altair as alt


# --- Athena Query Wrapper ---
def query_athena(sql):
    conn = connect(
        s3_staging_dir=st.secrets["s3_staging_dir"],
        region_name=st.secrets["aws_region"],
        aws_access_key_id=st.secrets["aws_access_key_id"],
        aws_secret_access_key=st.secrets["aws_secret_access_key"]
    )
    return pd.read_sql(sql, conn)
# --- Cached Queries (refresh daily) ---
@st.cache_data(ttl=86400)
def get_media_plays_over_time():
    return query_athena("""
        SELECT 
            name,
            DATE_FORMAT(event_date,'%Y-%m') AS year_month,
            SUM(unique_plays) AS unique_plays
        FROM wistia_project.fact_media_engagement_daily
        INNER JOIN wistia_project.media_info USING (media_id)
        GROUP BY 1, 2 
        ORDER BY 1, 2;
    """)

@st.cache_data(ttl=86400)
def get_avg_play_rate_by_channel():
    return query_athena("""
        SELECT 
            channel,
            ROUND(AVG(play_rate)*100, 0) AS average_play_rate
        FROM wistia_project.fact_media_engagement_daily
        INNER JOIN wistia_project.media_info USING (media_id)
        GROUP BY 1;
    """)

@st.cache_data(ttl=86400)
def get_top_countries_by_viewers():
    return query_athena("""
        WITH country_counts AS (
            SELECT 
                country,
                COUNT(DISTINCT visitor_id) AS country_viewers
            FROM wistia_project.fact_visitor_engagement_daily
            INNER JOIN wistia_project.visitor_info USING (visitor_id)
            WHERE unique_plays > 0
            GROUP BY country
        ),
        total_count AS (
            SELECT COUNT(DISTINCT visitor_id) AS total_viewers
            FROM wistia_project.fact_visitor_engagement_daily
            WHERE unique_plays > 0
        )

        SELECT 
            c.country,
            c.country_viewers,
            ROUND(100.0 * c.country_viewers / t.total_viewers, 2) AS percent_viewers
        FROM country_counts c
        CROSS JOIN total_count t
        ORDER BY percent_viewers DESC
        LIMIT 5;
    """)

# --- App UI ---
st.title("📊 Wistia Media Engagement Dashboard")
st.markdown("Data updates every 24 hours. Click below to refresh manually if needed.")

if st.button("🔄 Refresh Data Now"):
    st.cache_data.clear()
    st.success("Cache cleared! Please rerun to fetch fresh data.")

# --- Media Plays Over Time Chart ---
st.header("🎥 Unique Plays Over Time by Media")
df_plays = get_media_plays_over_time()

if not df_plays.empty:
    chart = alt.Chart(df_plays).mark_line(point=True).encode(
        x='year_month:T',
        y='unique_plays:Q',
        color=alt.Color('name:N', legend=alt.Legend(labelLimit=1000)),  # <-- prevents truncation
        tooltip=['name', 'year_month', 'unique_plays']
    ).properties(
        width=700,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("No data returned for media plays.")

# --- Average Play Rate Chart ---
st.header("📈 Average Play Rate by Channel")
df_rate = get_avg_play_rate_by_channel()

if not df_rate.empty:
    chart = alt.Chart(df_rate).mark_bar().encode(
        x=alt.X('channel:N', title='Channel'),
        y=alt.Y('average_play_rate:Q', title='Average Play Rate (%)', scale=alt.Scale(domain=[0, 100])),
        tooltip=['channel', 'average_play_rate']
    ).properties(
        width=700,
        height=400
    )
    st.altair_chart(chart, use_container_width=True)
else:
    st.warning("No data returned for play rate.")
# --- Top Countries Table ---
st.header("🌍 Top 5 Countries by Viewer Share")
df_country = get_top_countries_by_viewers()
if not df_country.empty:
    st.dataframe(df_country)
else:
    st.warning("No data returned for country breakdown.")
