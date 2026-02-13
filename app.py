from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import streamlit as st

from pipeline import (
    build_figures,
    build_outputs,
    export_png_pack,
    export_png_pack_by_cooperative,
    load_prepared_tables,
)
try:
    from pipeline import export_cooperative_excel_extract
except ImportError:
    export_cooperative_excel_extract = None

st.set_page_config(page_title="CL Case Remediation Dashboard", layout="wide")
st.title("CL Case-to-Remediation Dashboard")

if "tables" not in st.session_state:
    st.session_state["tables"] = None
if "cooperative_extract_path" not in st.session_state:
    st.session_state["cooperative_extract_path"] = None


@st.cache_data(show_spinner=False)
def _load_tables_cached() -> dict[str, pd.DataFrame]:
    return load_prepared_tables()


def _compute_kpis(cl_filtered: pd.DataFrame) -> dict[str, float]:
    children = int(cl_filtered["ChldID"].nunique()) if not cl_filtered.empty else 0
    households = int(cl_filtered["FarmerID"].nunique()) if not cl_filtered.empty else 0
    cooperatives = int(cl_filtered["CooperativeLabel"].nunique()) if not cl_filtered.empty else 0
    communities = int(cl_filtered["CommunityName"].nunique()) if not cl_filtered.empty else 0
    pct_any = float((cl_filtered["TotalRemCount"] > 0).mean() * 100) if not cl_filtered.empty else 0.0
    return {
        "children": children,
        "households": households,
        "cooperatives": cooperatives,
        "communities": communities,
        "pct_any": pct_any,
    }


st.sidebar.header("Data")
source_mode = st.sidebar.radio(
    "Source",
    ["Prepared outputs", "Upload Excel and rebuild outputs"],
    index=0,
)

if source_mode == "Prepared outputs":
    if st.sidebar.button("Load prepared outputs"):
        try:
            st.session_state["tables"] = _load_tables_cached()
            st.sidebar.success("Prepared outputs loaded.")
        except Exception as exc:
            st.sidebar.error(f"Failed to load prepared outputs: {exc}")
else:
    uploaded = st.sidebar.file_uploader("Upload workbook (.xlsx)", type=["xlsx"])
    if st.sidebar.button("Process uploaded workbook"):
        if uploaded is None:
            st.sidebar.warning("Please upload an .xlsx file first.")
        else:
            tmp_path = None
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
                    tmp.write(uploaded.getbuffer())
                    tmp_path = tmp.name
                result = build_outputs(tmp_path, export_charts=False)
                st.session_state["tables"] = result["tables"]
                st.sidebar.success("Workbook processed and outputs updated.")
                st.sidebar.info(f"CL children in CICL: {result['logs']['cl_children']}")
            except Exception as exc:
                st.sidebar.error(f"Processing failed: {exc}")
            finally:
                if tmp_path and Path(tmp_path).exists():
                    Path(tmp_path).unlink(missing_ok=True)

if st.session_state["tables"] is None:
    try:
        st.session_state["tables"] = _load_tables_cached()
        st.caption("Loaded prepared outputs from outputs/tables/.")
    except Exception:
        st.warning(
            "No prepared outputs found. Run `python pipeline.py` first or upload the workbook in the sidebar."
        )
        st.stop()

tables: dict[str, pd.DataFrame] = st.session_state["tables"]
cl_master = tables["cl_child_master"].copy()

cl_master["CooperativeLabel"] = cl_master["CooperativeLabel"].astype("string")
cl_master["CommunityName"] = cl_master["CommunityName"].astype("string")

st.sidebar.header("Filters")
cooperative_options = sorted(cl_master["CooperativeLabel"].dropna().unique().tolist())
selected_cooperatives = st.sidebar.multiselect("Cooperative", cooperative_options, default=cooperative_options)

community_source = (
    cl_master[cl_master["CooperativeLabel"].isin(selected_cooperatives)]
    if selected_cooperatives
    else cl_master.iloc[0:0]
)
community_lookup = (
    community_source[["ComID", "CommunityName", "CooperativeLabel"]]
    .dropna(subset=["ComID", "CommunityName"])
    .drop_duplicates()
    .copy()
)
community_lookup["CommunityLabel"] = (
    community_lookup["CommunityName"].astype(str)
    + " ("
    + community_lookup["CooperativeLabel"].astype(str)
    + " | "
    + community_lookup["ComID"].astype(str)
    + ")"
)
community_options = ["All"] + community_lookup.sort_values(["CooperativeLabel", "CommunityName"])[
    "CommunityLabel"
].tolist()
selected_community = st.sidebar.selectbox("Community", community_options, index=0)
if selected_community == "All":
    community_filter = None
else:
    community_filter = community_lookup.loc[
        community_lookup["CommunityLabel"] == selected_community, "ComID"
    ].iloc[0]

if selected_cooperatives:
    cl_filtered = cl_master[cl_master["CooperativeLabel"].isin(selected_cooperatives)]
else:
    cl_filtered = cl_master.iloc[0:0]
if community_filter:
    cl_filtered = cl_filtered[cl_filtered["ComID"] == community_filter]

kpis = _compute_kpis(cl_filtered)
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Unique CL children", f"{kpis['children']:,}")
col2.metric("Unique CL households", f"{kpis['households']:,}")
col3.metric("Cooperatives", f"{kpis['cooperatives']:,}")
col4.metric("Communities", f"{kpis['communities']:,}")
col5.metric("Children with at least 1 remediation need (%)", f"{kpis['pct_any']:.1f}%")

figures = build_figures(
    tables=tables,
    cooperative_filter=selected_cooperatives,
    community_filter=community_filter,
)

st.subheader("Cooperative Overview")
st.plotly_chart(figures["01_cl_children_by_cooperative"], use_container_width=True)
st.plotly_chart(figures["02_risk_need_quadrant"], use_container_width=True)
st.plotly_chart(figures["09_priority_score_by_cooperative"], use_container_width=True)

st.subheader("Hazardous activity and remediation needs distribution")
st.plotly_chart(figures["03_hazard_signature_heatmap"], use_container_width=True)
col_left, col_right = st.columns(2)
with col_left:
    st.plotly_chart(figures["04_remediation_spread"], use_container_width=True)
with col_right:
    st.plotly_chart(figures["10_workload_depth"], use_container_width=True)

st.subheader("Remediation Priorities")
st.plotly_chart(figures["06_top_child_items"], use_container_width=True)
st.plotly_chart(figures["07_top_household_items"], use_container_width=True)
st.plotly_chart(figures["08_top_community_items"], use_container_width=True)
st.plotly_chart(figures["11_popular_remediation_combinations"], use_container_width=True)

if st.button("Export PNG pack"):
    try:
        paths = export_png_pack(figures, out_dir="outputs/charts")
        st.success(f"Exported {len(paths)} PNG charts to outputs/charts/")
        if paths:
            st.caption("\n".join(paths))
    except Exception as exc:
        st.error(
            "PNG export failed. Ensure `kaleido` is installed and Chrome is available on the host. "
            f"Details: {exc}"
        )

if st.button("Export PNG packs by Cooperative"):
    try:
        cooperative_export = export_png_pack_by_cooperative(
            tables=tables,
            out_dir="outputs/charts/by_cooperative",
            make_zip=True,
        )
        st.success(
            f"Exported cooperative packs for {cooperative_export['cooperative_count']} cooperatives to outputs/charts/by_cooperative/"
        )
        if cooperative_export.get("zip_path"):
            st.caption(f"ZIP: {cooperative_export['zip_path']}")
    except Exception as exc:
        st.error(
            "Cooperative bulk export failed. Ensure `kaleido` is installed and Chrome is available on the host. "
            f"Details: {exc}"
        )

if st.button("Generate cooperative Excel extract"):
    if export_cooperative_excel_extract is None:
        st.error("Current pipeline module does not expose `export_cooperative_excel_extract`. Update/redeploy to latest code.")
    else:
        try:
            extract_path = export_cooperative_excel_extract(
                tables=tables,
                out_path="outputs/tables/cooperative_figure_extract.xlsx",
                top_n=15,
            )
            st.session_state["cooperative_extract_path"] = extract_path
            st.success("Cooperative Excel extract generated.")
        except Exception as exc:
            st.error(f"Failed to generate cooperative Excel extract: {exc}")

extract_candidate = st.session_state.get("cooperative_extract_path") or "outputs/tables/cooperative_figure_extract.xlsx"
extract_file = Path(extract_candidate)
if extract_file.exists():
    with extract_file.open("rb") as fh:
        st.download_button(
            "Download cooperative Excel extract",
            data=fh.read(),
            file_name=extract_file.name,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
