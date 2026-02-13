from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

REQUIRED_SHEETS = {
    "cicl": "CICL",
    "community": "Community Profile",
    "rem_child": "Remediation needs child",
    "rem_hh": "remediation needs household",
    "rem_com": "Remediation needs community",
}

DEFAULT_INPUT_PATHS = [
    Path(r"C:\Users\BrightKofiDzotor\Downloads\CL cases and remediation eveluation.xlsx"),
    Path("/mnt/data/CL cases and remediation eveluation.xlsx"),
]

TABLE_DIR = Path("outputs/tables")
CHART_DIR = Path("outputs/charts")

SPREAD_ORDER = ["0", "1", "2-3", "4-5", "6+"]
MIX_ORDER = [
    "None",
    "Child only",
    "Household only",
    "Community only",
    "Child + Household",
    "Child + Community",
    "Household + Community",
    "All three",
]

PNG_ORDER = [
    "01_cl_children_by_cooperative",
    "02_risk_need_quadrant",
    "09_priority_score_by_cooperative",
    "03_hazard_signature_heatmap",
    "04_remediation_spread",
    "10_workload_depth",
    "06_top_child_items",
    "07_top_household_items",
    "08_top_community_items",
    "11_popular_remediation_combinations",
]

ICI = {
    "green": "#007A3E",
    "lime": "#00AD50",
    "navy": "#004976",
    "sky": "#0081A6",
    "maroon": "#8A1538",
    "crimson": "#C5003E",
    "orange": "#DC4405",
    "burnt": "#A9431E",
    "cocoa": "#C99700",
    "soft_yellow": "#CAB64B",
    "slate": "#505759",
    "light_grey": "#919D9D",
    "bg": "#F7F9FB",
    "grid": "#E6E9EF",
    "white": "#FFFFFF",
}

ICI_HEAT = [
    [0.00, "#FFFFFF"],
    [0.20, "#D9F2E3"],
    [0.40, "#8ED9B0"],
    [0.60, "#00AD50"],
    [0.80, "#007A3E"],
    [1.00, "#004976"],
]

# Color grammar (single source of truth)
# 1) Continuous scale metrics: ICI_HEAT
# 2) Ordered intensity buckets: low -> high progression
# 3) Positive/factual primary bars: green
# 4) Risk/gap emphasis: crimson/maroon
# 5) Group comparisons: restrained navy/sky/green family
COLOR_PRIMARY_FACT = ICI["green"]
COLOR_PRIMARY_FACT_ALT = ICI["navy"]
COLOR_RISK = ICI["crimson"]
COLOR_RISK_ALT = ICI["maroon"]
COLOR_NEUTRAL = ICI["light_grey"]

COMPARISON_SEQUENCE = [
    ICI["navy"],
    ICI["sky"],
    ICI["green"],
    ICI["lime"],
    ICI["cocoa"],
]

SPREAD_COLOR_MAP = {
    "0": COLOR_NEUTRAL,
    "1": ICI["soft_yellow"],
    "2-3": ICI["sky"],
    "4-5": ICI["lime"],
    "6+": COLOR_PRIMARY_FACT,
}

MIX_COLOR_MAP = {
    "None": COLOR_RISK,
    "Child only": COLOR_PRIMARY_FACT,
    "Household only": ICI["navy"],
    "Community only": ICI["sky"],
    "Child + Household": ICI["lime"],
    "Child + Community": ICI["cocoa"],
    "Household + Community": ICI["burnt"],
    "All three": ICI["slate"],
}


def _register_ici_template() -> None:
    pio.templates["ici_modern"] = go.layout.Template(
        layout=go.Layout(
            paper_bgcolor=ICI["bg"],
            plot_bgcolor=ICI["bg"],
            colorway=[
                ICI["green"],
                ICI["navy"],
                ICI["sky"],
                ICI["lime"],
                ICI["orange"],
                ICI["cocoa"],
                ICI["maroon"],
            ],
            margin=dict(l=40, r=20, t=55, b=40),
            title=dict(x=0.02, xanchor="left"),
            xaxis=dict(
                showgrid=True,
                gridcolor=ICI["grid"],
                zeroline=False,
                linecolor=ICI["grid"],
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor=ICI["grid"],
                zeroline=False,
                linecolor=ICI["grid"],
            ),
            legend=dict(
                bgcolor="rgba(255,255,255,0.6)",
                bordercolor="rgba(0,0,0,0)",
                borderwidth=0,
            ),
        )
    )
    pio.templates.default = "ici_modern"


_register_ici_template()


def _normalize_identifier(series: pd.Series) -> pd.Series:
    clean = series.astype("string").str.strip()
    return clean.mask(clean.isna() | (clean == "") | (clean.str.lower() == "nan"), pd.NA)


def _normalize_label(series: pd.Series) -> pd.Series:
    clean = series.astype("string").str.strip()
    return clean.mask(clean.isna() | (clean == "") | (clean.str.lower() == "nan"), pd.NA)


def _resolve_input_path(input_path: str | None) -> Path:
    if input_path:
        candidate = Path(input_path)
        if not candidate.exists():
            raise FileNotFoundError(f"Input file not found: {candidate}")
        return candidate

    for candidate in DEFAULT_INPUT_PATHS:
        if candidate.exists():
            return candidate

    searched = "\n".join(f"- {p}" for p in DEFAULT_INPUT_PATHS)
    raise FileNotFoundError(
        "No input workbook found. Provide --input or place workbook at one of:\n" + searched
    )


def _find_column(columns: list[str], exact: str, fallback_patterns: list[str]) -> str:
    if exact in columns:
        return exact

    lowered = {c: c.lower() for c in columns}
    for pattern in fallback_patterns:
        pat = pattern.lower()
        for col in columns:
            if pat in lowered[col]:
                return col

    raise KeyError(
        f"Could not resolve required column. exact='{exact}', fallback_patterns={fallback_patterns}"
    )


def _find_optional_column(columns: list[str], exact: str, fallback_patterns: list[str]) -> str | None:
    try:
        return _find_column(columns, exact, fallback_patterns)
    except KeyError:
        return None


def _compose_item_with_other(item_series: pd.Series, other_series: pd.Series | None) -> pd.Series:
    item_clean = _normalize_label(item_series)
    if other_series is None:
        return item_clean
    other_clean = _normalize_label(other_series)
    is_other = item_clean.fillna("").str.contains("other", case=False, regex=False)
    has_other_text = other_clean.notna() & (other_clean.str.len() > 0)
    out = item_clean.where(~(is_other & has_other_text), "Other: " + other_clean.astype("string"))
    # Drop generic "Other (specify)" rows without a concrete specified value.
    out = out.mask(is_other & ~has_other_text, pd.NA)
    return out


def _detect_hazard_columns(df: pd.DataFrame) -> list[str]:
    hazard_cols: list[str] = []
    for col in df.columns:
        if not re.match(r"^[a-sA-S]\s", str(col)):
            continue

        series = df[col]
        numeric = pd.to_numeric(series, errors="coerce")
        non_na = numeric.dropna()
        if non_na.empty:
            continue

        unique_vals = set(non_na.unique().tolist())
        if unique_vals.issubset({0, 1}):
            hazard_cols.append(col)

    if not hazard_cols:
        raise ValueError("No hazard flag columns detected using [a-s] + 0/1-like logic.")

    return hazard_cols


def _spread_bucket(total: int) -> str:
    if total <= 0:
        return "0"
    if total == 1:
        return "1"
    if total <= 3:
        return "2-3"
    if total <= 5:
        return "4-5"
    return "6+"


def _rem_mix_category(child_count: int, hh_count: int, com_count: int) -> str:
    has_child = child_count > 0
    has_hh = hh_count > 0
    has_com = com_count > 0

    if not has_child and not has_hh and not has_com:
        return "None"
    if has_child and not has_hh and not has_com:
        return "Child only"
    if not has_child and has_hh and not has_com:
        return "Household only"
    if not has_child and not has_hh and has_com:
        return "Community only"
    if has_child and has_hh and not has_com:
        return "Child + Household"
    if has_child and not has_hh and has_com:
        return "Child + Community"
    if not has_child and has_hh and has_com:
        return "Household + Community"
    return "All three"


def _hazard_tier(hazard_count: int) -> str:
    if hazard_count <= 0:
        return "None (0)"
    if hazard_count <= 2:
        return "Low (1-2)"
    if hazard_count <= 4:
        return "Medium (3-4)"
    return "High (5+)"


def _empty_figure(title: str) -> go.Figure:
    fig = go.Figure()
    fig.add_annotation(
        text="No data for current filters",
        showarrow=False,
        x=0.5,
        y=0.5,
        font=dict(color=ICI["slate"]),
    )
    fig.update_layout(
        title=title,
        xaxis_visible=False,
        yaxis_visible=False,
        template="ici_modern",
        paper_bgcolor=ICI["bg"],
        plot_bgcolor=ICI["bg"],
    )
    return fig


def _filter_table(df: pd.DataFrame, cooperative_filter: list[str], community_filter: str | None) -> pd.DataFrame:
    out = df.copy()
    if "CooperativeLabel" in out.columns and cooperative_filter:
        out = out[out["CooperativeLabel"].isin(cooperative_filter)]

    if community_filter and community_filter != "All":
        if "ComID" in out.columns and community_filter in set(out["ComID"].dropna().astype(str)):
            out = out[out["ComID"].astype("string") == community_filter]
        elif "CommunityName" in out.columns:
            out = out[out["CommunityName"] == community_filter]

    return out


def _as_id_labels(series: pd.Series) -> pd.Series:
    labels = series.astype("string").str.strip()
    return labels.mask(labels.isna() | (labels == "") | (labels.str.lower() == "nan"), pd.NA)


def _center_single_value_range(value: float) -> tuple[float, float]:
    pad = max(abs(value) * 0.25, 0.5)
    return (value - pad, value + pad)


def _category_layout_profile(count: int) -> dict[str, Any]:
    if count <= 1:
        return {"bargap": 0.62, "margin": dict(l=90, r=90, t=55, b=40)}
    if count <= 3:
        return {"bargap": 0.48, "margin": dict(l=70, r=70, t=55, b=40)}
    if count <= 6:
        return {"bargap": 0.34, "margin": dict(l=55, r=55, t=55, b=40)}
    return {"bargap": 0.22, "margin": dict(l=40, r=20, t=55, b=40)}


def _unique_axis_values(fig: go.Figure, axis: str) -> list[str]:
    values: list[str] = []
    for trace in fig.data:
        arr = getattr(trace, axis, None)
        if arr is None:
            continue
        values.extend([str(v) for v in arr if v is not None])
    # Preserve first-seen order
    seen: set[str] = set()
    ordered: list[str] = []
    for v in values:
        if v not in seen:
            seen.add(v)
            ordered.append(v)
    return ordered


def _apply_single_category_focus(fig: go.Figure, axis: str) -> None:
    values = _unique_axis_values(fig, axis)
    if len(values) != 1:
        return
    # Shrink plot domain so a single bar does not look stretched across the canvas.
    if axis == "x":
        fig.update_layout(xaxis=dict(domain=[0.28, 0.72]))
    elif axis == "y":
        fig.update_layout(yaxis=dict(domain=[0.26, 0.74]))


def _nice_integer_tick_values(max_value: int, max_ticks: int = 6) -> list[int]:
    if max_value <= 1:
        return [0, 1]
    if max_value <= max_ticks:
        return list(range(0, max_value + 1))

    raw_step = max_value / (max_ticks - 1)
    candidates = [1, 2, 5, 10, 20, 25, 50, 100]
    step = candidates[-1]
    for c in candidates:
        if c >= raw_step:
            step = c
            break
    vals = list(range(0, max_value + step, step))
    if vals[-1] < max_value:
        vals.append(max_value)
    return sorted(set(vals))


def _ellipsize(text: str, max_len: int = 38) -> str:
    text = str(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _hex_to_rgba(hex_color: str, alpha: float) -> str:
    c = hex_color.lstrip("#")
    r = int(c[0:2], 16)
    g = int(c[2:4], 16)
    b = int(c[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def _safe_path_token(value: str) -> str:
    token = str(value).strip()
    token = re.sub(r"[^\w\-\.]+", "_", token)
    return token or "unknown"


def _drop_generic_other_specify(df: pd.DataFrame, item_col: str) -> pd.DataFrame:
    if df.empty or item_col not in df.columns:
        return df
    out = df.copy()
    labels = out[item_col].astype("string").str.strip()
    generic_other = labels.str.fullmatch(r"(?i)other\s*\(specify\)\s*:?\s*")
    return out[~generic_other.fillna(False)].copy()


def _build_cooperative_label(
    cooperative_name: pd.Series, cooperative_id: pd.Series
) -> pd.Series:
    name = _normalize_label(cooperative_name).fillna("Unknown cooperative")
    coop_id = _normalize_identifier(cooperative_id).fillna("Unknown ID")
    return name.astype("string") + " (" + coop_id.astype("string") + ")"


def _get_level_item_map(
    df: pd.DataFrame, entity_col: str, item_col: str, fallback_label: str
) -> dict[str, str]:
    if df.empty or entity_col not in df.columns or item_col not in df.columns:
        return {}

    work = df[[entity_col, item_col]].dropna().drop_duplicates().copy()
    if work.empty:
        return {}

    item_freq = work[item_col].value_counts().to_dict()
    work["freq"] = work[item_col].map(item_freq).fillna(0)
    work = work.sort_values([entity_col, "freq", item_col], ascending=[True, False, True])
    chosen = work.drop_duplicates(entity_col, keep="first")
    out = dict(zip(chosen[entity_col].astype(str), chosen[item_col].astype(str)))
    return {k: v if v else fallback_label for k, v in out.items()}


def _build_sankey_paths(
    cl: pd.DataFrame, rchild: pd.DataFrame, rhh: pd.DataFrame, rcom: pd.DataFrame
) -> pd.DataFrame:
    child_map = _get_level_item_map(rchild, "ChldID", "ChildItem", "No child item")
    hh_map = _get_level_item_map(rhh, "FarmerID", "HhItem", "No household item")
    com_map = _get_level_item_map(rcom, "ComID", "ComItem", "No community item")

    base = cl[["ChldID", "FarmerID", "ComID"]].drop_duplicates().copy()
    base["ChldID"] = base["ChldID"].astype(str)
    base["FarmerID"] = base["FarmerID"].astype(str)
    base["ComID"] = base["ComID"].astype(str)

    base["ChildNode"] = base["ChldID"].map(child_map).fillna("No child item")
    base["HouseholdNode"] = base["FarmerID"].map(hh_map).fillna("No household item")
    base["CommunityNode"] = base["ComID"].map(com_map).fillna("No community item")
    return base[["ChldID", "ChildNode", "HouseholdNode", "CommunityNode"]]


def _compress_nodes_with_other(
    paths: pd.DataFrame, col: str, top_n: int, other_label: str, no_label: str
) -> pd.Series:
    counts = (
        paths.groupby(col, dropna=False)["ChldID"]
        .nunique()
        .reset_index(name="Children")
        .sort_values(["Children", col], ascending=[False, True])
    )
    labels = counts[col].astype(str)
    concrete = counts[
        ~labels.str.startswith("No ", na=False) & ~labels.str.startswith("Other ", na=False)
    ]
    keep = concrete[col].head(top_n).astype(str).tolist()
    if no_label in set(labels):
        keep.append(no_label)
    keep_set = set(keep)
    return paths[col].astype(str).where(paths[col].astype(str).isin(keep_set), other_label)


def _aggregate_sankey_edges(paths: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    edge_ch_hh = (
        paths.groupby(["ChildNode", "HouseholdNode"], dropna=False)["ChldID"]
        .nunique()
        .reset_index(name="Children")
    )
    edge_hh_com = (
        paths.groupby(["HouseholdNode", "CommunityNode"], dropna=False)["ChldID"]
        .nunique()
        .reset_index(name="Children")
    )
    return edge_ch_hh, edge_hh_com


def _adaptive_sankey_top_n(unique_count: int) -> int:
    if unique_count > 18:
        return 4
    if unique_count > 14:
        return 5
    if unique_count > 10:
        return 6
    if unique_count > 7:
        return 7
    return 8


def _ordered_stage_nodes(
    counts: pd.DataFrame, col: str, no_label: str, other_label: str
) -> list[str]:
    work = counts.copy()
    work[col] = work[col].astype(str)
    work = work.sort_values(["Children", col], ascending=[False, True])
    normal = work[
        (work[col] != no_label)
        & (work[col] != other_label)
        & ~work[col].str.startswith("No ", na=False)
        & ~work[col].str.startswith("Other ", na=False)
    ][col].tolist()
    ordered = normal
    if no_label in set(work[col]):
        ordered.append(no_label)
    if other_label in set(work[col]):
        ordered.append(other_label)
    return ordered


def load_prepared_tables(base_dir: str = "outputs/tables") -> dict[str, pd.DataFrame]:
    table_path = Path(base_dir)
    tables = {
        "cl_child_master": pd.read_csv(table_path / "cl_child_master.csv", dtype="string"),
        "hazards_long": pd.read_csv(table_path / "hazards_long.csv", dtype="string"),
        "rem_child_long": pd.read_csv(table_path / "rem_child_long.csv", dtype="string"),
        "rem_hh_long": pd.read_csv(table_path / "rem_hh_long.csv", dtype="string"),
        "rem_com_priority": pd.read_csv(table_path / "rem_com_priority.csv", dtype="string"),
        "planning_quantities_by_cooperative": pd.read_excel(
            table_path / "planning_quantities_by_cooperative.xlsx",
            dtype={
                "CooperativeID": "string",
                "CooperativeName": "string",
                "CooperativeLabel": "string",
            },
        ),
    }

    numeric_cols = ["HazardCount", "ChildRemCount", "HhRemCount", "ComRemCount", "TotalRemCount"]
    for col in numeric_cols:
        tables["cl_child_master"][col] = pd.to_numeric(tables["cl_child_master"][col], errors="coerce").fillna(0)

    if "FlagNeeded" in tables["rem_com_priority"].columns:
        tables["rem_com_priority"]["FlagNeeded"] = pd.to_numeric(
            tables["rem_com_priority"]["FlagNeeded"], errors="coerce"
        ).fillna(0)

    return tables


def build_figures(
    tables: dict[str, pd.DataFrame],
    cooperative_filter: list[str],
    community_filter: str | None,
) -> dict[str, go.Figure]:
    _register_ici_template()
    cl = _filter_table(tables["cl_child_master"], cooperative_filter, community_filter)
    hz = _filter_table(tables["hazards_long"], cooperative_filter, community_filter)
    rchild = _filter_table(tables["rem_child_long"], cooperative_filter, community_filter)
    rhh = _filter_table(tables["rem_hh_long"], cooperative_filter, community_filter)
    rcom = _filter_table(tables["rem_com_priority"], cooperative_filter, community_filter)
    rchild = _drop_generic_other_specify(rchild, "ChildItem")
    rhh = _drop_generic_other_specify(rhh, "HhItem")
    rcom = _drop_generic_other_specify(rcom, "ComItem")

    cooperative_order = [str(d).strip() for d in cooperative_filter if str(d).strip()]
    for frame in [cl, hz, rchild, rhh, rcom]:
        if "CooperativeLabel" in frame.columns:
            frame["CooperativeLabel"] = _as_id_labels(frame["CooperativeLabel"])

    figures: dict[str, go.Figure] = {}

    if cl.empty:
        for key, title in [
            ("01_cl_children_by_cooperative", "CL children by cooperative"),
            ("02_risk_need_quadrant", "Cooperative priority map: hazard vs remediation needs"),
            ("09_priority_score_by_cooperative", "Cooperative priority score"),
            ("03_hazard_signature_heatmap", "Hazard signature heatmap"),
            ("04_remediation_spread", "Children by number of remediation needs"),
            ("10_workload_depth", "Average remediation needs per child"),
            ("06_top_child_items", "Top child remediation items"),
            ("07_top_household_items", "Top household remediation items"),
            ("08_top_community_items", "Top community remediation items"),
            ("11_popular_remediation_combinations", "Popular remediation item combinations"),
        ]:
            figures[key] = _empty_figure(title)
        return figures

    children_by_cooperative = (
        cl.groupby("CooperativeLabel", dropna=False)["ChldID"]
        .nunique()
        .reset_index(name="CLChildren")
        .sort_values("CLChildren", ascending=False)
    )
    figures["01_cl_children_by_cooperative"] = px.bar(
        children_by_cooperative,
        x="CooperativeLabel",
        y="CLChildren",
        title="CL children by cooperative",
        color_discrete_sequence=[COLOR_PRIMARY_FACT],
        category_orders={"CooperativeLabel": cooperative_order or children_by_cooperative["CooperativeLabel"].astype(str).tolist()},
    )
    figures["01_cl_children_by_cooperative"].update_traces(
        marker_line_width=0,
        text=children_by_cooperative["CLChildren"],
        textposition="outside",
        cliponaxis=False,
    )

    cooperative_summary = (
        cl.groupby("CooperativeLabel", dropna=False)
        .agg(
            AvgHazards=("HazardCount", "mean"),
            AvgTotalRem=("TotalRemCount", "mean"),
            CLChildren=("ChldID", "nunique"),
        )
        .reset_index()
    )
    figures["02_risk_need_quadrant"] = px.scatter(
        cooperative_summary,
        x="AvgHazards",
        y="AvgTotalRem",
        size="CLChildren",
        color="CooperativeLabel",
        hover_name="CooperativeLabel",
        title="Cooperative priority map: hazard vs remediation needs",
        labels={
            "AvgHazards": "Average hazardous activities per child",
            "AvgTotalRem": "Average remediation needs per child",
            "CLChildren": "Number of CL children",
        },
        color_discrete_sequence=COMPARISON_SEQUENCE,
    )
    figures["02_risk_need_quadrant"].update_traces(
        marker=dict(line=dict(width=1, color=ICI["light_grey"]), opacity=0.92)
    )
    x_ref = float(cooperative_summary["AvgHazards"].median())
    y_ref = float(cooperative_summary["AvgTotalRem"].median())
    figures["02_risk_need_quadrant"].add_vline(
        x=x_ref, line_width=1, line_dash="dash", line_color=ICI["light_grey"]
    )
    figures["02_risk_need_quadrant"].add_hline(
        y=y_ref, line_width=1, line_dash="dash", line_color=ICI["light_grey"]
    )
    figures["02_risk_need_quadrant"].add_annotation(
        xref="paper",
        yref="paper",
        x=0.02,
        y=0.98,
        text="Higher support need / lower risk",
        showarrow=False,
        font=dict(color=ICI["slate"]),
        xanchor="left",
    )
    figures["02_risk_need_quadrant"].add_annotation(
        xref="paper",
        yref="paper",
        x=0.98,
        y=0.98,
        text="Higher support need / higher risk",
        showarrow=False,
        font=dict(color=ICI["slate"]),
        xanchor="right",
    )
    figures["02_risk_need_quadrant"].add_annotation(
        xref="paper",
        yref="paper",
        x=0.02,
        y=0.02,
        text="Lower support need / lower risk",
        showarrow=False,
        font=dict(color=ICI["slate"]),
        xanchor="left",
        yanchor="bottom",
    )
    figures["02_risk_need_quadrant"].add_annotation(
        xref="paper",
        yref="paper",
        x=0.98,
        y=0.02,
        text="Lower support need / higher risk",
        showarrow=False,
        font=dict(color=ICI["slate"]),
        xanchor="right",
        yanchor="bottom",
    )
    if len(cooperative_summary) == 1:
        x0 = float(cooperative_summary["AvgHazards"].iloc[0])
        y0 = float(cooperative_summary["AvgTotalRem"].iloc[0])
        xr = _center_single_value_range(x0)
        yr = _center_single_value_range(y0)
        figures["02_risk_need_quadrant"].update_xaxes(range=[xr[0], xr[1]])
        figures["02_risk_need_quadrant"].update_yaxes(range=[yr[0], yr[1]])

    priority = cooperative_summary.copy()
    priority["PriorityScore"] = (
        priority["AvgHazards"] * priority["AvgTotalRem"] * priority["CLChildren"]
    )
    priority = priority.sort_values("PriorityScore", ascending=False)
    figures["09_priority_score_by_cooperative"] = px.bar(
        priority,
        x="PriorityScore",
        y="CooperativeLabel",
        orientation="h",
        title="Cooperative priority score (hazard x remediation needs x children)",
        labels={"PriorityScore": "Priority score", "CooperativeLabel": "Cooperative"},
        color_discrete_sequence=[COLOR_PRIMARY_FACT_ALT],
        category_orders={"CooperativeLabel": priority["CooperativeLabel"].astype(str).tolist()},
    )
    figures["09_priority_score_by_cooperative"].update_traces(marker_line_width=0)

    children_den = cl.groupby("CooperativeLabel", dropna=False)["ChldID"].nunique().rename("CooperativeChildren")
    hz_num = hz.groupby(["CooperativeLabel", "HazardLabel"], dropna=False)["ChldID"].nunique().rename("HazardChildren")
    hz_matrix = hz_num.reset_index().merge(children_den.reset_index(), on="CooperativeLabel", how="left")
    hz_matrix["Pct"] = (hz_matrix["HazardChildren"] / hz_matrix["CooperativeChildren"]) * 100
    if hz_matrix.empty:
        figures["03_hazard_signature_heatmap"] = _empty_figure("Hazard Signature Heatmap")
    else:
        figures["03_hazard_signature_heatmap"] = px.density_heatmap(
            hz_matrix,
            x="HazardLabel",
            y="CooperativeLabel",
            z="Pct",
            text_auto=".1f",
            title="Hazard signature heatmap (% CL children)",
            labels={"Pct": "% CL children"},
            color_continuous_scale="YlOrRd",
            category_orders={"CooperativeLabel": cooperative_order or hz_matrix["CooperativeLabel"].astype(str).tolist()},
        )

    spread = pd.crosstab(cl["CooperativeLabel"], cl["SpreadBucket"], normalize="index").reindex(columns=SPREAD_ORDER, fill_value=0)
    spread_long = spread.reset_index().melt(id_vars="CooperativeLabel", var_name="SpreadBucket", value_name="Share")
    spread_long["Share"] = spread_long["Share"] * 100
    figures["04_remediation_spread"] = px.bar(
        spread_long,
        x="CooperativeLabel",
        y="Share",
        color="SpreadBucket",
        title="Children by number of remediation needs",
        labels={"Share": "Share of children (%)", "CooperativeLabel": "Cooperative", "SpreadBucket": "Needs bucket"},
        category_orders={
            "SpreadBucket": SPREAD_ORDER,
            "CooperativeLabel": cooperative_order or spread_long["CooperativeLabel"].astype(str).tolist(),
        },
        color_discrete_map=SPREAD_COLOR_MAP,
    )
    figures["04_remediation_spread"].update_traces(marker_line_width=0)

    burden = (
        cl.assign(HighIntensity=(cl["TotalRemCount"] >= 4).astype(int))
        .groupby("CooperativeLabel", dropna=False)["HighIntensity"]
        .mean()
        .mul(100)
        .reset_index(name="PctHighIntensity")
    )
    workload_depth = cooperative_summary.merge(burden, on="CooperativeLabel", how="left").fillna({"PctHighIntensity": 0})
    workload_depth = workload_depth.sort_values("AvgTotalRem", ascending=False)
    figures["10_workload_depth"] = px.bar(
        workload_depth,
        x="AvgTotalRem",
        y="CooperativeLabel",
        orientation="h",
        title="Average remediation needs per child",
        labels={"AvgTotalRem": "Average remediation needs per child", "CooperativeLabel": "Cooperative"},
        color_discrete_sequence=[COLOR_PRIMARY_FACT_ALT],
        category_orders={"CooperativeLabel": workload_depth["CooperativeLabel"].astype(str).tolist()},
        custom_data=["PctHighIntensity", "CLChildren"],
    )
    figures["10_workload_depth"].update_traces(
        marker_line_width=0,
        opacity=0.88,
        text=workload_depth["AvgTotalRem"].round(2),
        textposition="outside",
        cliponaxis=False,
        hovertemplate=(
            "Cooperative %{y}<br>"
            "Average remediation needs/child: %{x:.2f}<br>"
            "Children needing 4+ remediation needs: %{customdata[0]:.1f}%<br>"
            "CL children: %{customdata[1]}<extra></extra>"
        ),
    )

    top_child = (
        rchild.groupby("ChildItem", dropna=False)["ChldID"]
        .nunique()
        .reset_index(name="Children")
        .sort_values("Children", ascending=False)
        .head(15)
    )
    top_child = top_child.sort_values("Children", ascending=True)
    top_child["Children"] = pd.to_numeric(top_child["Children"], errors="coerce").fillna(0).astype(int)
    if top_child.empty:
        figures["06_top_child_items"] = _empty_figure("Top Child Remediation Items")
    else:
        figures["06_top_child_items"] = px.bar(
            top_child,
            x="Children",
            y="ChildItem",
            orientation="h",
            title="Top child remediation items (Top 15)",
            color_discrete_sequence=[COLOR_PRIMARY_FACT],
            labels={"Children": "Children (count)", "ChildItem": "Child remediation item"},
        )
        figures["06_top_child_items"].update_traces(
            marker_line_width=0,
            text=top_child["Children"],
            textposition="outside",
            cliponaxis=False,
            hovertemplate="Item: %{y}<br>Children (count): %{x:.0f}<extra></extra>",
        )
        child_tickvals = _nice_integer_tick_values(int(top_child["Children"].max()) if not top_child.empty else 1)
        figures["06_top_child_items"].update_xaxes(
            type="linear",
            tickmode="array",
            tickvals=child_tickvals,
            ticktext=[str(v) for v in child_tickvals],
            rangemode="tozero",
        )

    top_hh = (
        rhh.groupby("HhItem", dropna=False)["FarmerID"]
        .nunique()
        .reset_index(name="Households")
        .sort_values("Households", ascending=False)
        .head(15)
    )
    top_hh = top_hh.sort_values("Households", ascending=True)
    top_hh["Households"] = pd.to_numeric(top_hh["Households"], errors="coerce").fillna(0).astype(int)
    if top_hh.empty:
        figures["07_top_household_items"] = _empty_figure("Top Household Remediation Items")
    else:
        figures["07_top_household_items"] = px.bar(
            top_hh,
            x="Households",
            y="HhItem",
            orientation="h",
            title="Top household remediation items (Top 15)",
            color_discrete_sequence=[COLOR_PRIMARY_FACT_ALT],
            labels={"Households": "Households (count)", "HhItem": "Household remediation item"},
        )
        figures["07_top_household_items"].update_traces(
            marker_line_width=0,
            text=top_hh["Households"],
            textposition="outside",
            cliponaxis=False,
            hovertemplate="Item: %{y}<br>Households (count): %{x:.0f}<extra></extra>",
        )
        hh_tickvals = _nice_integer_tick_values(int(top_hh["Households"].max()) if not top_hh.empty else 1)
        figures["07_top_household_items"].update_xaxes(
            type="linear",
            tickmode="array",
            tickvals=hh_tickvals,
            ticktext=[str(v) for v in hh_tickvals],
            rangemode="tozero",
        )

    com_children = cl[["ChldID", "ComID"]].drop_duplicates()
    top_com = (
        rcom[["ComID", "ComItem"]]
        .drop_duplicates()
        .merge(com_children, on="ComID", how="inner")
        .groupby("ComItem", dropna=False)["ChldID"]
        .nunique()
        .reset_index(name="Children")
        .sort_values("Children", ascending=False)
        .head(15)
    )
    top_com = top_com.sort_values("Children", ascending=True)
    top_com["Children"] = pd.to_numeric(top_com["Children"], errors="coerce").fillna(0).astype(int)
    if top_com.empty:
        figures["08_top_community_items"] = _empty_figure("Top community remediation items")
    else:
        figures["08_top_community_items"] = px.bar(
            top_com,
            x="Children",
            y="ComItem",
            orientation="h",
            title="Top community remediation items (Top 15)",
            labels={"Children": "Children (count)", "ComItem": "Community remediation item"},
            color_discrete_sequence=[ICI["sky"]],
        )
        figures["08_top_community_items"].update_traces(
            marker_line_width=0,
            text=top_com["Children"],
            textposition="outside",
            cliponaxis=False,
            hovertemplate="Item: %{y}<br>Children (count): %{x:.0f}<extra></extra>",
        )
        com_tickvals = _nice_integer_tick_values(int(top_com["Children"].max()) if not top_com.empty else 1)
        figures["08_top_community_items"].update_xaxes(
            type="linear",
            tickmode="array",
            tickvals=com_tickvals,
            ticktext=[str(v) for v in com_tickvals],
            rangemode="tozero",
        )

    paths = _build_sankey_paths(cl, rchild, rhh, rcom)
    if paths.empty:
        figures["11_popular_remediation_combinations"] = _empty_figure(
            "Most common remediation pathways (Child -> Household -> Community)"
        )
    else:
        child_unique = int(paths["ChildNode"].nunique())
        hh_unique = int(paths["HouseholdNode"].nunique())
        com_unique = int(paths["CommunityNode"].nunique())
        top_child_n = _adaptive_sankey_top_n(child_unique)
        top_hh_n = _adaptive_sankey_top_n(hh_unique)
        top_com_n = _adaptive_sankey_top_n(com_unique)

        # Keep the visual tight by limiting visible nodes per stage.
        paths["ChildNode"] = _compress_nodes_with_other(
            paths, "ChildNode", top_n=top_child_n, other_label="Other child", no_label="No child item"
        )
        paths["HouseholdNode"] = _compress_nodes_with_other(
            paths, "HouseholdNode", top_n=top_hh_n, other_label="Other household", no_label="No household item"
        )
        paths["CommunityNode"] = _compress_nodes_with_other(
            paths, "CommunityNode", top_n=top_com_n, other_label="Other community", no_label="No community item"
        )

        edge_ch_hh, edge_hh_com = _aggregate_sankey_edges(paths)
        if edge_ch_hh.empty and edge_hh_com.empty:
            figures["11_popular_remediation_combinations"] = _empty_figure(
                "Most common remediation pathways (Child -> Household -> Community)"
            )
        else:
            child_counts = (
                paths.groupby("ChildNode", dropna=False)["ChldID"]
                .nunique()
                .reset_index(name="Children")
                .sort_values(["Children", "ChildNode"], ascending=[False, True])
            )
            hh_counts = (
                paths.groupby("HouseholdNode", dropna=False)["ChldID"]
                .nunique()
                .reset_index(name="Children")
                .sort_values(["Children", "HouseholdNode"], ascending=[False, True])
            )
            com_counts = (
                paths.groupby("CommunityNode", dropna=False)["ChldID"]
                .nunique()
                .reset_index(name="Children")
                .sort_values(["Children", "CommunityNode"], ascending=[False, True])
            )

            child_nodes = _ordered_stage_nodes(
                child_counts, "ChildNode", no_label="No child item", other_label="Other child"
            )
            hh_nodes = _ordered_stage_nodes(
                hh_counts, "HouseholdNode", no_label="No household item", other_label="Other household"
            )
            com_nodes = _ordered_stage_nodes(
                com_counts, "CommunityNode", no_label="No community item", other_label="Other community"
            )

            node_ids = (
                [f"C|{str(n)}" for n in child_nodes]
                + [f"H|{str(n)}" for n in hh_nodes]
                + [f"M|{str(n)}" for n in com_nodes]
            )
            labels = (
                [_ellipsize(str(n), 30) for n in child_nodes]
                + [_ellipsize(str(n), 30) for n in hh_nodes]
                + [_ellipsize(str(n), 30) for n in com_nodes]
            )
            node_index = {node_id: i for i, node_id in enumerate(node_ids)}
            level_tags = (
                ["Child"] * len(child_nodes)
                + ["Household"] * len(hh_nodes)
                + ["Community"] * len(com_nodes)
            )
            node_raw_names = [str(n) for n in child_nodes] + [str(n) for n in hh_nodes] + [str(n) for n in com_nodes]
            child_count_map = dict(zip(child_counts["ChildNode"].astype(str), child_counts["Children"].astype(int)))
            hh_count_map = dict(zip(hh_counts["HouseholdNode"].astype(str), hh_counts["Children"].astype(int)))
            com_count_map = dict(zip(com_counts["CommunityNode"].astype(str), com_counts["Children"].astype(int)))
            node_counts = (
                [child_count_map.get(n, 0) for n in child_nodes]
                + [hh_count_map.get(n, 0) for n in hh_nodes]
                + [com_count_map.get(n, 0) for n in com_nodes]
            )

            def _node_color(node: str, level: str, idx: int) -> str:
                if node.startswith("No ") or node.startswith("Other "):
                    return ICI["light_grey"]
                if level == "child":
                    palette = [ICI["green"], ICI["lime"]]
                elif level == "household":
                    palette = [ICI["navy"], ICI["sky"]]
                else:
                    palette = [ICI["cocoa"], ICI["soft_yellow"]]
                return palette[idx % len(palette)]

            node_colors = []
            for i, n in enumerate(child_nodes):
                node_colors.append(_node_color(n, "child", i))
            for i, n in enumerate(hh_nodes):
                node_colors.append(_node_color(n, "household", i))
            for i, n in enumerate(com_nodes):
                node_colors.append(_node_color(n, "community", i))

            sources: list[int] = []
            targets: list[int] = []
            values: list[float] = []
            link_colors: list[str] = []

            for row in edge_ch_hh.itertuples(index=False):
                src = f"C|{str(row.ChildNode)}"
                dst = f"H|{str(row.HouseholdNode)}"
                if src in node_index and dst in node_index:
                    sources.append(node_index[src])
                    targets.append(node_index[dst])
                    values.append(float(row.Children))
                    link_colors.append(_hex_to_rgba(ICI["green"], 0.28))

            for row in edge_hh_com.itertuples(index=False):
                src = f"H|{str(row.HouseholdNode)}"
                dst = f"M|{str(row.CommunityNode)}"
                if src in node_index and dst in node_index:
                    sources.append(node_index[src])
                    targets.append(node_index[dst])
                    values.append(float(row.Children))
                    link_colors.append(_hex_to_rgba(ICI["navy"], 0.24))

            if not values:
                figures["11_popular_remediation_combinations"] = _empty_figure(
                    "Most common remediation pathways (Child -> Household -> Community)"
                )
            else:
                sankey = go.Figure(
                    data=[
                        go.Sankey(
                            arrangement="snap",
                            textfont=dict(color=ICI["slate"], size=12),
                            node=dict(
                                label=labels,
                                color=node_colors,
                                customdata=list(zip(level_tags, node_raw_names, node_counts)),
                                pad=24,
                                thickness=16,
                                line=dict(color=ICI["grid"], width=0.5),
                                x=([0.01] * len(child_nodes))
                                + ([0.50] * len(hh_nodes))
                                + ([0.99] * len(com_nodes)),
                                hovertemplate=(
                                    "%{customdata[0]}: %{customdata[1]}<br>"
                                    "Children linked: %{customdata[2]:.0f}<extra></extra>"
                                ),
                            ),
                            link=dict(
                                source=sources,
                                target=targets,
                                value=values,
                                color=link_colors,
                                hovertemplate=(
                                    "%{source.label} -> %{target.label}<br>"
                                    "Children (count): %{value:.0f}<extra></extra>"
                                ),
                            ),
                        )
                    ]
                )
                sankey.update_layout(
                    title="Most common remediation pathways (Child -> Household -> Community)",
                    template="ici_modern",
                    margin=dict(l=20, r=20, t=76, b=30),
                )
                sankey.add_annotation(
                    xref="paper",
                    yref="paper",
                    x=0.01,
                    y=1.02,
                    showarrow=False,
                    text="Child level",
                    font=dict(color=ICI["green"], size=12),
                    xanchor="left",
                )
                sankey.add_annotation(
                    xref="paper",
                    yref="paper",
                    x=0.50,
                    y=1.02,
                    showarrow=False,
                    text="Household level",
                    font=dict(color=ICI["navy"], size=12),
                    xanchor="center",
                )
                sankey.add_annotation(
                    xref="paper",
                    yref="paper",
                    x=0.99,
                    y=1.02,
                    showarrow=False,
                    text="Community level",
                    font=dict(color=ICI["cocoa"], size=12),
                    xanchor="right",
                )
                sankey.add_annotation(
                    xref="paper",
                    yref="paper",
                    x=0.01,
                    y=1.10,
                    showarrow=False,
                    text="Flow width = number of children following that pathway under current filters",
                    font=dict(color=ICI["slate"], size=12),
                    xanchor="left",
                )
                figures["11_popular_remediation_combinations"] = sankey

    for key, fig in figures.items():
        if key in {"04_remediation_spread"}:
            fig.update_layout(barmode="stack")
        fig.update_layout(template="ici_modern")
        if key in {"01_cl_children_by_cooperative", "04_remediation_spread"}:
            fig.update_xaxes(type="category")
            cat_count = len(_unique_axis_values(fig, "x"))
            profile = _category_layout_profile(cat_count)
            fig.update_layout(bargap=profile["bargap"], margin=profile["margin"])
            _apply_single_category_focus(fig, "x")
        if key in {"09_priority_score_by_cooperative", "10_workload_depth"}:
            fig.update_yaxes(type="category")
            cat_count = len(_unique_axis_values(fig, "y"))
            profile = _category_layout_profile(cat_count)
            fig.update_layout(bargap=profile["bargap"], margin=profile["margin"])
            _apply_single_category_focus(fig, "y")
        if key in {"06_top_child_items", "07_top_household_items", "08_top_community_items"}:
            fig.update_yaxes(type="category")
            cat_count = len(_unique_axis_values(fig, "y"))
            profile = _category_layout_profile(cat_count)
            fig.update_layout(bargap=profile["bargap"], margin=profile["margin"])
            _apply_single_category_focus(fig, "y")
        if key in {"03_hazard_signature_heatmap"}:
            fig.update_yaxes(type="category")
        if key in {"03_hazard_signature_heatmap"}:
            fig.update_xaxes(tickangle=-30)

    return figures


def export_png_pack(
    figures: dict[str, go.Figure],
    out_dir: str = "outputs/charts",
    width: int = 2000,
    height: int = 1000,
    scale: int = 2,
) -> list[str]:
    _register_ici_template()
    output = Path(out_dir)
    output.mkdir(parents=True, exist_ok=True)

    written: list[str] = []
    try:
        for chart_name in PNG_ORDER:
            fig = figures.get(chart_name)
            if fig is None:
                continue
            path = output / f"{chart_name}.png"
            if chart_name in {"03_hazard_signature_heatmap"}:
                fig.write_image(path, width=2400, height=height, scale=scale)
            else:
                fig.write_image(path, width=width, height=height, scale=scale)
            written.append(str(path))
    except Exception as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "PNG export failed. Ensure kaleido is installed and Chrome is available for static image rendering."
        ) from exc

    return written


def export_png_pack_by_cooperative(
    tables: dict[str, pd.DataFrame],
    out_dir: str = "outputs/charts/by_cooperative",
    width: int = 2000,
    height: int = 1000,
    scale: int = 2,
    make_zip: bool = True,
) -> dict[str, Any]:
    output_root = Path(out_dir)
    output_root.mkdir(parents=True, exist_ok=True)

    cl = tables["cl_child_master"].copy()
    cooperatives = sorted(cl["CooperativeLabel"].dropna().astype(str).unique().tolist())

    cooperative_outputs: dict[str, list[str]] = {}
    for cooperative in cooperatives:
        cooperative_safe = _safe_path_token(cooperative)
        cooperative_dir = output_root / f"cooperative_{cooperative_safe}"
        figures = build_figures(tables, cooperative_filter=[cooperative], community_filter=None)
        paths = export_png_pack(
            figures,
            out_dir=str(cooperative_dir),
            width=width,
            height=height,
            scale=scale,
        )
        cooperative_outputs[cooperative] = paths

    zip_path: str | None = None
    if make_zip:
        zip_file = output_root / "cooperative_png_packs.zip"
        with ZipFile(zip_file, "w", compression=ZIP_DEFLATED) as zf:
            for cooperative, paths in cooperative_outputs.items():
                cooperative_safe = _safe_path_token(cooperative)
                for p in paths:
                    path_obj = Path(p)
                    if path_obj.exists():
                        arc = Path(f"cooperative_{cooperative_safe}") / path_obj.name
                        zf.write(path_obj, arcname=str(arc))
        zip_path = str(zip_file)

    return {
        "cooperative_count": len(cooperatives),
        "cooperative_outputs": cooperative_outputs,
        "zip_path": zip_path,
        "out_dir": str(output_root),
    }


def export_png_pack_by_district(  # backward compatibility shim
    tables: dict[str, pd.DataFrame],
    out_dir: str = "outputs/charts/by_cooperative",
    width: int = 2000,
    height: int = 1000,
    scale: int = 2,
    make_zip: bool = True,
) -> dict[str, Any]:
    return export_png_pack_by_cooperative(
        tables=tables,
        out_dir=out_dir,
        width=width,
        height=height,
        scale=scale,
        make_zip=make_zip,
    )


def build_outputs(
    input_path: str,
    export_charts: bool = False,
    export_charts_by_cooperative: bool = False,
    zip_cooperative_pack: bool = True,
) -> dict[str, Any]:
    workbook = Path(input_path)
    if not workbook.exists():
        raise FileNotFoundError(f"Input file not found: {workbook}")

    TABLE_DIR.mkdir(parents=True, exist_ok=True)
    CHART_DIR.mkdir(parents=True, exist_ok=True)

    xl = pd.ExcelFile(workbook)
    for sheet in REQUIRED_SHEETS.values():
        if sheet not in xl.sheet_names:
            raise KeyError(f"Required sheet missing: {sheet}")

    cicl = pd.read_excel(workbook, sheet_name=REQUIRED_SHEETS["cicl"])
    community = pd.read_excel(workbook, sheet_name=REQUIRED_SHEETS["community"])
    rem_child = pd.read_excel(workbook, sheet_name=REQUIRED_SHEETS["rem_child"])
    rem_hh = pd.read_excel(workbook, sheet_name=REQUIRED_SHEETS["rem_hh"])
    rem_com = pd.read_excel(workbook, sheet_name=REQUIRED_SHEETS["rem_com"])

    cicl["ChldID"] = _normalize_identifier(cicl["ChldID"])
    cicl["FarmerID"] = _normalize_identifier(cicl["FarmerID"])
    cicl["ComID"] = _normalize_identifier(cicl["ComID"])

    community_lookup = community[
        ["A 05 community code", "A 01 id cooperative", "A 01 cooperative name", "A 04 community name"]
    ].copy()
    community_lookup.columns = ["ComID", "CooperativeID", "CooperativeName", "CommunityName"]
    community_lookup["ComID"] = _normalize_identifier(community_lookup["ComID"])
    community_lookup["CooperativeID"] = _normalize_identifier(community_lookup["CooperativeID"])
    community_lookup["CooperativeName"] = _normalize_label(community_lookup["CooperativeName"])
    community_lookup["CooperativeLabel"] = _build_cooperative_label(
        community_lookup["CooperativeName"], community_lookup["CooperativeID"]
    )
    community_lookup["CommunityName"] = _normalize_label(community_lookup["CommunityName"])
    community_lookup = community_lookup.drop_duplicates("ComID")

    hazard_cols = _detect_hazard_columns(cicl)
    hazard_numeric = cicl[hazard_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    cicl["HazardCount"] = hazard_numeric.sum(axis=1).astype(int)

    cl_base = cicl[["ChldID", "FarmerID", "ComID", "HazardCount"]].drop_duplicates(subset=["ChldID"]).copy()
    cl_base = cl_base.merge(community_lookup, on="ComID", how="left")

    unmatched = cl_base[
        cl_base["CooperativeID"].isna() | cl_base["CooperativeName"].isna() | cl_base["CommunityName"].isna()
    ]["ComID"].dropna().unique().tolist()

    hazard_long = cicl[["ChldID", "FarmerID", "ComID"] + hazard_cols].copy()
    hazard_long = hazard_long.merge(community_lookup, on="ComID", how="left")
    hazard_long = hazard_long.melt(
        id_vars=[
            "ChldID",
            "FarmerID",
            "ComID",
            "CooperativeID",
            "CooperativeName",
            "CooperativeLabel",
            "CommunityName",
        ],
        value_vars=hazard_cols,
        var_name="HazardLabel",
        value_name="Flag",
    )
    hazard_long["Flag"] = pd.to_numeric(hazard_long["Flag"], errors="coerce").fillna(0)
    hazard_long = hazard_long[hazard_long["Flag"] == 1].copy()
    hazard_long["HazardLabel"] = hazard_long["HazardLabel"].str.replace(r"^[a-sA-S]\s+", "", regex=True)
    hazards_long = hazard_long[
        [
            "ChldID",
            "FarmerID",
            "ComID",
            "CooperativeID",
            "CooperativeName",
            "CooperativeLabel",
            "CommunityName",
            "HazardLabel",
        ]
    ].drop_duplicates()

    child_id_col = _find_column(list(rem_child.columns), "D 07 child clmrs code", ["child clmrs code", "clmrs code"])
    child_label_col = _find_column(
        list(rem_child.columns),
        "D 32 label remediation",
        ["d 32", "child remediation", "label remediation"],
    )
    child_other_col = _find_optional_column(
        list(rem_child.columns),
        "D 32 remediation other",
        ["remediation other", "other", "d 32"],
    )
    hh_id_col = _find_column(list(rem_hh.columns), "D 07 child clmrs code", ["child clmrs code", "clmrs code"])
    hh_label_col = _find_column(list(rem_hh.columns), "D 33 label remediation", ["d 33", "label remediation"])
    hh_other_col = _find_optional_column(
        list(rem_hh.columns),
        "D 33 remediation other",
        ["remediation other", "other", "d 33"],
    )
    com_id_col = _find_column(list(rem_com.columns), "D 07 child clmrs code", ["child clmrs code", "clmrs code"])
    com_label_col = _find_column(list(rem_com.columns), "D 34 label remediation", ["d 34", "label remediation"])
    com_other_col = _find_optional_column(
        list(rem_com.columns),
        "D 34 remediation other",
        ["remediation other", "other", "d 34"],
    )

    cl_child_set = set(cl_base["ChldID"].dropna().tolist())

    def _prep_rem(
        df: pd.DataFrame, id_col: str, label_col: str, other_col: str | None = None
    ) -> tuple[pd.DataFrame, dict[str, int]]:
        out = df.copy()
        out["ChldID"] = _normalize_identifier(out[id_col])
        other_series = out[other_col] if (other_col and other_col in out.columns) else None
        out["Item"] = _compose_item_with_other(out[label_col], other_series)
        out = out[out["ChldID"].notna() & out["Item"].notna()].copy()

        stats = {
            "rows_before": len(df),
            "rows_after": len(out[out["ChldID"].isin(cl_child_set)]),
            "children_before": int(out["ChldID"].nunique()),
            "children_after": int(out[out["ChldID"].isin(cl_child_set)]["ChldID"].nunique()),
        }
        out = out[out["ChldID"].isin(cl_child_set)].copy()
        return out, stats

    rem_child_clean, rem_child_stats = _prep_rem(rem_child, child_id_col, child_label_col, child_other_col)
    rem_hh_clean, rem_hh_stats = _prep_rem(rem_hh, hh_id_col, hh_label_col, hh_other_col)
    rem_com_clean, rem_com_stats = _prep_rem(rem_com, com_id_col, com_label_col, com_other_col)

    geo_cols = [
        "ChldID",
        "FarmerID",
        "ComID",
        "CooperativeID",
        "CooperativeName",
        "CooperativeLabel",
        "CommunityName",
    ]

    rem_child_long = rem_child_clean[["ChldID", "Item"]].rename(columns={"Item": "ChildItem"}).merge(
        cl_base[geo_cols], on="ChldID", how="left"
    )
    rem_child_long = rem_child_long[
        [
            "ChldID",
            "FarmerID",
            "ComID",
            "CooperativeID",
            "CooperativeName",
            "CooperativeLabel",
            "CommunityName",
            "ChildItem",
        ]
    ]
    rem_child_long = rem_child_long.drop_duplicates(["ChldID", "ChildItem"]).reset_index(drop=True)

    rem_hh_long = rem_hh_clean[["ChldID", "Item"]].rename(columns={"Item": "HhItem"}).merge(
        cl_base[geo_cols], on="ChldID", how="left"
    )
    rem_hh_long = rem_hh_long[
        ["FarmerID", "HhItem", "CooperativeID", "CooperativeName", "CooperativeLabel", "CommunityName", "ComID"]
    ]
    rem_hh_long = rem_hh_long.drop_duplicates(["FarmerID", "HhItem"]).reset_index(drop=True)

    rem_com_priority = rem_com_clean[["ChldID", "Item"]].rename(columns={"Item": "ComItem"}).merge(
        cl_base[geo_cols], on="ChldID", how="left"
    )
    rem_com_priority = rem_com_priority[
        ["ComID", "CooperativeID", "CooperativeName", "CooperativeLabel", "CommunityName", "ComItem"]
    ]
    rem_com_priority = rem_com_priority.drop_duplicates(["ComID", "ComItem"]).reset_index(drop=True)
    rem_com_priority["FlagNeeded"] = 1

    cl_child_master = cl_base[
        [
            "ChldID",
            "FarmerID",
            "ComID",
            "CooperativeID",
            "CooperativeName",
            "CooperativeLabel",
            "CommunityName",
            "HazardCount",
        ]
    ].copy()

    child_counts = rem_child_long.groupby("ChldID")["ChildItem"].nunique().rename("ChildRemCount")
    hh_counts = rem_hh_long.groupby("FarmerID")["HhItem"].nunique().rename("HhRemCount")
    com_counts = rem_com_priority.groupby("ComID")["ComItem"].nunique().rename("ComRemCount")

    cl_child_master = cl_child_master.merge(child_counts, on="ChldID", how="left")
    cl_child_master = cl_child_master.merge(hh_counts, on="FarmerID", how="left")
    cl_child_master = cl_child_master.merge(com_counts, on="ComID", how="left")

    for col in ["ChildRemCount", "HhRemCount", "ComRemCount"]:
        cl_child_master[col] = cl_child_master[col].fillna(0).astype(int)

    cl_child_master["TotalRemCount"] = (
        cl_child_master["ChildRemCount"] + cl_child_master["HhRemCount"] + cl_child_master["ComRemCount"]
    )
    cl_child_master["SpreadBucket"] = cl_child_master["TotalRemCount"].apply(_spread_bucket)
    cl_child_master["RemMixCategory"] = cl_child_master.apply(
        lambda row: _rem_mix_category(row["ChildRemCount"], row["HhRemCount"], row["ComRemCount"]),
        axis=1,
    )
    cl_child_master["HazardTier"] = cl_child_master["HazardCount"].apply(_hazard_tier)

    cl_child_master = cl_child_master[
        [
            "ChldID",
            "FarmerID",
            "ComID",
            "CooperativeID",
            "CooperativeName",
            "CooperativeLabel",
            "CommunityName",
            "HazardCount",
            "HazardTier",
            "ChildRemCount",
            "HhRemCount",
            "ComRemCount",
            "TotalRemCount",
            "SpreadBucket",
            "RemMixCategory",
        ]
    ].sort_values(["CooperativeLabel", "CommunityName", "ChldID"])

    planning = (
        cl_child_master.groupby(["CooperativeID", "CooperativeName", "CooperativeLabel"], dropna=False)
        .agg(
            cl_children=("ChldID", "nunique"),
            cl_households=("FarmerID", "nunique"),
            avg_hazards=("HazardCount", "mean"),
            avg_total_rem=("TotalRemCount", "mean"),
            pct_zero_rem=("TotalRemCount", lambda s: (s == 0).mean()),
        )
        .reset_index()
        .sort_values("CooperativeLabel")
    )

    cl_child_master.to_csv(TABLE_DIR / "cl_child_master.csv", index=False)
    hazards_long.to_csv(TABLE_DIR / "hazards_long.csv", index=False)
    rem_child_long.to_csv(TABLE_DIR / "rem_child_long.csv", index=False)
    rem_hh_long.to_csv(TABLE_DIR / "rem_hh_long.csv", index=False)
    rem_com_priority.to_csv(TABLE_DIR / "rem_com_priority.csv", index=False)
    planning.to_excel(TABLE_DIR / "planning_quantities_by_cooperative.xlsx", index=False)

    tables = {
        "cl_child_master": cl_child_master,
        "hazards_long": hazards_long,
        "rem_child_long": rem_child_long,
        "rem_hh_long": rem_hh_long,
        "rem_com_priority": rem_com_priority,
        "planning_quantities_by_cooperative": planning,
    }

    chart_paths: list[str] = []
    if export_charts:
        all_cooperatives = sorted(cl_child_master["CooperativeLabel"].dropna().unique().tolist())
        figures = build_figures(tables, cooperative_filter=all_cooperatives, community_filter=None)
        chart_paths = export_png_pack(figures, out_dir=str(CHART_DIR))

    cooperative_export: dict[str, Any] | None = None
    if export_charts_by_cooperative:
        cooperative_export = export_png_pack_by_cooperative(
            tables=tables,
            out_dir=str(CHART_DIR / "by_cooperative"),
            width=2000,
            height=1000,
            scale=2,
            make_zip=zip_cooperative_pack,
        )

    logs = {
        "cl_children": int(cl_child_master["ChldID"].nunique()),
        "remediation_stats": {
            "child": rem_child_stats,
            "household": rem_hh_stats,
            "community": rem_com_stats,
        },
        "unmatched_comid_count": len(unmatched),
        "unmatched_comid_sample": unmatched[:10],
        "output_tables": [
            str(TABLE_DIR / "cl_child_master.csv"),
            str(TABLE_DIR / "hazards_long.csv"),
            str(TABLE_DIR / "rem_child_long.csv"),
            str(TABLE_DIR / "rem_hh_long.csv"),
            str(TABLE_DIR / "rem_com_priority.csv"),
            str(TABLE_DIR / "planning_quantities_by_cooperative.xlsx"),
        ],
        "output_charts": chart_paths,
        "cooperative_export": cooperative_export,
    }

    return {"tables": tables, "logs": logs}


def _print_acceptance_logs(logs: dict[str, Any]) -> None:
    print(f"CL children in CICL: {logs['cl_children']}")
    print("Remediation children before/after case-only filtering:")
    print(
        "- child: unique_children={children_before}->{children_after}, rows={rows_before}->{rows_after}".format(
            **logs["remediation_stats"]["child"]
        )
    )
    print(
        "- household: unique_children={children_before}->{children_after}, rows={rows_before}->{rows_after}".format(
            **logs["remediation_stats"]["household"]
        )
    )
    print(
        "- community: unique_children={children_before}->{children_after}, rows={rows_before}->{rows_after}".format(
            **logs["remediation_stats"]["community"]
        )
    )

    print(f"ComID unmatched to Community Profile: {logs['unmatched_comid_count']}")
    if logs["unmatched_comid_count"] > 0:
        print(f"Sample unmatched ComID values: {logs['unmatched_comid_sample']}")

    print("Written tables:")
    for path in logs["output_tables"]:
        print(f"- {path}")

    if logs["output_charts"]:
        print("Written charts:")
        for path in logs["output_charts"]:
            print(f"- {path}")
    if logs.get("cooperative_export"):
        cooperative_export = logs["cooperative_export"]
        print(f"Written cooperative chart packs: {cooperative_export['cooperative_count']} cooperatives")
        print(f"- base folder: {cooperative_export['out_dir']}")
        if cooperative_export.get("zip_path"):
            print(f"- zip: {cooperative_export['zip_path']}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CL case remediation pipeline")
    parser.add_argument("--input", type=str, default=None, help="Path to input Excel workbook")
    parser.add_argument("--export-charts", action="store_true", help="Export PNG chart pack")
    parser.add_argument(
        "--export-charts-by-cooperative",
        action="store_true",
        help="Export one PNG chart pack per CooperativeLabel",
    )
    parser.add_argument(
        "--no-zip-cooperative-pack",
        action="store_true",
        help="Do not create ZIP for per-cooperative chart packs",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    workbook = _resolve_input_path(args.input)
    result = build_outputs(
        str(workbook),
        export_charts=args.export_charts,
        export_charts_by_cooperative=args.export_charts_by_cooperative,
        zip_cooperative_pack=(not args.no_zip_cooperative_pack),
    )
    _print_acceptance_logs(result["logs"])


if __name__ == "__main__":
    main()

