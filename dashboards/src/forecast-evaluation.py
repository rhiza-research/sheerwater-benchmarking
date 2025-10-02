#!/usr/bin/env python3
"""
Forecast Evaluation Dashboard

Complex dashboard with 16 variables and 7 panels.
Generated from: build/forecast-evaluation.json
"""

from grafana_foundation_sdk.builders.dashboard import Dashboard, Row, Panel, AnnotationQuery, CustomVariable, QueryVariable
from grafana_foundation_sdk.cog import variants as cogvariants
from grafana_foundation_sdk.cog import builder
from typing import Any, Optional, Self
from grafana_foundation_sdk.models.dashboard import (
    DataSourceRef,
    GridPos,
    FieldConfigSource,
    FieldConfig,
    ThresholdsConfig,
    Threshold,
    ThresholdsMode,
    VariableOption,
    VariableHide,
    VariableRefresh,
    VariableSort,
)
from grafana_foundation_sdk.models.common import TimeZoneUtc
import json


def get_datasource_config(datasource_config_file):
    """Get the datasource config from the config file"""
    with open(datasource_config_file, "r") as f:
        datasource_config = json.load(f)
    return {key: DataSourceRef(type_val=key, uid=value) for key, value in datasource_config.items()}


class PostgreSQLQuery(cogvariants.Dataquery):
    """PostgreSQL query following Foundation SDK patterns"""
    # This is based on the example code from https://github.com/grafana/grafana-foundation-sdk/blob/main/examples/python/custom-query/src/customquery.py
    ref_id: Optional[str]
    hide: Optional[bool]
    raw_sql: str
    format: str
    raw_query: bool
    editor_mode: str
    sql: dict[str, Any]
    datasource: Optional[dict[str, str]]

    def __init__(
        self,
        raw_sql: str,
        ref_id: Optional[str] = None,
        hide: Optional[bool] = None,
        format: str = "table",
        raw_query: bool = True,
        editor_mode: str = "code",
        sql: Optional[dict[str, Any]] = None,
        datasource: Optional[dict[str, str]] = None
    ):
        self.raw_sql = raw_sql
        self.ref_id = ref_id
        self.hide = hide
        self.format = format
        self.raw_query = raw_query
        self.editor_mode = editor_mode
        self.sql = sql or {}
        self.datasource = datasource

    def to_json(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "rawSql": self.raw_sql,
            "format": self.format,
            "rawQuery": self.raw_query,
            "editorMode": self.editor_mode,
            "sql": self.sql,
        }
        if self.ref_id is not None:
            payload["refId"] = self.ref_id
        if self.hide is not None:
            payload["hide"] = self.hide
        if self.datasource is not None:
            payload["datasource"] = self.datasource
        return payload

    @classmethod
    def from_json(cls, data: dict[str, Any]) -> Self:
        args: dict[str, Any] = {}
        if "rawSql" in data:
            args["raw_sql"] = data["rawSql"]
        if "refId" in data:
            args["ref_id"] = data["refId"]
        if "hide" in data:
            args["hide"] = data["hide"]
        if "format" in data:
            args["format"] = data["format"]
        if "rawQuery" in data:
            args["raw_query"] = data["rawQuery"]
        if "editorMode" in data:
            args["editor_mode"] = data["editorMode"]
        if "sql" in data:
            args["sql"] = data["sql"]
        if "datasource" in data:
            args["datasource"] = data["datasource"]
        return cls(**args)


class PostgreSQLQueryBuilder(builder.Builder[PostgreSQLQuery]):
    """PostgreSQL query builder following Foundation SDK patterns"""
    __internal: PostgreSQLQuery

    def __init__(self, raw_sql: str):
        self.__internal = PostgreSQLQuery(raw_sql=raw_sql)

    def build(self) -> PostgreSQLQuery:
        return self.__internal

    def ref_id(self, ref_id: str) -> Self:
        self.__internal.ref_id = ref_id
        return self

    def hide(self, hide: bool) -> Self:
        self.__internal.hide = hide
        return self

    def format(self, format_type: str) -> Self:
        self.__internal.format = format_type
        return self

    def raw_query(self, raw_query: bool) -> Self:
        self.__internal.raw_query = raw_query
        return self

    def editor_mode(self, mode: str) -> Self:
        self.__internal.editor_mode = mode
        return self

    def sql(self, sql_config: dict[str, Any]) -> Self:
        self.__internal.sql = sql_config
        return self

    def datasource(self, datasource: dict[str, str]) -> Self:
        self.__internal.datasource = datasource
        return self


# TODO: this is needed to match the original JSON, but maybe its not needed to get good dashboards. 
# We should test removing it and see if the dashboards are fine. 
# If so I would rather not mess with _internal fields if we dont need to.
def clean_variable_defaults(var):
    """Remove unwanted default fields from variables to match original JSON"""
    var.allow_custom_value(False)
    var.multi(False)
    var._internal.auto = None
    var._internal.auto_count = None
    var._internal.auto_min = None
    var._internal.skip_url_sync = None
    return var


def build_dashboard(datasource_config_file):
    """Build the Forecast Evaluation dashboard"""
    datasource_config = get_datasource_config(datasource_config_file)
    postgres_ds = datasource_config["grafana-postgresql-datasource"]

    dashboard = Dashboard("Forecast Evaluation")
    dashboard.uid("ee2jzeymn1o8wf")
    dashboard.id(9)
    dashboard.tags([])
    dashboard.links([])
    dashboard.refresh("")
    dashboard.time("now-6h", "now")
    dashboard.timezone(TimeZoneUtc)

    dashboard.preload(False)

    # Add annotations
    dashboard.annotation(
        AnnotationQuery()
        .name("Annotations & Alerts")
        .datasource(DataSourceRef(type_val="grafana", uid="-- Grafana --"))
        .enable(True)
        .hide(True)
        .icon_color("rgba(0, 211, 255, 1)")
        .type("dashboard")
        .built_in(1)
    )
    # Add all 16 variables

    # 1. Metric (custom)
    metric_options = [
        VariableOption(text="MAE", value="mae", selected=False),
        VariableOption(text="CRPS", value="crps", selected=False),
        VariableOption(text="RMSE", value="rmse", selected=False),
        VariableOption(text="ACC", value="acc", selected=True),
        VariableOption(text="Bias", value="bias", selected=False),
        VariableOption(text="SMAPE", value="smape", selected=False),
        VariableOption(text="SEEPS", value="seeps", selected=False),
        VariableOption(text="Heidke (1/5/10/20mm)", value="heidke-1-5-10-20", selected=False),
        VariableOption(text="POD 1mm", value="pod-1", selected=False),
        VariableOption(text="POD 5mm", value="pod-5", selected=False),
        VariableOption(text="POD 10mm", value="pod-10", selected=False),
        VariableOption(text="FAR 1mm", value="far-1", selected=False),
        VariableOption(text="FAR 5mm", value="far-5", selected=False),
        VariableOption(text="FAR 10mm", value="far-10", selected=False),
        VariableOption(text="ETS 1mm", value="ets-1", selected=False),
        VariableOption(text="ETS 5mm", value="ets-5", selected=False),
        VariableOption(text="ETS 10mm", value="ets-10", selected=False)
    ]
    var = CustomVariable("metric")
    var.label("Metric")
    var.options(metric_options)
    var.current(VariableOption(text="acc", value="acc"))
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    # 2. Baseline (query)
    var = QueryVariable("baseline")
    var.label("Baseline")
    # TODO: should this be moved to a .sql file?
    # maybe each panel should have a convention for the panel sql, js, and css files.
    var.query("""select 
forecast as __value,
case
 when forecast = 'salient' then 'Salient' 
when forecast = 'climatology_2015' then 'Clim 1985-2014' 
when forecast = 'climatology_trend_2015' then 'Clim + Trend' 
when forecast = 'climatology_rolling' then 'Clim Rolling' 
when forecast = 'ecmwf_ifs_er' then 'ECMWF IFS ER' 
when forecast = 'ecmwf_ifs_er_debiased' then 'ECMWF IFS ER Debiased' 
when forecast = 'fuxi' then 'FuXi S2S' 
 else forecast
end as __text
from "${precip_tab_name_baseline}" where week1 is not null""")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="Clim 1985-2014", value="climatology_2015"))
    var.refresh(VariableRefresh.ON_DASHBOARD_LOAD)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    # 3. Truth (custom)
    truth_options = [
        VariableOption(text="ERA5", value="era5", selected=True)
    ]
    var = CustomVariable("truth")
    var.label("Ground Truth")
    var.options(truth_options)
    var.current(VariableOption(text="era5", value="era5"))
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    # 4. Grid (custom)
    grid_options = [
        VariableOption(text="1.5", value="global1_5", selected=True),
        VariableOption(text="0.25", value="global0_25", selected=False)
    ]
    var = CustomVariable("grid")
    var.label("Grid")
    var.options(grid_options)
    var.current(VariableOption(text="global1_5", value="global1_5"))
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    # 5. Region (custom)
    region_options = [
        VariableOption(text="Global", value="global", selected=False),
        VariableOption(text="Africa", value="africa", selected=True),
        VariableOption(text="East Africa", value="east_africa", selected=False),
        VariableOption(text="Nigeria", value="nigeria", selected=False),
        VariableOption(text="Kenya", value="kenya", selected=False),
        VariableOption(text="Ethiopia", value="ethiopia", selected=False),
        VariableOption(text="Chile", value="chile", selected=False),
        VariableOption(text="Bangladesh", value="bangladesh", selected=False),
        VariableOption(text="CONUS", value="conus", selected=False)
    ]
    var = CustomVariable("region")
    var.label("Region")
    var.options(region_options)
    var.current(VariableOption(text="africa", value="africa"))
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    # 6. Time grouping (custom)
    time_grouping_options = [
        VariableOption(text="None", value="None", selected=True),
        VariableOption(text="Month of Year", value="month_of_year", selected=False),
        VariableOption(text="Year", value="year", selected=False)
    ]
    var = CustomVariable("time_grouping")
    var.label("Time Grouping")
    var.options(time_grouping_options)
    var.current(VariableOption(text="None", value="None"))
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    # 7-16: Query variables (hidden, computed from other variables)
    var = QueryVariable("tmp2m_tab_name")
    var.query("select * from md5('summary_metrics_table/2022-12-31_${grid}_lsm_${metric}_${region}_2016-01-01_${time_grouping}_${truth}_tmp2m')")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="603561dbe136e8d30da05b8eb27b3708", value="603561dbe136e8d30da05b8eb27b3708"))
    var.hide(VariableHide.HIDE_VARIABLE)
    var.refresh(VariableRefresh.ON_TIME_RANGE_CHANGED)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    var = QueryVariable("seasonal_tmp2m_tab_name")
    var.query("select * from md5('seasonal_metrics_table/2022-12-31_${grid}_lsm_${metric}_${region}_2016-01-01_${time_grouping}_${truth}_tmp2m')")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="2585031bf3d9d2576182203d8d37227d", value="2585031bf3d9d2576182203d8d37227d"))
    var.hide(VariableHide.HIDE_VARIABLE)
    var.refresh(VariableRefresh.ON_TIME_RANGE_CHANGED)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    var = QueryVariable("precip_tab_name")
    var.query("select * from md5('summary_metrics_table/2022-12-31_${grid}_lsm_${metric}_${region}_2016-01-01_${time_grouping}_${truth}_precip')")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="ded8af931c81e7f097229a75c4e4d0f0", value="ded8af931c81e7f097229a75c4e4d0f0"))
    var.hide(VariableHide.HIDE_VARIABLE)
    var.refresh(VariableRefresh.ON_TIME_RANGE_CHANGED)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    var = QueryVariable("seasonal_precip_tab_name")
    var.query("select * from md5('seasonal_metrics_table/2022-12-31_${grid}_lsm_${metric}_${region}_2016-01-01_${time_grouping}_${truth}_precip')")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="371a2b24d7253ce68d31373c3e388bff", value="371a2b24d7253ce68d31373c3e388bff"))
    var.hide(VariableHide.HIDE_VARIABLE)
    var.refresh(VariableRefresh.ON_TIME_RANGE_CHANGED)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    var = QueryVariable("time_filter_filter_query")
    var.query("""select 
CASE
    WHEN v.g = 'None' THEN 'select v.* from (values (''None'')) v(t)'
    ELSE 'select distinct time from "${precip_tab_name}"'
END
from (values ('$time_grouping')) v(g)""")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="select v.* from (values ('None')) v(t)", value="select v.* from (values ('None')) v(t)"))
    var.hide(VariableHide.HIDE_VARIABLE)
    var.refresh(VariableRefresh.ON_DASHBOARD_LOAD)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    var = QueryVariable("time_filter")
    var.label("Time Filter")
    var.query("${time_filter_filter_query:raw}")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="None", value="None"))
    var.refresh(VariableRefresh.ON_DASHBOARD_LOAD)
    var.sort(VariableSort.NUMERICAL_ASC)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    var = QueryVariable("time_filter_query")
    var.query("""select
CASE
    WHEN v.g = 'None' THEN ''
    ELSE 'where time = ' || v.t
END
from (values ('$time_filter', '$time_grouping')) v(t, g)""")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="", value=""))
    var.hide(VariableHide.HIDE_VARIABLE)
    var.refresh(VariableRefresh.ON_DASHBOARD_LOAD)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    var = QueryVariable("tmp2m_tab_name_biweekly")
    var.query("select * from md5('biweekly_summary_metrics_table/2022-12-31_${grid}_lsm_${metric}_${region}_2016-01-01_${time_grouping}_${truth}_tmp2m')")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="f0cccbbc24afc419c5ef96b316a3cfd7", value="f0cccbbc24afc419c5ef96b316a3cfd7"))
    var.hide(VariableHide.HIDE_VARIABLE)
    var.refresh(VariableRefresh.ON_TIME_RANGE_CHANGED)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    var = QueryVariable("precip_tab_name_biweekly")
    var.query("select * from md5('biweekly_summary_metrics_table/2022-12-31_${grid}_lsm_${metric}_${region}_2016-01-01_${time_grouping}_${truth}_precip')")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="168940b89ad339e3e76045958cb8ea3e", value="168940b89ad339e3e76045958cb8ea3e"))
    var.hide(VariableHide.HIDE_VARIABLE)
    var.refresh(VariableRefresh.ON_TIME_RANGE_CHANGED)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    var = QueryVariable("precip_tab_name_baseline")
    var.query("select * from md5('summary_metrics_table/2022-12-31_${grid}_lsm_${metric}_${region}_2016-01-01_${time_grouping}_${truth}_precip')")
    var.datasource(postgres_ds)
    var.current(VariableOption(text="ded8af931c81e7f097229a75c4e4d0f0", value="ded8af931c81e7f097229a75c4e4d0f0"))
    var.hide(VariableHide.HIDE_VARIABLE)
    var.refresh(VariableRefresh.ON_TIME_RANGE_CHANGED)
    clean_variable_defaults(var)
    dashboard.with_variable(var)

    # Load large text content from files
    with open('./src/forecast-eval-metric-desc.txt', 'r') as f:
        metric_desc_content = f.read()

    with open('./src/forecast-eval-caveats.txt', 'r') as f:
        caveats_content = f.read()
    
    with open('./src/forecast-evaluation-after-render.js', 'r') as f:
        after_render_script = f.read()

    # Panel 1: Metric description (marcusolsson-dynamictext-panel)
    panel1 = Panel()
    panel1.title("")
    panel1.type("marcusolsson-dynamictext-panel")
    panel1.transparent(True)
    panel1.datasource(postgres_ds)
    panel1.grid_pos(GridPos(h=4, w=24, x=0, y=0))
    panel1.id(5)
    panel1.plugin_version("5.6.0")
    panel1.repeat_direction(None)
    panel1.field_config(
        FieldConfigSource(
            defaults=FieldConfig(
                thresholds=ThresholdsConfig(
                    mode=ThresholdsMode.ABSOLUTE,
                    steps=[
                        Threshold(color="green"),
                        Threshold(value=80, color="red")
                    ]
                )
            ),
            overrides=[]
        )
    )
    panel1.options({
        "afterRender": after_render_script,
        "content": metric_desc_content,
        "contentPartials": [],
        "defaultContent": "The query didn't return any results.",
        "editor": {
            "format": "auto",
            "language": "markdown"
        },
        "editors": [],
        "externalStyles": [],
        "helpers": "",
        "renderMode": "everyRow",
        "styles": "",
        "wrap": True
    })


    # Use custom PostgreSQL query builder following Foundation SDK patterns
    postgresql_query = (
        PostgreSQLQueryBuilder("select '${metric}' as metric\n")
        .ref_id("A")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    )

    panel1.with_target(postgresql_query)
    dashboard.with_panel(panel1)

    # Row 1: Notes and Caveats (collapsed with nested panel)
    panel_caveats = Panel()
    panel_caveats.title("")
    panel_caveats.type("marcusolsson-dynamictext-panel")
    panel_caveats.transparent(True)
    panel_caveats.datasource(postgres_ds)
    panel_caveats.grid_pos(GridPos(h=12, w=24, x=0, y=5))
    panel_caveats.id(8)
    panel_caveats.plugin_version("5.6.0")
    panel_caveats.repeat_direction(None)
    panel_caveats.field_config(
        FieldConfigSource(
            defaults=FieldConfig(
                thresholds=ThresholdsConfig(
                    mode=ThresholdsMode.ABSOLUTE,
                    steps=[
                        Threshold(color="green"),
                        Threshold(value=80, color="red")
                    ]
                )
            ),
            overrides=[]
        )
    )
    panel_caveats.options({
        "afterRender": after_render_script,
        "content": caveats_content,
        "contentPartials": [],
        "defaultContent": "The query didn't return any results.",
        "editor": {
            "format": "auto",
            "language": "markdown"
        },
        "editors": [],
        "externalStyles": [],
        "helpers": "",
        "renderMode": "everyRow",
        "styles": "",
        "wrap": True
    })
    panel_caveats.with_target(
        PostgreSQLQueryBuilder("select '${metric}' as metric\n")
        .ref_id("A")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    )

    dashboard.with_row(
        Row("Notes and Caveats")
        .id(7)
        .collapsed(True)
        .with_panel(panel_caveats)
    )

    # Row 2: Metrics
    dashboard.with_row(
        Row("Metrics")
        .id(9)
        .collapsed(False)
        .grid_pos(GridPos(h=1, w=24, x=0, y=5))
    )

    # Load JavaScript files
    with open('./src/forecast-evaluation-weekly-temp.js', 'r') as f:
        weekly_temp_script = f.read()

    with open('./src/forecast-evaluation-weekly-precip.js', 'r') as f:
        weekly_precip_script = f.read()

    with open('./src/forecast-evaluation-monthly-temp.js', 'r') as f:
        monthly_temp_script = f.read()

    with open('./src/forecast-evaluation-monthly-precip.js', 'r') as f:
        monthly_precip_script = f.read()


    # Panel 2: Weekly Temperature
    panel2 = Panel()
    panel2.title("")
    panel2.type("nline-plotlyjs-panel")
    panel2.plugin_version("1.8.2")
    panel2.transparent(True)
    panel2.datasource(postgres_ds)
    panel2.grid_pos(GridPos(h=12, w=12, x=0, y=6))
    panel2.id(1)
    panel2.repeat_direction(None)
    panel2.options({
        "allData": {},
        "config": {},
        "data": [],
        "imgFormat": "png",
        "layout": {
            "font": {"family": "Inter, sans-serif"},
            "margin": {"b": 4, "l": 0, "r": 0, "t": 0},
            "paper_bgcolor": "#F4F5F5",
            "plog_bgcolor": "#F4F5F5",
            "title": {
                "align": "left",
                "automargin": True,
                "font": {"color": "black", "family": "Inter, sans-serif", "size": 14, "weight": 500},
                "pad": {"b": 30},
                "text": "Weekly",
                "x": 0,
                "xanchor": "left"
            },
            "xaxis": {"automargin": True, "autorange": True, "type": "date"},
            "yaxis": {"automargin": True, "autorange": True}
        },
        "onclick": "",
        "resScale": 2,
        "script": weekly_temp_script,
        "syncTimeRange": False,
        "timeCol": ""
    })
    panel2.field_config(
        FieldConfigSource(
            defaults=FieldConfig(),
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "forecast"},
                    "properties": []
                },
                {
                    "matcher": {"id": "byName", "options": "forecast"},
                    "properties": []
                }
            ]
        )
    )
    panel2.with_target(
        PostgreSQLQueryBuilder("select * from \"$tmp2m_tab_name\"\n ${time_filter_query}")
        .ref_id("A")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .datasource(postgres_ds)
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    ).with_target(
        PostgreSQLQueryBuilder("select '${baseline}'")
        .ref_id("B")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .hide(False)
        .datasource(postgres_ds)
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    )
    dashboard.with_panel(panel2)

    # Panel 3: Weekly Precipitation
    panel3 = Panel()
    panel3.title("")
    panel3.type("nline-plotlyjs-panel")
    panel3.plugin_version("1.8.2")
    panel3.transparent(True)
    panel3.datasource(postgres_ds)
    panel3.grid_pos(GridPos(h=12, w=12, x=12, y=6))
    panel3.id(2)
    panel3.repeat_direction(None)
    panel3.options({
        "allData": {},
        "config": {},
        "data": [],
        "imgFormat": "png",
        "layout": {
            "font": {"family": "Inter, sans-serif"},
            "margin": {"b": 0, "l": 0, "r": 0, "t": 0},
            "paper_bgcolor": "#F4F5F5",
            "plog_bgcolor": "#F4F5F5",
            "title": {
                "align": "left",
                "automargin": True,
                "font": {"color": "black", "family": "Inter, sans-serif", "size": 14, "weight": 500},
                "pad": {"b": 30},
                "test": "blah",  # TODO: probably dont need this
                "text": "Weekly",
                "x": 0,
                "xanchor": "left"
            },
            "xaxis": {"automargin": True, "autorange": True, "type": "date"},
            "yaxis": {"automargin": True, "autorange": True}
        },
        "onclick": "",
        "resScale": 0,
        "script": weekly_precip_script,
        "syncTimeRange": False,
        "timeCol": ""
    })
    panel3.field_config(
        FieldConfigSource(
            defaults=FieldConfig(),
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "forecast"},
                    "properties": []
                },
                {
                    "matcher": {"id": "byName", "options": "forecast"},
                    "properties": []
                }
            ]
        )
    )
    panel3.with_target(
        PostgreSQLQueryBuilder("select * from \"$precip_tab_name\"\n ${time_filter_query}")
        .ref_id("A")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .datasource(postgres_ds)
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    ).with_target(
        PostgreSQLQueryBuilder("select '${baseline}'")
        .ref_id("B")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .hide(False)
        .datasource(postgres_ds)
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    )
    dashboard.with_panel(panel3)

    # Panel 4: Monthly Temperature
    panel4 = Panel()
    panel4.title("")
    panel4.type("nline-plotlyjs-panel")
    panel4.plugin_version("1.8.2")
    panel4.transparent(True)
    panel4.datasource(postgres_ds)
    panel4.grid_pos(GridPos(h=5, w=12, x=0, y=18))
    panel4.id(4)
    panel4.repeat_direction(None)
    panel4.options({
        "allData": {},
        "config": {},
        "data": [],
        "imgFormat": "png",
        "layout": {
            "font": {"family": "Inter, sans-serif"},
            "margin": {"b": 0, "l": 0, "r": 0, "t": 0},
            "paper_bgcolor": "#F4F5F5",
            "plog_bgcolor": "#F4F5F5",
            "title": {
                "align": "left",
                "automargin": True,
                "font": {"color": "black", "family": "Inter, sans-serif", "size": 14, "weight": 500},
                "pad": {"b": 30},
                "x": 0,
                "xanchor": "left"
            },
            "xaxis": {"automargin": True, "autorange": True, "type": "date"},
            "yaxis": {"automargin": True, "autorange": True}
        },
        "onclick": "",
        "resScale": 0,
        "script": monthly_temp_script,
        "syncTimeRange": False,
        "timeCol": ""
    })
    panel4.field_config(
        FieldConfigSource(
            defaults=FieldConfig(),
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "forecast"},
                    "properties": []
                },
                {
                    "matcher": {"id": "byName", "options": "forecast"},
                    "properties": []
                }
            ]
        )
    )
    panel4.with_target(
        PostgreSQLQueryBuilder("select * from \"$seasonal_tmp2m_tab_name\"\n ${time_filter_query}")
        .ref_id("A")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .datasource(postgres_ds)
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    ).with_target(
        PostgreSQLQueryBuilder("select '${baseline}'")
        .ref_id("B")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .hide(False)
        .datasource(postgres_ds)
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    )
    dashboard.with_panel(panel4)

    # Panel 5: Monthly Precipitation
    panel5 = Panel()
    panel5.title("")
    panel5.type("nline-plotlyjs-panel")
    panel5.plugin_version("1.8.2")
    panel5.transparent(True)
    panel5.datasource(postgres_ds)
    panel5.grid_pos(GridPos(h=5, w=12, x=12, y=18))
    panel5.id(3)
    panel5.repeat_direction(None)
    panel5.options({
        "allData": {},
        "config": {},
        "data": [],
        "imgFormat": "png",
        "layout": {
            "font": {"family": "Inter, sans-serif"},
            "margin": {"b": 0, "l": 0, "r": 0, "t": 0},
            "paper_bgcolor": "#F4F5F5",
            "plog_bgcolor": "#F4F5F5",
            "title": {
                "align": "left",
                "automargin": True,
                "font": {"color": "black", "family": "Inter, sans-serif", "size": 14, "weight": 500},
                "pad": {"b": 30},
                "x": 0,
                "xanchor": "left"
            },
            "xaxis": {"automargin": True, "autorange": True, "type": "date"},
            "yaxis": {"automargin": True, "autorange": True}
        },
        "onclick": "",
        "resScale": 0,
        "script": monthly_precip_script,
        "syncTimeRange": False,
        "timeCol": ""
    })
    panel5.field_config(
        FieldConfigSource(
            defaults=FieldConfig(),
            overrides=[
                {
                    "matcher": {"id": "byName", "options": "forecast"},
                    "properties": []
                },
                {
                    "matcher": {"id": "byName", "options": "forecast"},
                    "properties": []
                }
            ]
        )
    )
    panel5.with_target(
        PostgreSQLQueryBuilder("select * from \"$seasonal_precip_tab_name\"\n ${time_filter_query}")
        .ref_id("A")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .datasource(postgres_ds)
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    ).with_target(
        PostgreSQLQueryBuilder("select '${baseline}'")
        .ref_id("B")
        .format("table")
        .raw_query(True)
        .editor_mode("code")
        .hide(False)
        .datasource(postgres_ds)
        .sql({
            "columns": [{"parameters": [], "type": "function"}],
            "groupBy": [{"property": {"type": "string"}, "type": "groupBy"}],
            "limit": 50
        })
    )
    dashboard.with_panel(panel5)

    return dashboard.build()