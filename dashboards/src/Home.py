#!/usr/bin/env python3
from grafana_foundation_sdk.builders.dashboard import Dashboard, Row, Panel, AnnotationQuery, TimePicker
from grafana_foundation_sdk.models.dashboard import DataSourceRef, GridPos, FieldConfigSource, FieldConfig, FieldColor, ThresholdsConfig, Threshold
from grafana_foundation_sdk.models.common import TimeZoneUtc
from grafana_foundation_sdk.cog import variants as cogvariants

def build_dashboard():
    """Build the Home dashboard"""
    dashboard = Dashboard("Home")
    dashboard.uid("ee4mze492j0n4d")
    dashboard.id(15)
    dashboard.tags([])
    dashboard.links([])
    dashboard.refresh("")
    dashboard.time("now-6h", "now")
    dashboard.timezone(TimeZoneUtc)
    dashboard.version(34)
    dashboard.week_start("")
    dashboard.timepicker(TimePicker().hidden(True))
    
    # Add annotations
    dashboard.annotation(
        AnnotationQuery()
        .name("Annotations & Alerts")
        .datasource(DataSourceRef(type_val="grafana", uid="-- Grafana --"))
        .enable(True)
        .hide(True)
        .icon_color("rgba(0, 211, 255, 1)")
        .type("dashboard")
    )
    
    # Add the main HTML panel
    dashboard.with_panel(
        Panel()
        .title("")
        .type("gapit-htmlgraphics-panel")
        .transparent(True)
        .datasource(DataSourceRef(type_val="grafana-postgresql-datasource", uid="bdz3m3xs99p1cf"))
        .plugin_version("2.1.1")
        .grid_pos(GridPos(h=19, w=24, x=0, y=0))
        .id(2)
        .options({
                "SVGBaseFix": True,
                "add100Percentage": True,
                "calcsMutation": "none",
                "centerAlignContent": True,
                "codeData": "{\n  \"text\": \"\"\n}",
                "css": """div {
    font-family: 'Karla', Helvetica, Arial, sans-serif;
}

.container {
    text-align: center;
    width: 100%;
    margin: 40px auto;
    padding-bottom: 20px;
}

.title {
    border-radius: 25px;
    width: 70%;
    margin: 20px auto;
    padding: 15px 20px;
    font-size: 28px;
    font-weight: 700; /* Bold weight for title */
    color: #323232; /* Darker for contrast */
    background-color: transparent; /* No box */
    text-shadow: 0 1px 2px rgba(0, 0, 0, 0.1); /* Subtle text shadow */
}

.info {
    font-size: 16px;
    font-weight: 400; /* Regular weight for readability */
    color: #4a4a4a; /* Softer contrast */
    margin: 10px auto 30px auto; /* Space below title and above the next section */
    line-height: 1.6; /* Improved readability */
    max-width: 80%; /* Limit paragraph width for better legibility */
}

.fixed {
    width: 220px;
    height: 200px; /* Ensure consistent height */
    display: inline-block;
    margin: 30px;
    text-align: center; /* Center-align contents */
}

.fixed .label {
    font-size: 18px;
    font-weight: 600; /* Slightly bold for emphasis */
    color: #585858;
    margin: 0 0 10px 0; /* Add space between label and image */
}

.met-table,
.met-map,
.rain-graph,
.spw-plot,
.coming-soon {
    border-radius: 20px;
    width: 220px;
    height: 200px; /* Ensure consistent height for all containers */
    margin: 0 auto;
    border: 2px solid rgba(200, 200, 200, 0.7); /* Use a semi-transparent border */
    display: flex;
    align-items: center;
    justify-content: center;
    background-color: #f1f1f1; /* Clean white background */
    text-align: center;
    position: relative; /* To manage the positioning of the pseudo-element */
}

.coming-soon::before {
    content: "Coming Soon";
    font-size: 18px; /* Larger text */
    font-weight: 600; /* Bold for readability */
    color: #888; /* Neutral color for text */
    text-transform: uppercase; /* Make text uppercase */
    letter-spacing: 1.2px; /* Improve readability */
    position: absolute;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%); /* Perfectly center the text */
}

.met-table {
    background-image: url("https://storage.googleapis.com/sheerwater-public/grafana_table_icon.png");
    background-size: cover;
    background-position: center;
}

.met-map {
    background-image: url("https://storage.googleapis.com/sheerwater-public/grafana_map_icon.png");
    background-size: cover;
    background-position: center;
}

.rain-graph {
    background-image: url("https://storage.googleapis.com/sheerwater-public/rainfall-graph.png");
    background-size: cover;
    background-position: center;    
}

.spw-plot {
    background-image: url("https://storage.googleapis.com/sheerwater-public/kenya-map.png");
    background-size: cover;
    background-position: center;    
}
.met-table:hover,
.met-map:hover,
.rain-graph:hover,
.spw-plot:hover,
.coming-soon:hover {
    transform: scale(1.05);
    border-color: #aaa; /* Change border color on hover */
}""",
                "dynamicData": False,
                "dynamicFieldDisplayValues": False,
                "dynamicHtmlGraphics": False,
                "dynamicProps": False,
                "html": """<div class="container">
    <div class="title">The SheerWater Benchmark: Preliminary Results</div>
    <p class="info">Estimating the size of the ground truth and forecasting problems, regionally and task-specifically.</p>
    
    <!-- <a href="d/eeu7f55ajbvnkb/aim-for-scale-benchmarking">    -->
    <!-- <div class="fixed"> -->
        <!-- <p class="label">Aim for Scale<br>Benchmark Tables</p> -->
        <!-- <div class="coming-soon"></div> -->
        <!-- <div class="met-table"></div> -->
    <!-- </div></a>  -->

    <!-- <div class="fixed">
        <p class="label">Comparing ERA5<br>to Local Data</p>
        <div class="coming-soon"></div>
    </div> -->

    <a href="d/ce6tgp4j3d9fkf/reanalysis-metrics">   
    <div class="fixed">
        <p class="label">Comparing Stations to<br>"Ground Truth" Data</p>
        <div class="rain-graph"></div>
    </div></a>     

    <a href="d/ee2jzeymn1o8wf/summary-results-tables?orgId=1">   
    <div class="fixed">
        <p class="label">Forecast<br>Evaluation Tables</p>
        <div class="met-table"></div>
    </div></a> 
    
    <a href="d/ae39q2k3jv668d/metrics-maps">
    <div class="fixed">
        <p class="label">Forecast<br>Evaluation Maps</p>
        <div class="met-map"></div>
    </div></a>

    <a href="d/dea0hyle4eby8c/suitable-planting-window">
    <div class="fixed">
        <p class="label">Suitable Planting<br>Window Evaluation</p>
        <div class="spw-plot"></div>
    </div></a>    
    
    <!-- <div class="fixed">
        <p class="label">Task Evaluation</p>
        <div class="coming-soon"></div>
    </div> -->
</div>""",
                "onInit": "\n",
                "onInitOnResize": False,
                "onRender": "",
                "overflow": "visible",
                "panelupdateOnMount": False,
                "reduceOptions": {
                    "calcs": []
                },
                "renderOnMount": False,
                "rootCSS": "",
                "useGrafanaScrollbar": True
                }
            )
    )
    
    return dashboard.build()