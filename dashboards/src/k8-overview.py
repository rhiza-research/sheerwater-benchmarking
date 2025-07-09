#!/usr/bin/env python3
from grafana_foundation_sdk.builders.dashboard import Dashboard, Row
from grafana_foundation_sdk.builders import prometheus, timeseries
from grafana_foundation_sdk.models.common import TimeZoneBrowser, VisibilityMode, GraphDrawStyle
from grafana_foundation_sdk.models import units

def build_dashboard():
    """Build the Kubernetes Overview dashboard"""
    dashboard = Dashboard("Kubernetes Overview")
    dashboard.uid("k8s-overview")
    dashboard.tags(["generated", "ephemeral", "foundation-sdk", "kubernetes"])
    dashboard.refresh("10s")
    dashboard.time("now-1h", "now")
    dashboard.timezone(TimeZoneBrowser)
    dashboard.with_row(
        Row("System Metrics3")
        .with_panel(
            timeseries.Panel()
            .title("CPU Usage")
            .unit(units.Percent)
            .min(0)
            .max(100)
            .draw_style(GraphDrawStyle.LINE)
            .show_points(VisibilityMode.NEVER)
            .with_target(
                prometheus.Dataquery()
                .expr('100 - (avg by (instance) (irate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)')
                .legend_format("CPU Usage")
            )
        )
        .with_panel(
            timeseries.Panel()
            .title("Memory Usage")
            .unit(units.BytesIEC)
            .min(0)
            .draw_style(GraphDrawStyle.LINE)
            .show_points(VisibilityMode.NEVER)
            .with_target(
                prometheus.Dataquery()
                .expr('node_memory_MemTotal_bytes - node_memory_MemAvailable_bytes')
                .legend_format("Memory Used")
            )
        )
    )

    dashboard.with_row(
        Row("Kubernetes Overview")
        .with_panel(
            timeseries.Panel()
            .title("Pod CPU Usage")
            .unit(units.Percent)
            .min(0)
            .max(100)
            .draw_style(GraphDrawStyle.LINE)
            .show_points(VisibilityMode.NEVER)
            .with_target(
                prometheus.Dataquery()
                .expr('sum(rate(container_cpu_usage_seconds_total{container!=""}[5m])) by (pod) * 100')
                .legend_format("{{pod}}")
            )
        )
        .with_panel(
            timeseries.Panel()
            .title("Pod Memory Usage")
            .unit(units.BytesIEC)
            .min(0)
            .draw_style(GraphDrawStyle.LINE)
            .show_points(VisibilityMode.NEVER)
            .with_target(
                prometheus.Dataquery()
                .expr('sum(container_memory_working_set_bytes{container!=""}) by (pod)')
                .legend_format("{{pod}}")
            )
        )
    )
    return dashboard.build()