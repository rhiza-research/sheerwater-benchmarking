#!/usr/bin/env python3
from grafana_foundation_sdk.builders.dashboard import Dashboard, Row
from grafana_foundation_sdk.builders import prometheus, timeseries
from grafana_foundation_sdk.models.common import TimeZoneBrowser, VisibilityMode, GraphDrawStyle
from grafana_foundation_sdk.models import units

def build_dashboard():
    """Build the system overview dashboard"""
    dashboard = Dashboard("System Overview")
    dashboard.uid("system-overview")
    dashboard.tags(["generated", "ephemeral", "foundation-sdk", "system"])
    dashboard.refresh("10s")
    dashboard.time("now-1h", "now")
    dashboard.timezone(TimeZoneBrowser)
    
    # System metrics row
    dashboard.with_row(
        Row("System Metrics")
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
                .expr('rate(node_cpu_seconds_total{mode="user"}[5m])')
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
    
    # Kubernetes row
    dashboard.with_row(
        Row("Kubernetes")
        .with_panel(
            timeseries.Panel()
            .title("Pod Count")
            .unit(units.Short)
            .min(0)
            .draw_style(GraphDrawStyle.LINE)
            .show_points(VisibilityMode.NEVER)
            .with_target(
                prometheus.Dataquery()
                .expr('count(kube_pod_info)')
                .legend_format("Total Pods")
            )
        )
        .with_panel(
            timeseries.Panel()
            .title("Node Status")
            .unit(units.Short)
            .min(0)
            .draw_style(GraphDrawStyle.LINE)
            .show_points(VisibilityMode.NEVER)
            .with_target(
                prometheus.Dataquery()
                .expr('kube_node_status_condition')
                .legend_format("{{condition}}")
            )
        )
    )
    
    return dashboard.build() 