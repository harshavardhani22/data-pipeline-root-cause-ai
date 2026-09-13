# Dependency graph for the retail data platform

DEPENDENCIES = {
    "sales_pipeline": [
        "customer_pipeline",
        "analytics_pipeline"
    ],

    "customer_pipeline": [
        "customer_dashboard"
    ],

    "analytics_pipeline": [
        "sales_dashboard",
        "revenue_dashboard"
    ],

    "inventory_pipeline": [
        "inventory_dashboard"
    ]
}


def find_downstream_impact(failed_pipeline):

    affected_pipelines = set()
    affected_dashboards = set()

    queue = [failed_pipeline]

    while queue:

        current = queue.pop(0)

        for dependency in DEPENDENCIES.get(current, []):

            if dependency in affected_pipelines:
                continue

            if "dashboard" in dependency:
                affected_dashboards.add(dependency)
            else:
                affected_pipelines.add(dependency)

            queue.append(dependency)

    return {
        "affected_pipelines": sorted(affected_pipelines),
        "affected_dashboards": sorted(affected_dashboards)
    }