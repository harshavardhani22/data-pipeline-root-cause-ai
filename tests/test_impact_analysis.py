from src.root_cause.impact_analysis import find_downstream_impact


result = find_downstream_impact("sales_pipeline")


print("\n================================")
print("DOWNSTREAM IMPACT ANALYSIS")
print("================================")


print("\nFailed Pipeline:")
print("sales_pipeline")


print("\nAffected Pipelines:")

for pipeline in result["affected_pipelines"]:
    print("  →", pipeline)


print("\nAffected Dashboards:")

for dashboard in result["affected_dashboards"]:
    print("  →", dashboard)


print("\nPipeline Count:",
      len(result["affected_pipelines"]))

print("Dashboard Count:",
      len(result["affected_dashboards"]))


print("\n================================")