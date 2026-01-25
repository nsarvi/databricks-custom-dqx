from __future__ import annotations

def get_runtime_context(dbutils) -> dict:
    """
    Best-effort Databricks runtime/job context.
    Works in Jobs and interactive notebooks.
    """
    if dbutils is None:
        return {}

    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    except Exception:
        return {}

    def _opt(x):
        try:
            return x.get() if x.isDefined() else None
        except Exception:
            return None

    tags = {}
    try:
        t = ctx.tags().get()
        for k in t.keySet():
            tags[str(k)] = str(t.get(k).get())
    except Exception:
        pass

    return {
        "workspace_id": _opt(ctx.workspaceId()),
        "notebook_path": _opt(ctx.notebookPath()),
        "notebook_id": _opt(ctx.notebookId()),

        # Job / run identifiers (usually in tags)
        "job_id": tags.get("jobId") or tags.get("job_id"),
        "run_id": tags.get("runId") or tags.get("run_id"),
        "parent_run_id": tags.get("parentRunId") or tags.get("parent_run_id"),
        "task_run_id": tags.get("taskRunId") or tags.get("task_run_id"),
        "job_name": tags.get("jobName") or tags.get("job_name"),
        "task_key": tags.get("taskKey") or tags.get("task_key"),

        "cluster_id": tags.get("clusterId") or tags.get("cluster_id"),
        "cluster_name": tags.get("clusterName") or tags.get("cluster_name"),
        "user": tags.get("user") or tags.get("user_name"),
        "org_id": tags.get("orgId") or tags.get("org_id"),

        "tags": tags,
    }
