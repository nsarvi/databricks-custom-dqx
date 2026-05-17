from __future__ import annotations
from pyspark.dbutils import DBUtils
from idea4.dqx import yaml_constants as YC
from idea4.dqx.utils.logging_utils import LoggingHandler


logger = LoggingHandler(__name__).get_logger()


def get_runtime_context(dbutils: DBUtils ) -> dict:
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
        YC.WORKSPACE_ID_KEY: _opt(ctx.workspaceId()),
        YC.NOTEBOOK_PATH_KEY: _opt(ctx.notebookPath()),
        YC.NOTEBOOK_ID_KEY: _opt(ctx.notebookId()),
        # Job / run identifiers (usually in tags)
        YC.JOB_ID_KEY: tags.get("jobId") or tags.get("job_id"),
        YC.RUN_ID_KEY: tags.get("runId") or tags.get("run_id"),
        YC.PARENT_RUN_ID_KEY: tags.get("parentRunId") or tags.get("parent_run_id"),
        YC.TASK_RUN_ID_KEY: tags.get("taskRunId") or tags.get("task_run_id"),
        YC.JOB_NAME_KEY: tags.get("jobName") or tags.get("job_name"),
        YC.TASK_KEY_KEY: tags.get("taskKey") or tags.get("task_key"),

        YC.CLUSTER_ID_KEY: tags.get("clusterId") or tags.get("cluster_id"),
        YC.CLUSTER_NAME_KEY: tags.get("clusterName") or tags.get("cluster_name"),
        YC.USER_KEY: tags.get("user") or tags.get("user_name"),
        YC.ORG_ID_KEY: tags.get("orgId") or tags.get("org_id"),
        YC.TAGS_KEY: tags,
    }
