"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
exports.activate = activate;
exports.deactivate = deactivate;
const vscode = __importStar(require("vscode"));
const databricksClient_1 = require("./databricksClient");
const databricksView_1 = require("./databricksView");
class DatabricksGetRunsTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const input = options.input;
        const limit = input.limit ?? 20;
        const message = `Call Databricks jobs/runs/list for job ${input.jobId} (limit ${limit}).`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Fetch runs',
                message: new vscode.MarkdownString(`About to query recent runs for job **${input.jobId}** (limit ${limit}).`),
            },
        };
    }
    async invoke(options, token) {
        const input = options.input;
        const limit = input.limit ?? 20;
        const includeRawJson = input.includeRawJson ?? false;
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            const runs = await client.listRuns(input.jobId, limit, token);
            if (runs.length === 0) {
                const msg = `Databricks Jobs API returned 200 but no runs were found for job ${input.jobId}. Verify the job ID, permissions, or try a different jobsApiVersion (e.g., set databricksTools.jobsApiVersion to 2.0).`;
                return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(msg)]);
            }
            const markdown = this.formatRuns(input.jobId, runs, limit, includeRawJson);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            if (err instanceof databricksClient_1.ConfigCancelled) {
                return new vscode.LanguageModelToolResult([
                    new vscode.LanguageModelTextPart(`Configuration cancelled: ${err.message}`),
                ]);
            }
            if (err instanceof databricksClient_1.ApiError) {
                const hint = err.status === 404 || err.status === 405 || (err.body && /endpoint not found/i.test(err.body))
                    ? 'Jobs API endpoint missing. Try setting databricksTools.jobsApiVersion to 2.0 in settings.'
                    : err.status === 401 || err.status === 403
                        ? 'Permission denied. Verify your PAT or Azure CLI login.'
                        : 'Check Databricks availability and your credentials.';
                const message = `Databricks Jobs API (${err.version ?? 'unknown'}) returned HTTP ${err.status ?? 'unknown'}: ${err.body || err.message}. ${hint}`;
                return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
            }
            const message = err instanceof Error ? err.message : String(err);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(`Databricks error: ${message}`)]);
        }
    }
    formatRuns(jobId, runs, limit, includeRawJson) {
        const successes = runs.filter(r => (r.state?.result_state || '').toUpperCase() === 'SUCCESS').length;
        const failures = runs.filter(r => (r.state?.result_state || '').toUpperCase() === 'FAILED').length;
        const timestamps = runs
            .map(r => r.start_time)
            .filter((v) => typeof v === 'number')
            .sort();
        const earliest = timestamps[0] ? new Date(timestamps[0]).toISOString() : 'n/a';
        const latest = timestamps[timestamps.length - 1] ? new Date(timestamps[timestamps.length - 1]).toISOString() : 'n/a';
        const lines = [];
        lines.push(`# Databricks runs for job ${jobId}`);
        lines.push(`- runs returned: ${runs.length} (limit ${limit})`);
        lines.push(`- time range: ${earliest} -> ${latest}`);
        lines.push(`- successes: ${successes}, failures: ${failures}`);
        lines.push('');
        lines.push('| run_id | start | end | duration_s | life_cycle | result | cluster |');
        lines.push('| --- | --- | --- | --- | --- | --- | --- |');
        for (const run of runs) {
            const start = run.start_time ? new Date(run.start_time).toLocaleString() : 'n/a';
            const end = run.end_time ? new Date(run.end_time).toLocaleString() : 'n/a';
            const durationSeconds = run.start_time && run.end_time ? ((run.end_time - run.start_time) / 1000).toFixed(1) : 'n/a';
            const cluster = run.cluster_spec?.new_cluster
                ? `${run.cluster_spec.new_cluster.num_workers ?? '?'}x ${run.cluster_spec.new_cluster.node_type_id ?? ''}`
                : 'n/a';
            lines.push(`| ${run.run_id} | ${start} | ${end} | ${durationSeconds} | ${run.state?.life_cycle_state ?? 'n/a'} | ${run.state?.result_state ?? 'n/a'} | ${cluster} |`);
        }
        if (includeRawJson) {
            const truncated = runs.slice(0, Math.min(runs.length, limit));
            lines.push('');
            lines.push('```json');
            lines.push(JSON.stringify(truncated, null, 2));
            lines.push('```');
            if (runs.length > truncated.length) {
                lines.push(`(truncated to ${truncated.length} of ${runs.length} runs)`);
            }
        }
        return lines.join('\n');
    }
}
class DatabricksGetJobDefinitionTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const { jobId } = options.input;
        return {
            invocationMessage: `Fetch Databricks job definition for job ${jobId}.`,
            confirmationMessages: {
                title: 'Databricks: Get Job Definition',
                message: new vscode.MarkdownString(`Retrieve job definition (tasks, schedule, clusters) for job **${jobId}**.`),
            },
        };
    }
    async invoke(options, token) {
        const { jobId } = options.input;
        const includeRawJson = options.input.includeRawJson ?? false;
        const config = vscode.workspace.getConfiguration('databricksTools');
        const apiVersion = config.get('jobsApiVersion', 'auto');
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            const { data, usedVersion } = await client.getJobDefinition(jobId, apiVersion, token);
            const markdown = formatJobDefinition(jobId, data, usedVersion, includeRawJson);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'job definition');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksListClustersTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(_options, _token) {
        return {
            invocationMessage: 'List Databricks clusters.',
            confirmationMessages: {
                title: 'Databricks: List Clusters',
                message: new vscode.MarkdownString('List Databricks clusters (IDs, names, states, auto-termination).'),
            },
        };
    }
    async invoke(options, token) {
        const includeRawJson = options.input.includeRawJson ?? false;
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            const clusters = await client.listClusters(token);
            const markdown = formatClusterList(clusters, includeRawJson);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'cluster list');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksStartClusterTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const { clusterId } = options.input;
        return {
            invocationMessage: `Start Databricks cluster ${clusterId}.`,
            confirmationMessages: {
                title: 'Databricks: Start Cluster',
                message: new vscode.MarkdownString(`Start Databricks cluster **${clusterId}**? This may incur cost.`),
            },
        };
    }
    async invoke(options, token) {
        const { clusterId } = options.input;
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            await client.startCluster(clusterId, token);
            const msg = `Cluster start requested for ${clusterId}. Databricks accepted the request. ` +
                'Use listDatabricksClusters to check status until RUNNING.';
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(msg)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'start cluster');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
function activate(context) {
    const tool = new DatabricksGetRunsTool(context);
    context.subscriptions.push(vscode.lm.registerTool('databricks_getRuns', tool));
    context.subscriptions.push(vscode.lm.registerTool('databricks_get_job_definition', new DatabricksGetJobDefinitionTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_list_clusters', new DatabricksListClustersTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_start_cluster', new DatabricksStartClusterTool(context)));
    const viewProvider = new databricksView_1.DatabricksViewProvider(context);
    const view = vscode.window.createTreeView('databricksTools.view', { treeDataProvider: viewProvider, showCollapseAll: false });
    context.subscriptions.push(view);
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.configure', async () => {
        try {
            await (0, databricksClient_1.configureConnection)(context);
            viewProvider.refresh('Configured');
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            void vscode.window.showErrorMessage(`Databricks configuration failed: ${message}`);
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.clearCredentials', async () => {
        try {
            await (0, databricksClient_1.clearStoredCredentials)(context);
            viewProvider.refresh('Credentials cleared');
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            void vscode.window.showErrorMessage(`Failed to clear Databricks credentials: ${message}`);
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.testConnection', async () => {
        const cts = new vscode.CancellationTokenSource();
        try {
            const cfg = await (0, databricksClient_1.ensureDatabricksConfig)(context);
            const client = new databricksClient_1.DatabricksClient(cfg.host, cfg.token);
            await client.testConnection(cts.token);
            const msg = 'Connected to Databricks successfully.';
            viewProvider.refresh('Connected');
            void vscode.window.showInformationMessage(msg);
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            viewProvider.refresh(`Error: ${message}`);
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.testJobsApi', async () => {
        const cts = new vscode.CancellationTokenSource();
        try {
            const cfg = await (0, databricksClient_1.ensureDatabricksConfig)(context);
            const client = new databricksClient_1.DatabricksClient(cfg.host, cfg.token);
            const config = vscode.workspace.getConfiguration('databricksTools');
            const apiVersion = config.get('jobsApiVersion', 'auto');
            const result = await client.testJobsApi(apiVersion, cts.token);
            const msg = `Jobs API reachable (version used: ${result.usedVersion}).`;
            viewProvider.refresh('Jobs API OK');
            void vscode.window.showInformationMessage(msg);
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            viewProvider.refresh(`Jobs API error: ${message}`);
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.listClusters', async () => {
        const cts = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            const clusters = await client.listClusters(cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(formatClusterList(clusters, false));
            output.show(true);
            viewProvider.refresh('Clusters listed');
        }
        catch (err) {
            const message = formatDatabricksError(err, 'cluster list');
            viewProvider.refresh(`Cluster list error`);
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.startCluster', async () => {
        const clusterId = await vscode.window.showInputBox({
            title: 'Databricks Cluster ID',
            prompt: 'Enter the cluster ID to start',
            validateInput: value => (value.trim() ? null : 'Cluster ID is required'),
        });
        if (!clusterId) {
            return;
        }
        const confirmed = await vscode.window.showWarningMessage(`Start Databricks cluster ${clusterId}? This may incur cost.`, { modal: true }, 'Start', 'Cancel');
        if (confirmed !== 'Start') {
            return;
        }
        const cts = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            await client.startCluster(clusterId.trim(), cts.token);
            void vscode.window.showInformationMessage(`Cluster start requested for ${clusterId}. Use "List Clusters" to monitor status.`);
            viewProvider.refresh('Cluster start requested');
        }
        catch (err) {
            const message = formatDatabricksError(err, 'start cluster');
            viewProvider.refresh(`Start cluster error`);
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.getJobDefinition', async () => {
        const jobIdInput = await vscode.window.showInputBox({
            title: 'Databricks Job ID',
            prompt: 'Enter the job ID to fetch its definition',
            validateInput: value => {
                if (!value.trim()) {
                    return 'Job ID is required';
                }
                return /^\d+$/.test(value.trim()) ? null : 'Job ID must be a number';
            },
        });
        if (!jobIdInput) {
            return;
        }
        const jobId = Number(jobIdInput.trim());
        const cts = new vscode.CancellationTokenSource();
        try {
            const config = vscode.workspace.getConfiguration('databricksTools');
            const apiVersion = config.get('jobsApiVersion', 'auto');
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            const { data, usedVersion } = await client.getJobDefinition(jobId, apiVersion, cts.token);
            const markdown = formatJobDefinition(jobId, data, usedVersion, false);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            viewProvider.refresh('Job definition fetched');
        }
        catch (err) {
            const message = formatDatabricksError(err, 'job definition');
            viewProvider.refresh(`Job definition error`);
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.openPanel', async () => {
        await vscode.commands.executeCommand('workbench.view.extension.databricksTools');
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.setAuthMode', async () => {
        const pick = await vscode.window.showQuickPick([
            { label: 'Auto', value: 'auto', description: 'Env PAT > Stored PAT > Azure CLI > prompt' },
            { label: 'PAT only', value: 'pat', description: 'Use PAT from env/secret; prompt if missing.' },
            { label: 'Azure CLI only', value: 'azureCli', description: 'Use az login to get token.' },
        ], { title: 'Select Databricks auth mode', canPickMany: false, ignoreFocusOut: true });
        if (!pick) {
            return;
        }
        await (0, databricksClient_1.setAuthMode)(pick.value);
        viewProvider.refresh(`Auth mode set to ${pick.value}`);
        void vscode.window.showInformationMessage(`Databricks auth mode set to ${pick.value}.`);
    }));
}
function deactivate() { }
function formatDatabricksError(err, context) {
    if (err instanceof databricksClient_1.ConfigCancelled) {
        return `Configuration cancelled: ${err.message}`;
    }
    if (err instanceof databricksClient_1.ApiError) {
        const hint = err.status === 404
            ? 'Check the ID and ensure you have access.'
            : err.status === 401 || err.status === 403
                ? 'Permission denied. Verify your PAT or Azure CLI login.'
                : 'Check Databricks availability and credentials.';
        return `Databricks ${context} failed (API ${err.version ?? 'unknown'}) HTTP ${err.status ?? 'unknown'}: ${err.body ?? err.message}. ${hint}`;
    }
    const message = err instanceof Error ? err.message : String(err);
    return `Databricks ${context} failed: ${message}`;
}
function formatJobDefinition(jobId, data, usedVersion, includeRawJson) {
    const settings = data.settings ?? {};
    const tasks = settings.tasks ?? [];
    const tags = settings.tags ?? {};
    const lines = [];
    lines.push(`# Databricks job ${settings.name ?? 'n/a'} (ID ${jobId})`);
    lines.push(`- API version used: ${usedVersion}`);
    lines.push(`- Creator: ${data.created_by ?? 'n/a'}`);
    lines.push(`- Tags: ${Object.keys(tags).length ? Object.entries(tags).map(([k, v]) => `${k}=${v}`).join(', ') : 'none'}`);
    const schedule = settings.schedule;
    if (schedule?.quartz_cron_expression) {
        lines.push(`- Schedule: cron=${schedule.quartz_cron_expression} tz=${schedule.timezone_id ?? 'UTC'} (${schedule.pause_status ?? 'active'})`);
    }
    else if (settings.trigger?.periodic?.interval) {
        lines.push(`- Trigger: every ${settings.trigger.periodic.interval} ${settings.trigger.periodic.unit ?? 'unit'}`);
    }
    else {
        lines.push('- Schedule: none (manual/triggered)');
    }
    lines.push('');
    lines.push('## Tasks');
    if (tasks.length === 0) {
        lines.push('No tasks defined.');
    }
    else {
        lines.push('| task | type | resource | cluster | depends |');
        lines.push('| --- | --- | --- | --- | --- |');
        for (const task of tasks) {
            const typeAndResource = summarizeTask(task);
            const cluster = summarizeTaskCluster(task);
            const depends = task.depends_on && task.depends_on.length
                ? task.depends_on.map(d => d.task_key).filter(Boolean).join(', ')
                : 'none';
            lines.push(`| ${task.task_key ?? 'n/a'} | ${typeAndResource.type} | ${typeAndResource.resource} | ${cluster} | ${depends} |`);
        }
    }
    if (includeRawJson) {
        lines.push('');
        lines.push('<details>');
        lines.push('<summary>Raw job settings</summary>');
        lines.push('');
        lines.push('```json');
        lines.push(JSON.stringify(data.settings ?? {}, null, 2));
        lines.push('```');
        lines.push('</details>');
    }
    return lines.join('\n');
}
function summarizeTask(task) {
    if (task.notebook_task) {
        return { type: 'notebook', resource: task.notebook_task.notebook_path ?? 'notebook' };
    }
    if (task.pipeline_task) {
        return { type: 'pipeline', resource: task.pipeline_task.pipeline_id ?? 'pipeline' };
    }
    if (task.spark_python_task) {
        return { type: 'python', resource: task.spark_python_task.python_file ?? 'python file' };
    }
    if (task.spark_jar_task) {
        return { type: 'jar', resource: task.spark_jar_task.main_class_name ?? 'jar task' };
    }
    if (task.spark_submit_task) {
        return { type: 'spark-submit', resource: task.spark_submit_task.parameters?.join(' ') || 'spark submit' };
    }
    if (task.sql_task) {
        const q = task.sql_task.query;
        return { type: 'sql', resource: q?.query_name ?? q?.query_id ?? 'SQL task' };
    }
    if (task.python_wheel_task) {
        return { type: 'python wheel', resource: task.python_wheel_task.package_name ?? 'wheel task' };
    }
    return { type: 'task', resource: task.description ?? 'n/a' };
}
function summarizeTaskCluster(task) {
    if (task.existing_cluster_id) {
        return `existing ${task.existing_cluster_id}`;
    }
    if (task.new_cluster) {
        const nc = task.new_cluster;
        const size = nc.autoscale
            ? `auto ${nc.autoscale.min_workers ?? '?'}-${nc.autoscale.max_workers ?? '?'}`
            : `${nc.num_workers ?? '?'} workers`;
        const node = nc.node_type_id ?? nc.driver_node_type_id ?? '?';
        const version = nc.spark_version ?? '?';
        const auto = nc.autotermination_minutes ? `, auto-term ${nc.autotermination_minutes}m` : '';
        return `${size}, node ${node}, spark ${version}${auto}`;
    }
    return 'default cluster';
}
function formatClusterList(clusters, includeRawJson) {
    const lines = [];
    const total = clusters.length;
    const byState = clusters.reduce((acc, c) => {
        const state = (c.state || 'UNKNOWN').toUpperCase();
        acc[state] = (acc[state] || 0) + 1;
        return acc;
    }, {});
    lines.push('# Databricks clusters');
    lines.push(`- total: ${total}`);
    lines.push(`- by state: ${Object.entries(byState).map(([k, v]) => `${k}:${v}`).join(', ') || 'n/a'}`);
    lines.push('');
    lines.push('| cluster_id | name | state | size | spark_version | node_type | auto-termination |');
    lines.push('| --- | --- | --- | --- | --- | --- | --- |');
    const display = clusters.slice(0, 50);
    for (const c of display) {
        const size = c.autoscale
            ? `auto ${c.autoscale.min_workers ?? '?'}-${c.autoscale.max_workers ?? '?'}`
            : `${c.num_workers ?? '?'} workers`;
        const node = c.node_type_id || c.driver_node_type_id || 'n/a';
        const auto = c.autotermination_minutes != null ? `${c.autotermination_minutes}m` : 'n/a';
        lines.push(`| ${c.cluster_id ?? 'n/a'} | ${c.cluster_name ?? 'n/a'} | ${c.state ?? 'n/a'} | ${size} | ${c.spark_version ?? 'n/a'} | ${node} | ${auto} |`);
    }
    if (clusters.length > display.length) {
        lines.push('');
        lines.push(`(truncated to ${display.length} of ${clusters.length} clusters)`);
    }
    if (includeRawJson) {
        lines.push('');
        lines.push('<details>');
        lines.push('<summary>Raw clusters JSON</summary>');
        lines.push('');
        lines.push('```json');
        lines.push(JSON.stringify(display, null, 2));
        lines.push('```');
        lines.push('</details>');
    }
    return lines.join('\n');
}
//# sourceMappingURL=extension.js.map