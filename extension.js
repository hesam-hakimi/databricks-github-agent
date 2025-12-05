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
        return formatRunsMarkdown(jobId, runs, limit, includeRawJson);
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
        const includeTaskSource = options.input.includeTaskSource ?? false;
        const rawMaxSourceChars = options.input.maxSourceCharsPerTask;
        const maxSourceCharsPerTask = typeof rawMaxSourceChars === 'number' && Number.isFinite(rawMaxSourceChars)
            ? Math.max(0, Math.floor(rawMaxSourceChars))
            : 8000;
        const sourceTaskLimit = 20; // normal jobs rarely exceed this; limit to protect output size
        const config = vscode.workspace.getConfiguration('databricksTools');
        const apiVersion = config.get('jobsApiVersion', 'auto');
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            const { data, usedVersion } = await client.getJobDefinition(jobId, apiVersion, token);
            const tasks = data.settings?.tasks ?? [];
            const tasksForSource = includeTaskSource ? tasks.slice(0, sourceTaskLimit) : [];
            const taskSources = includeTaskSource
                ? await fetchTaskSources(client, tasksForSource, maxSourceCharsPerTask, token)
                : undefined;
            const markdown = formatJobDefinition(jobId, data, usedVersion, {
                includeRawJson,
                includeTaskSource,
                taskSources,
                maxSourceCharsPerTask,
                sourceTaskLimit,
                totalTasks: tasks.length,
            });
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'job definition');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksGetNotebookSourceTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const input = options.input;
        const hasPath = !!input.workspacePath;
        const message = hasPath
            ? `Fetch Databricks notebook source for path ${input.workspacePath}.`
            : `Resolve notebook path from job ${input.jobId} task ${input.taskKey} and fetch source.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Get Notebook Source',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        const input = options.input;
        const workspacePath = input.workspacePath?.trim();
        const jobId = input.jobId;
        const taskKey = input.taskKey?.trim();
        const rawMax = input.maxSourceChars;
        const maxSourceChars = typeof rawMax === 'number' && Number.isFinite(rawMax) ? Math.max(0, Math.floor(rawMax)) : 16000;
        const hasPath = !!workspacePath;
        const hasJobTask = jobId != null && taskKey;
        if (!hasPath && !hasJobTask) {
            const msg = 'Provide either workspacePath, or jobId and taskKey, to fetch the notebook source. See input schema for details.';
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(msg)]);
        }
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            const config = vscode.workspace.getConfiguration('databricksTools');
            const apiVersion = config.get('jobsApiVersion', 'auto');
            let resolvedPath = workspacePath;
            if (!resolvedPath && hasJobTask && jobId != null && taskKey) {
                try {
                    resolvedPath = await resolveNotebookPathFromJob(client, jobId, taskKey, apiVersion, token);
                }
                catch (err) {
                    const message = formatNotebookPathResolutionError(jobId, taskKey, err);
                    return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
                }
            }
            if (!resolvedPath) {
                const msg = 'Notebook path could not be resolved.';
                return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(msg)]);
            }
            try {
                const exported = await client.exportWorkspaceSource(resolvedPath);
                const truncated = exported.source.length > maxSourceChars;
                const source = truncated ? exported.source.slice(0, maxSourceChars) : exported.source;
                const markdown = formatNotebookSource(resolvedPath, exported.language, source, truncated, maxSourceChars);
                return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
            }
            catch (err) {
                const message = formatWorkspaceExportError(resolvedPath, err);
                return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
            }
        }
        catch (err) {
            if (err instanceof databricksClient_1.ConfigCancelled) {
                return new vscode.LanguageModelToolResult([
                    new vscode.LanguageModelTextPart(`Configuration cancelled: ${err.message}`),
                ]);
            }
            const message = err instanceof Error ? err.message : String(err);
            const path = workspacePath || (taskKey ? `job ${jobId} task ${taskKey}` : 'unknown path');
            const markdown = `Could not fetch notebook source for path \`${path}\`\n${message}`;
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
    }
}
class DatabricksCreateJobFromCodeTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const { jobName } = options.input;
        const message = `Upload code and submit a Databricks run for job ${jobName}.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Create & Run Job From Code',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, _token) {
        const input = options.input;
        try {
            const markdown = await createJobFromCode(this.context, input);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'create and run job from code');
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
    context.subscriptions.push(vscode.lm.registerTool('databricks_get_notebook_source', new DatabricksGetNotebookSourceTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_create_job_from_code', new DatabricksCreateJobFromCodeTool(context)));
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
            const markdown = formatJobDefinition(jobId, data, usedVersion, { includeRawJson: false, includeTaskSource: false });
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
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.getNotebookSource', async () => {
        const input = await promptNotebookSourceInput();
        if (!input) {
            return;
        }
        const cts = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            const config = vscode.workspace.getConfiguration('databricksTools');
            const apiVersion = config.get('jobsApiVersion', 'auto');
            const rawMax = input.maxSourceChars;
            const maxSourceChars = typeof rawMax === 'number' && Number.isFinite(rawMax) ? Math.max(0, Math.floor(rawMax)) : 16000;
            let resolvedPath = input.workspacePath?.trim();
            if (!resolvedPath && input.jobId != null && input.taskKey) {
                try {
                    resolvedPath = await resolveNotebookPathFromJob(client, input.jobId, input.taskKey, apiVersion, cts.token);
                }
                catch (err) {
                    const message = formatNotebookPathResolutionError(input.jobId, input.taskKey, err);
                    void vscode.window.showErrorMessage(message);
                    return;
                }
            }
            if (!resolvedPath) {
                void vscode.window.showErrorMessage('Notebook path could not be resolved.');
                return;
            }
            try {
                const exported = await client.exportWorkspaceSource(resolvedPath);
                const truncated = exported.source.length > maxSourceChars;
                const source = truncated ? exported.source.slice(0, maxSourceChars) : exported.source;
                const markdown = formatNotebookSource(resolvedPath, exported.language, source, truncated, maxSourceChars);
                const output = (0, databricksClient_1.getOutputChannel)();
                output.appendLine(markdown);
                output.show(true);
                void vscode.window.showInformationMessage(`Fetched notebook source for ${resolvedPath}`);
            }
            catch (err) {
                const message = formatWorkspaceExportError(resolvedPath, err);
                void vscode.window.showErrorMessage(message);
            }
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.createJobFromCode', async () => {
        const cts = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            const input = await promptCreateJobFromCodeInput(client, cts.token);
            if (!input) {
                return;
            }
            const markdown = await createJobFromCode(context, input);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            void vscode.window.showInformationMessage('Job submitted to Databricks.');
        }
        catch (err) {
            const message = formatDatabricksError(err, 'create and run job from code');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.getRuns', async () => {
        const jobIdInput = await vscode.window.showInputBox({
            title: 'Databricks Job ID',
            prompt: 'Enter the job ID to fetch recent runs',
            ignoreFocusOut: true,
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
        const limitInput = await vscode.window.showInputBox({
            title: 'Limit (optional)',
            prompt: 'Enter number of runs to fetch (default 20)',
            ignoreFocusOut: true,
            validateInput: value => {
                if (!value.trim()) {
                    return null;
                }
                return /^\d+$/.test(value.trim()) ? null : 'Enter a positive integer or leave empty';
            },
        });
        const limit = limitInput && limitInput.trim() ? Math.max(1, Math.min(100, Number(limitInput.trim()))) : 20;
        const jobId = Number(jobIdInput.trim());
        const cts = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            const runs = await client.listRuns(jobId, limit, cts.token);
            const markdown = formatRunsMarkdown(jobId, runs, limit, false);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            void vscode.window.showInformationMessage(`Fetched runs for job ${jobId}.`);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'job runs');
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
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.showLogs', async () => {
        const output = (0, databricksClient_1.getOutputChannel)();
        output.show(true);
    }));
}
function deactivate() { }
async function promptNotebookSourceInput() {
    const mode = await vscode.window.showQuickPick([
        { label: 'Workspace path', description: 'Provide a Databricks workspace path', value: 'path' },
        { label: 'Job + task', description: 'Resolve from jobId and taskKey', value: 'jobTask' },
    ], { title: 'Get Notebook Source', placeHolder: 'Choose how to locate the notebook', ignoreFocusOut: true });
    if (!mode) {
        return undefined;
    }
    const maxSourceChars = await vscode.window.showInputBox({
        title: 'Max source characters (optional)',
        prompt: 'Leave empty for default 16000 characters. Large notebooks will be truncated to this length.',
        ignoreFocusOut: true,
        validateInput: value => {
            if (!value.trim()) {
                return null;
            }
            return /^\d+$/.test(value.trim()) ? null : 'Enter a positive integer or leave empty';
        },
    });
    const parsedMax = parseOptionalPositiveInt(maxSourceChars);
    if (mode.value === 'path') {
        const workspacePath = await vscode.window.showInputBox({
            title: 'Workspace notebook path',
            prompt: 'Enter the workspace path, e.g. /Workspace/Users/... or /Repos/...',
            ignoreFocusOut: true,
            validateInput: value => (value.trim() ? null : 'Workspace path is required'),
        });
        if (!workspacePath) {
            return undefined;
        }
        return { workspacePath: workspacePath.trim(), maxSourceChars: parsedMax };
    }
    const jobId = await vscode.window.showInputBox({
        title: 'Job ID',
        prompt: 'Enter the Databricks job ID',
        ignoreFocusOut: true,
        validateInput: value => {
            if (!value.trim()) {
                return 'Job ID is required';
            }
            return /^\d+$/.test(value.trim()) ? null : 'Job ID must be a number';
        },
    });
    if (!jobId) {
        return undefined;
    }
    const taskKey = await vscode.window.showInputBox({
        title: 'Task key',
        prompt: 'Enter the task key within the job',
        ignoreFocusOut: true,
        validateInput: value => (value.trim() ? null : 'Task key is required'),
    });
    if (!taskKey) {
        return undefined;
    }
    return { jobId: Number(jobId.trim()), taskKey: taskKey.trim(), maxSourceChars: parsedMax };
}
async function promptCreateJobFromCodeInput(client, token) {
    const jobName = await vscode.window.showInputBox({
        title: 'Job name',
        prompt: 'Enter a name for the new Databricks job/run',
        ignoreFocusOut: true,
        validateInput: value => (value.trim() ? null : 'Job name is required'),
    });
    if (!jobName) {
        return undefined;
    }
    const languagePick = await vscode.window.showQuickPick([
        { label: 'PYTHON', description: 'Default', value: 'PYTHON' },
        { label: 'SQL', value: 'SQL' },
        { label: 'SCALA', value: 'SCALA' },
        { label: 'R', value: 'R' },
    ], { title: 'Language', placeHolder: 'Select code language', ignoreFocusOut: true });
    const language = languagePick?.value ?? 'PYTHON';
    const workspacePath = await vscode.window.showInputBox({
        title: 'Workspace path (optional)',
        prompt: 'Provide a workspace path or leave empty to use the default folder',
        ignoreFocusOut: true,
    });
    const sourceCode = await vscode.window.showInputBox({
        title: 'Source code',
        prompt: 'Paste the Python/SQL code to upload and run',
        ignoreFocusOut: true,
        validateInput: value => (value.trim() ? null : 'Source code is required'),
        value: '',
    });
    if (!sourceCode) {
        return undefined;
    }
    const clusterModePick = await vscode.window.showQuickPick([
        { label: 'Use existing cluster', value: 'existingCluster' },
        { label: 'Create new job cluster', value: 'newJobCluster' },
    ], { title: 'Cluster selection', placeHolder: 'Choose how to run the job', ignoreFocusOut: true });
    if (!clusterModePick) {
        return undefined;
    }
    if (clusterModePick.value === 'existingCluster') {
        let clusters = [];
        try {
            clusters = await client.listClusters(token);
        }
        catch {
            // ignore list errors here; user can still enter manually
        }
        const items = clusters.slice(0, 50).map(c => ({
            label: c.cluster_name ?? c.cluster_id ?? 'unknown cluster',
            description: c.cluster_id ?? '',
            detail: `state: ${c.state ?? 'n/a'}`,
            clusterId: c.cluster_id,
        }));
        items.push({ label: 'Enter cluster ID manually…', description: 'Type an existing cluster ID', detail: '', clusterId: undefined });
        const pick = await vscode.window.showQuickPick(items, {
            title: 'Select existing cluster',
            placeHolder: 'Choose a cluster to run the job',
            ignoreFocusOut: true,
        });
        if (!pick) {
            return undefined;
        }
        let clusterId = pick.clusterId;
        if (!clusterId) {
            const manual = await vscode.window.showInputBox({
                title: 'Existing cluster ID',
                prompt: 'Enter the cluster ID to use',
                ignoreFocusOut: true,
                validateInput: value => (value.trim() ? null : 'Cluster ID is required'),
            });
            if (!manual) {
                return undefined;
            }
            clusterId = manual.trim();
        }
        return {
            jobName: jobName.trim(),
            sourceCode,
            language,
            workspacePath: workspacePath?.trim() || undefined,
            clusterMode: 'existingCluster',
            existingClusterId: clusterId,
        };
    }
    const sparkVersion = await vscode.window.showInputBox({
        title: 'Runtime version',
        prompt: 'Enter Databricks runtime version, e.g. 14.3.x-scala2.12',
        ignoreFocusOut: true,
        validateInput: value => (value.trim() ? null : 'Runtime version is required'),
    });
    if (!sparkVersion) {
        return undefined;
    }
    const nodeTypeId = await vscode.window.showInputBox({
        title: 'Node type',
        prompt: 'Enter node_type_id for workers/driver',
        ignoreFocusOut: true,
        validateInput: value => (value.trim() ? null : 'Node type is required'),
    });
    if (!nodeTypeId) {
        return undefined;
    }
    const numWorkersInput = await vscode.window.showInputBox({
        title: 'Number of workers (optional)',
        prompt: 'Enter worker count (default 1)',
        ignoreFocusOut: true,
        validateInput: value => {
            if (!value.trim()) {
                return null;
            }
            return /^\d+$/.test(value.trim()) ? null : 'Enter a non-negative integer or leave empty';
        },
    });
    const autoTermInput = await vscode.window.showInputBox({
        title: 'Auto-termination minutes (optional)',
        prompt: 'Enter auto-termination in minutes (default 60)',
        ignoreFocusOut: true,
        validateInput: value => {
            if (!value.trim()) {
                return null;
            }
            return /^\d+$/.test(value.trim()) ? null : 'Enter a non-negative integer or leave empty';
        },
    });
    return {
        jobName: jobName.trim(),
        sourceCode,
        language,
        workspacePath: workspacePath?.trim() || undefined,
        clusterMode: 'newJobCluster',
        newClusterConfig: {
            sparkVersion: sparkVersion.trim(),
            nodeTypeId: nodeTypeId.trim(),
            numWorkers: parseOptionalPositiveInt(numWorkersInput),
            autoTerminationMinutes: parseOptionalPositiveInt(autoTermInput),
        },
    };
}
function parseOptionalPositiveInt(value) {
    if (!value) {
        return undefined;
    }
    const trimmed = value.trim();
    if (!trimmed) {
        return undefined;
    }
    const parsed = Number(trimmed);
    if (!Number.isFinite(parsed) || parsed < 0) {
        return undefined;
    }
    return Math.floor(parsed);
}
async function createJobFromCode(context, input) {
    const jobName = input.jobName?.trim();
    const sourceCode = input.sourceCode;
    if (!jobName || !sourceCode) {
        throw new Error('jobName and sourceCode are required to create and run a job.');
    }
    const language = (input.language ?? 'PYTHON').toUpperCase();
    const clusterMode = input.clusterMode === 'newJobCluster' ? 'newJobCluster' : 'existingCluster';
    if (clusterMode === 'existingCluster' && !input.existingClusterId) {
        throw new Error('existingClusterId is required when clusterMode is existingCluster.');
    }
    let newClusterConfig;
    if (clusterMode === 'newJobCluster') {
        const cfg = input.newClusterConfig;
        if (!cfg || !cfg.sparkVersion || !cfg.nodeTypeId) {
            throw new Error('newClusterConfig.sparkVersion and nodeTypeId are required when clusterMode is newJobCluster.');
        }
        newClusterConfig = {
            sparkVersion: cfg.sparkVersion,
            nodeTypeId: cfg.nodeTypeId,
            numWorkers: normalizeOptionalInt(cfg.numWorkers, 1),
            autoTerminationMinutes: normalizeOptionalInt(cfg.autoTerminationMinutes, 60),
        };
    }
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    const defaultFolder = getDefaultUploadFolder();
    const targetPath = buildWorkspaceUploadPath(jobName, language, input.workspacePath, defaultFolder);
    await client.importWorkspaceSource(targetPath, language, sourceCode);
    const result = await client.submitRunFromNotebook(jobName, targetPath, clusterMode, {
        existingClusterId: input.existingClusterId,
        newClusterConfig,
    });
    return formatCreateJobResult(jobName, targetPath, clusterMode, input.existingClusterId, newClusterConfig, result);
}
async function fetchTaskSources(client, tasks, maxSourceCharsPerTask, token) {
    const results = [];
    for (const task of tasks) {
        if (token.isCancellationRequested) {
            break;
        }
        const taskKey = task.task_key ?? 'n/a';
        const target = resolveTaskSourceTarget(task);
        if (target.skipReason) {
            results.push({
                taskKey,
                taskType: target.type,
                path: target.path,
                error: target.skipReason,
            });
            continue;
        }
        if (!target.path) {
            results.push({
                taskKey,
                taskType: target.type,
                path: target.path,
                error: 'No workspace path available for this task.',
            });
            continue;
        }
        try {
            const exported = await client.exportWorkspaceSource(target.path);
            const truncated = exported.source.length > maxSourceCharsPerTask;
            results.push({
                taskKey,
                taskType: target.type,
                path: target.path,
                language: exported.language ?? undefined,
                source: truncated ? exported.source.slice(0, maxSourceCharsPerTask) : exported.source,
                truncated,
            });
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            results.push({
                taskKey,
                taskType: target.type,
                path: target.path,
                error: message,
            });
        }
    }
    return results;
}
// Best-effort source resolution: notebooks are guaranteed; other task types need workspace paths and may be skipped.
function resolveTaskSourceTarget(task) {
    if (task.notebook_task?.notebook_path) {
        return { type: 'notebook', path: task.notebook_task.notebook_path };
    }
    if (task.spark_python_task?.python_file) {
        const path = task.spark_python_task.python_file;
        if (!isWorkspacePath(path)) {
            return {
                type: 'python',
                path,
                skipReason: 'Source fetch skipped: path is not a workspace path (requires /Workspace, /Repos, /Users, or /Shared).',
            };
        }
        return { type: 'python', path };
    }
    if (task.spark_jar_task) {
        return { type: 'jar', skipReason: 'Jar tasks reference artifacts outside workspace export (DBFS/external).' };
    }
    if (task.spark_submit_task) {
        return { type: 'spark-submit', skipReason: 'Spark submit tasks do not expose a workspace source path for export.' };
    }
    if (task.sql_task) {
        return { type: 'sql', skipReason: 'SQL tasks reference queries/warehouses; workspace export is not supported.' };
    }
    if (task.python_wheel_task) {
        return { type: 'python wheel', skipReason: 'Wheel tasks use package artifacts; workspace export not supported.' };
    }
    if (task.pipeline_task) {
        return { type: 'pipeline', skipReason: 'Pipeline tasks do not expose a workspace path for source export.' };
    }
    return { type: 'task', skipReason: 'Task type not supported for workspace export.' };
}
async function resolveNotebookPathFromJob(client, jobId, taskKey, apiVersion, token) {
    const { data } = await client.getJobDefinition(jobId, apiVersion, token);
    const tasks = data.settings?.tasks ?? [];
    const target = tasks.find(t => t.task_key === taskKey);
    if (!target) {
        throw new Error(`Task "${taskKey}" was not found in job ${jobId}.`);
    }
    if (!target.notebook_task || !target.notebook_task.notebook_path) {
        throw new Error(`Task "${taskKey}" in job ${jobId} is not a notebook task or has no notebook_path. I cannot fetch notebook source for it.`);
    }
    return target.notebook_task.notebook_path;
}
function formatNotebookSource(path, language, source, truncated, maxSourceChars) {
    const fence = languageToFence(language);
    const langLabel = language ?? 'UNKNOWN';
    const lines = [];
    lines.push('## Notebook Source');
    lines.push('');
    lines.push(`Path: \`${path}\``);
    lines.push(`Language: \`${langLabel}\``);
    lines.push('');
    lines.push(`\`\`\`${fence}`);
    lines.push(source);
    lines.push('```');
    if (truncated) {
        lines.push('');
        lines.push(`Source truncated to ${maxSourceChars} characters. Ask the user to increase maxSourceChars if you need more of the notebook.`);
    }
    return lines.join('\n');
}
function formatCreateJobResult(jobName, path, clusterMode, existingClusterId, newClusterConfig, result) {
    const lines = [];
    lines.push('## Job submitted to Databricks');
    lines.push('');
    lines.push(`Run ID: \`${result.runId}\``);
    if (result.jobId !== undefined) {
        lines.push(`Job ID: \`${result.jobId}\``);
    }
    lines.push(`Run name: \`${jobName}\``);
    lines.push(`Notebook path: \`${path}\``);
    if (clusterMode === 'existingCluster') {
        lines.push(`Cluster mode: existingCluster (id: ${existingClusterId ?? 'n/a'})`);
    }
    else if (newClusterConfig) {
        lines.push(`Cluster mode: newJobCluster (runtime ${newClusterConfig.sparkVersion}, node ${newClusterConfig.nodeTypeId}, workers ${newClusterConfig.numWorkers ?? 1}, auto-term ${newClusterConfig.autoTerminationMinutes ?? 60}m)`);
    }
    lines.push('');
    lines.push('You can now:');
    lines.push('- Ask me to monitor this run with `getDatabricksRuns` by providing the job/run details.');
    lines.push('- Open the run in the Databricks UI if you prefer (use the run ID above).');
    return lines.join('\n');
}
function formatWorkspaceExportError(path, err) {
    if (err instanceof databricksClient_1.ApiError) {
        const status = err.status ?? 'unknown';
        const code = err.errorCode ? ` (${err.errorCode})` : '';
        const detail = err.body ?? err.message;
        return `Could not fetch notebook source for path \`${path}\`\nDatabricks Workspace API returned HTTP ${status}${code}: ${detail}`;
    }
    const message = err instanceof Error ? err.message : String(err);
    return `Could not fetch notebook source for path \`${path}\`\n${message}`;
}
function formatNotebookPathResolutionError(jobId, taskKey, err) {
    if (err instanceof databricksClient_1.ApiError) {
        const status = err.status ?? 'unknown';
        const code = err.errorCode ? ` (${err.errorCode})` : '';
        const detail = err.body ?? err.message;
        return `Could not resolve notebook path for job ${jobId} task ${taskKey}.\nDatabricks Jobs API returned HTTP ${status}${code}: ${detail}`;
    }
    const message = err instanceof Error ? err.message : String(err);
    return message;
}
function formatRunsMarkdown(jobId, runs, limit, includeRawJson) {
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
function getDefaultUploadFolder() {
    const cfg = vscode.workspace.getConfiguration('databricksTools');
    const folder = (cfg.get('defaultUploadFolder') || '/Workspace/Shared/CopilotJobs').trim();
    return folder || '/Workspace/Shared/CopilotJobs';
}
function buildWorkspaceUploadPath(jobName, language, explicitPath, defaultFolder) {
    if (explicitPath && explicitPath.trim()) {
        return explicitPath.trim();
    }
    const ext = languageToExtension(language);
    const safeName = sanitizeName(jobName || 'job');
    const timestamp = formatTimestamp(new Date());
    const baseFolder = defaultFolder.replace(/\/$/, '');
    return `${baseFolder}/${safeName}-${timestamp}.${ext}`;
}
function sanitizeName(name) {
    return name.trim().toLowerCase().replace(/[^a-z0-9-_]+/g, '-').replace(/^-+|-+$/g, '') || 'job';
}
function formatTimestamp(d) {
    const pad = (n) => n.toString().padStart(2, '0');
    const yyyy = d.getFullYear();
    const mm = pad(d.getMonth() + 1);
    const dd = pad(d.getDate());
    const hh = pad(d.getHours());
    const mi = pad(d.getMinutes());
    const ss = pad(d.getSeconds());
    return `${yyyy}${mm}${dd}-${hh}${mi}${ss}`;
}
function languageToExtension(language) {
    const normalized = (language || '').toUpperCase();
    switch (normalized) {
        case 'SQL':
            return 'sql';
        case 'SCALA':
            return 'scala';
        case 'R':
            return 'r';
        default:
            return 'py';
    }
}
function normalizeOptionalInt(value, fallback) {
    if (value === undefined || value === null) {
        return fallback;
    }
    if (!Number.isFinite(value)) {
        return fallback;
    }
    const n = Math.floor(value);
    return n >= 0 ? n : fallback;
}
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
        const code = err.errorCode ? ` (${err.errorCode})` : '';
        return `Databricks ${context} failed (API ${err.version ?? 'unknown'}) HTTP ${err.status ?? 'unknown'}${code}: ${err.body ?? err.message}. ${hint}`;
    }
    const message = err instanceof Error ? err.message : String(err);
    return `Databricks ${context} failed: ${message}`;
}
function isWorkspacePath(path) {
    const lowered = path.toLowerCase();
    if (lowered.startsWith('dbfs:') || lowered.startsWith('/dbfs/')) {
        return false;
    }
    if (/^[a-z]+:\/\//i.test(path)) {
        return false;
    }
    if (lowered.startsWith('file:')) {
        return false;
    }
    return path.startsWith('/');
}
function formatJobDefinition(jobId, data, usedVersion, options) {
    const includeRawJson = options.includeRawJson ?? false;
    const includeTaskSource = options.includeTaskSource ?? false;
    const taskSources = options.taskSources ?? [];
    const maxSourceCharsPerTask = Math.max(0, options.maxSourceCharsPerTask ?? 8000);
    const settings = data.settings ?? {};
    const tasks = settings.tasks ?? [];
    const totalTasks = options.totalTasks ?? tasks.length;
    const sourceTaskLimit = Math.max(0, options.sourceTaskLimit ?? totalTasks);
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
    if (includeTaskSource) {
        lines.push('');
        lines.push('### Task Source Code');
        if (tasks.length === 0) {
            lines.push('No tasks available for source export.');
        }
        else {
            if (totalTasks > sourceTaskLimit) {
                lines.push(`Source export attempted for first ${sourceTaskLimit} of ${totalTasks} tasks (limit reached).`);
            }
            if (taskSources.length === 0) {
                lines.push('_No task source code fetched._');
            }
            for (const ts of taskSources) {
                lines.push('');
                lines.push(`#### Task: ${ts.taskKey ?? 'n/a'} (type: ${ts.taskType})`);
                const pathLabel = ts.taskType === 'notebook' ? 'Notebook path' : 'Path';
                lines.push(`${pathLabel}: \`${ts.path ?? 'n/a'}\``);
                if (ts.error) {
                    lines.push(`_Could not fetch source for this task: ${ts.error}_`);
                    continue;
                }
                const fence = languageToFence(ts.language);
                lines.push(`\`\`\`${fence}`);
                lines.push(ts.source ?? '');
                lines.push('```');
                if (ts.truncated) {
                    lines.push(`_Source truncated to ${maxSourceCharsPerTask} characters. Ask the user to increase \`maxSourceCharsPerTask\` if more code is needed._`);
                }
            }
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
function languageToFence(language) {
    if (!language) {
        return 'text';
    }
    const normalized = language.toLowerCase();
    switch (normalized) {
        case 'python':
            return 'python';
        case 'sql':
            return 'sql';
        case 'scala':
            return 'scala';
        case 'r':
            return 'r';
        default:
            return 'text';
    }
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