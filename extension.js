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
exports.createAndRunJobFromCode = createAndRunJobFromCode;
exports.runCodeAndGetResult = runCodeAndGetResult;
exports.showClusterDefinition = showClusterDefinition;
exports.getClusterDefinitionMarkdown = getClusterDefinitionMarkdown;
const vscode = __importStar(require("vscode"));
const path = __importStar(require("path"));
const fs_1 = require("fs");
const databricksClient_1 = require("./databricksClient");
const databricksView_1 = require("./databricksView");
const workspacePath_1 = require("./workspacePath");
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
class DatabricksGetRunDetailsTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const { runId } = options.input;
        const message = `Fetch Databricks run details for run ${runId}.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Get Run Details',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        const { runId } = options.input;
        const includeRawJson = options.input.includeRawJson ?? false;
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            const run = await client.getRunDetails(runId);
            const markdown = formatRunDetails(run, includeRawJson);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'run details');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksAnalyzeRunPerformanceTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const { runId } = options.input;
        const message = `Analyze performance for Databricks run ${runId}.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Analyze Run Performance',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        const { runId } = options.input;
        const includeRawJson = options.input.includeRawJson ?? false;
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            const markdown = await analyzeRunPerformance(client, runId, includeRawJson, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'run performance analysis');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksProfileTableLayoutTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const target = options.input.tableName?.trim() || options.input.tablePath?.trim() || 'table/path';
        const message = `Profile Databricks table layout for ${target}.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Profile Table Layout',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        try {
            const markdown = await profileTableLayout(this.context, options.input, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'table layout profiling');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksProfileTableStatsTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const target = options.input.tableName || options.input.path || 'table/path';
        const message = `Profile table stats for ${target}`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Profile Table Stats',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        try {
            const markdown = await profileTableStats(this.context, options.input, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'profile table stats');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksAnalyzeRunStagesTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const { runId } = options.input;
        const message = `Analyze Spark stages for Databricks run ${runId}.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Analyze Run Stages',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        try {
            const markdown = await analyzeRunStages(this.context, options.input, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'run stages analysis');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksExplainSqlTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const sql = options.input.sql?.trim() ?? '';
        const preview = sql.length > 120 ? `${sql.slice(0, 120)}…` : sql || 'SQL statement';
        const message = `Explain SQL (EXPLAIN FORMATTED): ${preview}`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Explain SQL',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        try {
            const markdown = await explainSql(this.context, options.input, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'explain SQL');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksSummarizeJobHistoryTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const { jobId } = options.input;
        const limit = options.input.limit ?? 20;
        const message = `Summarize recent ${limit} runs for Databricks job ${jobId}.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Summarize Job History',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        try {
            const markdown = await summarizeJobHistory(this.context, options.input, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'job history summary');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksResolveArtifactMappingTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const target = options.input.jobId ? `job ${options.input.jobId}` : options.input.workspacePath || 'artifact';
        const message = `Resolve Databricks artifact mapping for ${target}.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Resolve Artifact Mapping',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, _token) {
        try {
            const markdown = await resolveArtifactMapping(this.context, options.input);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'resolve artifact mapping');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksExecuteSqlOnClusterTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const sql = options.input.sql?.trim() ?? '';
        const preview = sql.length > 120 ? `${sql.slice(0, 120)}…` : sql || 'SQL statement';
        const target = options.input.clusterId ? `cluster ${options.input.clusterId}` : 'default all-purpose cluster';
        const message = `Execute SQL on ${target}: ${preview}`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Execute SQL on All-purpose Cluster',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        try {
            const markdown = await executeSqlOnCluster(this.context, options.input, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'execute SQL on cluster');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksExecutePythonOnClusterTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const code = options.input.code?.trim() ?? '';
        const preview = code.length > 120 ? `${code.slice(0, 120)}…` : code || 'Python code';
        const target = options.input.clusterId ? `cluster ${options.input.clusterId}` : 'default all-purpose cluster';
        const message = `Execute Python on ${target}: ${preview}`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Execute Python on All-purpose Cluster',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        try {
            const markdown = await executePythonOnCluster(this.context, options.input, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'execute Python on cluster');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksGetClusterDefinitionTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const target = options.input.clusterId ? `cluster ${options.input.clusterId}` : 'default cluster';
        const message = `Fetch Databricks cluster definition for ${target}.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Get Cluster Definition',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        try {
            const markdown = await getClusterDefinitionMarkdown(this.context, options.input, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'get cluster definition');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
    }
}
class DatabricksRunCodeAndGetResultTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const jobName = options.input.jobName?.trim() || 'Ad hoc code run';
        const clusterMode = options.input.clusterMode ?? 'defaultCluster';
        const clusterTarget = clusterMode === 'newJobCluster'
            ? 'a new job cluster'
            : clusterMode === 'existingCluster'
                ? 'the specified existing cluster'
                : 'the default cluster (auto-start if needed)';
        const message = `Upload code, run it as a Databricks notebook, and return JSON output for ${jobName} on ${clusterTarget}.`;
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Run Code and Get Result',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        try {
            const markdown = await runCodeAndGetResult(this.context, options.input, token);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'run code and get result');
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(message)]);
        }
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
class DatabricksCreateAndRunJobFromCodeTool {
    context;
    constructor(context) {
        this.context = context;
    }
    async prepareInvocation(options, _token) {
        const { jobName } = options.input;
        const clusterMode = options.input.clusterMode ?? 'defaultCluster';
        const clusterTarget = clusterMode === 'newJobCluster'
            ? 'a new job cluster'
            : clusterMode === 'existingCluster'
                ? 'the specified existing cluster'
                : 'the default cluster (auto-start if needed)';
        const message = `Upload code, create a Databricks job, and start a run for ${jobName} on ${clusterTarget}.`;
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
            const markdown = await createAndRunJobFromCode(this.context, input, _token);
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
        const includeInteractiveClusters = options.input.includeInteractiveClusters ?? true;
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            const clusters = await client.listClusters(token, { includeInteractiveClusters });
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
        const message = clusterId
            ? `Start Databricks cluster ${clusterId}.`
            : 'Start the default Databricks cluster (or pick one if no default is set).';
        return {
            invocationMessage: message,
            confirmationMessages: {
                title: 'Databricks: Start Cluster',
                message: new vscode.MarkdownString(message),
            },
        };
    }
    async invoke(options, token) {
        const inputClusterId = options.input.clusterId?.trim();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(this.context);
            const resolved = await resolveClusterForStart(this.context, client, token, inputClusterId, {
                promptToSetDefault: true,
            });
            if (!resolved?.id) {
                return new vscode.LanguageModelToolResult([
                    new vscode.LanguageModelTextPart('Cluster start cancelled; no cluster was selected.'),
                ]);
            }
            await client.startCluster(resolved.id, token);
            const clusterLabel = resolved.name ? `${resolved.name} (${resolved.id})` : resolved.id;
            const msg = `Cluster start requested for ${clusterLabel}. Databricks accepted the request. ` +
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
    context.subscriptions.push(vscode.lm.registerTool('databricks_run_code_and_get_result', new DatabricksRunCodeAndGetResultTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_get_job_definition', new DatabricksGetJobDefinitionTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_get_notebook_source', new DatabricksGetNotebookSourceTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_create_and_run_job_from_code', new DatabricksCreateAndRunJobFromCodeTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_get_run_details', new DatabricksGetRunDetailsTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_analyze_run_performance', new DatabricksAnalyzeRunPerformanceTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_profile_table_layout', new DatabricksProfileTableLayoutTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_profile_table_stats', new DatabricksProfileTableStatsTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_analyze_run_stages', new DatabricksAnalyzeRunStagesTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_explain_sql', new DatabricksExplainSqlTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_summarize_job_history', new DatabricksSummarizeJobHistoryTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_resolve_artifact_mapping', new DatabricksResolveArtifactMappingTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_list_clusters', new DatabricksListClustersTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_start_cluster', new DatabricksStartClusterTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_execute_sql_on_cluster', new DatabricksExecuteSqlOnClusterTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_execute_python_on_cluster', new DatabricksExecutePythonOnClusterTool(context)));
    context.subscriptions.push(vscode.lm.registerTool('databricks_get_cluster_definition', new DatabricksGetClusterDefinitionTool(context)));
    const viewProvider = new databricksView_1.DatabricksViewProvider(context);
    const view = vscode.window.createTreeView('databricksTools.view', { treeDataProvider: viewProvider, showCollapseAll: false });
    context.subscriptions.push(view);
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.configure', async () => {
        try {
            await (0, databricksClient_1.configureConnection)(context);
            const pick = await vscode.window.showQuickPick([
                { label: 'Select default cluster now', value: 'choose' },
                { label: 'Skip', value: 'skip' },
            ], { title: 'Select a default cluster?', ignoreFocusOut: true });
            if (pick?.value === 'choose') {
                const cts = new vscode.CancellationTokenSource();
                try {
                    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
                    await chooseDefaultCluster(context, client, cts.token, {
                        allowClear: true,
                        title: 'Select Default Cluster (optional)',
                    });
                }
                finally {
                    cts.dispose();
                }
            }
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
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.setDefaultCluster', async () => {
        const cts = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            await chooseDefaultCluster(context, client, cts.token, { allowClear: true, title: 'Select Default Cluster' });
            viewProvider.refresh('Default cluster updated');
        }
        catch (err) {
            const message = formatDatabricksError(err, 'set default cluster');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.setDefaultWorkspaceFolder', async () => {
        const current = (0, databricksClient_1.getDefaultWorkspaceFolder)();
        const input = await vscode.window.showInputBox({
            title: 'Default Databricks workspace folder',
            prompt: 'Enter default Databricks workspace folder (e.g. /Workspace/Users/<user>/Jobs)',
            value: current || '/Workspace/Shared/CopilotJobs',
            ignoreFocusOut: true,
            validateInput: value => {
                const trimmed = value.trim();
                if (!trimmed) {
                    return 'Workspace folder is required';
                }
                if (!trimmed.startsWith('/Workspace/')) {
                    return 'Path must start with /Workspace/.';
                }
                if (/\.(py|sql|scala|r)$/i.test(trimmed)) {
                    return 'Provide a folder path, not a file path.';
                }
                return null;
            },
        });
        if (!input) {
            return;
        }
        const trimmed = input.trim();
        await (0, databricksClient_1.setDefaultWorkspaceFolder)(trimmed);
        viewProvider.refresh('Workspace folder updated');
        void vscode.window.showInformationMessage(`Default workspace folder set to ${trimmed}`);
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.toggleAppendProjectSubfolder', async () => {
        const current = (0, databricksClient_1.getAppendProjectSubfolder)();
        await (0, databricksClient_1.setAppendProjectSubfolder)(!current);
        const state = !current ? 'enabled' : 'disabled';
        viewProvider.refresh(`Append project subfolder ${state}`);
        void vscode.window.showInformationMessage(`Append project subfolder ${state}.`);
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
        const cts = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            const currentDefault = await (0, databricksClient_1.getDefaultCluster)(context);
            let resolved;
            if (currentDefault.id) {
                const pick = await vscode.window.showQuickPick([
                    {
                        label: `Start default cluster ${currentDefault.name ?? currentDefault.id}`,
                        description: currentDefault.id,
                        value: 'default',
                    },
                    { label: 'Start a different cluster…', value: 'other' },
                    { label: 'Cancel', value: 'cancel' },
                ], { title: 'Start Cluster', ignoreFocusOut: true });
                if (!pick || pick.value === 'cancel') {
                    return;
                }
                if (pick.value === 'default') {
                    resolved = { id: currentDefault.id, name: currentDefault.name };
                }
                else {
                    resolved = await pickClusterFromList(client, cts.token, { title: 'Select a cluster to start' });
                }
            }
            else {
                resolved = await resolveClusterForStart(context, client, cts.token, undefined, { promptToSetDefault: true });
            }
            if (!resolved?.id) {
                return;
            }
            const confirmLabel = resolved.name ? `${resolved.name} (${resolved.id})` : resolved.id;
            const confirmed = await vscode.window.showWarningMessage(`Start Databricks cluster ${confirmLabel}? This may incur cost.`, { modal: true }, 'Start', 'Cancel');
            if (confirmed !== 'Start') {
                return;
            }
            await client.startCluster(resolved.id.trim(), cts.token);
            void vscode.window.showInformationMessage(`Cluster start requested for ${confirmLabel}. Use "List Clusters" to monitor status.`);
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
            const input = await promptCreateJobFromCodeInput(context, client, cts.token);
            if (!input) {
                return;
            }
            const markdown = await createAndRunJobFromCode(context, input, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            void vscode.window.showInformationMessage('Job created and run started on Databricks.');
        }
        catch (err) {
            const message = formatDatabricksError(err, 'create and run job from code');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.getRunDetails', async () => {
        const runIdInput = await vscode.window.showInputBox({
            title: 'Databricks Run ID',
            prompt: 'Enter the run ID to fetch details',
            ignoreFocusOut: true,
            validateInput: value => {
                if (!value.trim()) {
                    return 'Run ID is required';
                }
                return /^\d+$/.test(value.trim()) ? null : 'Run ID must be a number';
            },
        });
        if (!runIdInput) {
            return;
        }
        const includeRawJsonPick = await vscode.window.showQuickPick([
            { label: 'No', description: 'Summarized output', value: false },
            { label: 'Yes', description: 'Include raw JSON', value: true },
        ], { title: 'Include raw JSON output?', ignoreFocusOut: true });
        const includeRawJson = includeRawJsonPick?.value ?? false;
        const runId = Number(runIdInput.trim());
        const cts = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            const run = await client.getRunDetails(runId);
            const markdown = formatRunDetails(run, includeRawJson);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            void vscode.window.showInformationMessage(`Fetched details for run ${runId}.`);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'run details');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.analyzeRunPerformance', async () => {
        const runIdInput = await vscode.window.showInputBox({
            title: 'Databricks Run ID',
            prompt: 'Enter the run ID to analyze performance',
            ignoreFocusOut: true,
            validateInput: value => {
                if (!value.trim()) {
                    return 'Run ID is required';
                }
                return /^\d+$/.test(value.trim()) ? null : 'Run ID must be a number';
            },
        });
        if (!runIdInput) {
            return;
        }
        const includeRawJsonPick = await vscode.window.showQuickPick([
            { label: 'No', description: 'Summary only', value: false },
            { label: 'Yes', description: 'Include raw JSON', value: true },
        ], { title: 'Include raw JSON output?', ignoreFocusOut: true });
        const includeRawJson = includeRawJsonPick?.value ?? false;
        const runId = Number(runIdInput.trim());
        const cts = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            const markdown = await analyzeRunPerformance(client, runId, includeRawJson, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            void vscode.window.showInformationMessage(`Performance analysis ready for run ${runId}.`);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'run performance analysis');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.analyzeRunStages', async () => {
        const runIdInput = await vscode.window.showInputBox({
            title: 'Databricks Run ID',
            prompt: 'Enter the run ID to analyze Spark stages',
            ignoreFocusOut: true,
            validateInput: value => {
                if (!value.trim()) {
                    return 'Run ID is required';
                }
                return /^\d+$/.test(value.trim()) ? null : 'Run ID must be a number';
            },
        });
        if (!runIdInput) {
            return;
        }
        const runId = Number(runIdInput.trim());
        const cts = new vscode.CancellationTokenSource();
        try {
            const markdown = await analyzeRunStages(context, { runId }, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            void vscode.window.showInformationMessage(`Stage analysis ready for run ${runId}.`);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'run stages analysis');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.summarizeJobHistory', async () => {
        const jobIdInput = await vscode.window.showInputBox({
            title: 'Databricks Job ID',
            prompt: 'Enter the job ID to summarize history',
            ignoreFocusOut: true,
            validateInput: value => {
                if (!value.trim())
                    return 'Job ID is required';
                return /^\d+$/.test(value.trim()) ? null : 'Job ID must be numeric';
            },
        });
        if (!jobIdInput) {
            return;
        }
        const limitInput = await vscode.window.showInputBox({
            title: 'History limit (optional)',
            prompt: 'Number of recent runs to include (default 20)',
            ignoreFocusOut: true,
            validateInput: value => {
                if (!value.trim())
                    return null;
                return /^\d+$/.test(value.trim()) ? null : 'Enter a positive integer or leave blank';
            },
        });
        const limit = limitInput && limitInput.trim() ? Number(limitInput.trim()) : undefined;
        const jobId = Number(jobIdInput.trim());
        const cts = new vscode.CancellationTokenSource();
        try {
            const markdown = await summarizeJobHistory(context, { jobId, limit }, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'summarize job history');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.profileTableLayout', async () => {
        const targetMode = await vscode.window.showQuickPick([
            { label: 'Unity Catalog table', description: 'catalog.schema.table', value: 'table' },
            { label: 'Delta path', description: 'dbfs:/... or /Volumes/...', value: 'path' },
        ], { title: 'Profile target', ignoreFocusOut: true });
        if (!targetMode) {
            return;
        }
        let tableName;
        let tablePath;
        if (targetMode.value === 'table') {
            const input = await vscode.window.showInputBox({
                title: 'Table name',
                prompt: 'Enter full table name, e.g., catalog.schema.table',
                ignoreFocusOut: true,
                validateInput: value => (value.trim() ? null : 'Table name is required'),
            });
            if (!input) {
                return;
            }
            tableName = input.trim();
        }
        else {
            const input = await vscode.window.showInputBox({
                title: 'Table path',
                prompt: 'Enter Delta table path, e.g., dbfs:/mnt/data/table or /Volumes/catalog/schema/table',
                ignoreFocusOut: true,
                validateInput: value => (value.trim() ? null : 'Table path is required'),
            });
            if (!input) {
                return;
            }
            tablePath = input.trim();
        }
        const historyLimitInput = await vscode.window.showInputBox({
            title: 'History rows (optional)',
            prompt: 'Number of history entries to include (default 5)',
            ignoreFocusOut: true,
            validateInput: value => {
                if (!value.trim()) {
                    return null;
                }
                return /^\d+$/.test(value.trim()) ? null : 'Enter a non-negative integer or leave empty';
            },
        });
        const historyLimit = historyLimitInput && historyLimitInput.trim() ? Number(historyLimitInput.trim()) : undefined;
        const clusterModePick = await vscode.window.showQuickPick([
            { label: 'Use default cluster (recommended)', value: 'defaultCluster' },
            { label: 'Pick an existing cluster', value: 'existingCluster' },
        ], { title: 'Choose cluster', ignoreFocusOut: true });
        if (!clusterModePick) {
            return;
        }
        let existingClusterId;
        if (clusterModePick.value === 'existingCluster') {
            const ctsPick = new vscode.CancellationTokenSource();
            try {
                const client = await databricksClient_1.DatabricksClient.fromConfig(context);
                const picked = await pickClusterFromList(client, ctsPick.token, {
                    title: 'Select existing cluster for profiling',
                    placeHolder: 'Cluster to run profiling job',
                });
                if (!picked?.id) {
                    return;
                }
                existingClusterId = picked.id;
            }
            finally {
                ctsPick.dispose();
            }
        }
        const cts = new vscode.CancellationTokenSource();
        try {
            const markdown = await profileTableLayout(context, { tableName, tablePath, historyLimit, clusterMode: clusterModePick.value, existingClusterId }, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            void vscode.window.showInformationMessage('Table layout profiling completed.');
        }
        catch (err) {
            const message = formatDatabricksError(err, 'table layout profiling');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.profileTableStats', async () => {
        const cts = new vscode.CancellationTokenSource();
        try {
            const input = await promptTableStatsInput(context, cts.token);
            if (!input) {
                return;
            }
            const markdown = await profileTableStats(context, input, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            void vscode.window.showInformationMessage('Table stats profiling completed.');
        }
        catch (err) {
            const message = formatDatabricksError(err, 'table stats profiling');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.addArtifactMapping', async () => {
        try {
            await addArtifactMapping();
        }
        catch (err) {
            const message = formatDatabricksError(err, 'add artifact mapping');
            void vscode.window.showErrorMessage(message);
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.runCodeAndGetResult', async () => {
        const editor = vscode.window.activeTextEditor;
        const selection = editor?.selection && !editor.selection.isEmpty ? editor.document.getText(editor.selection) : undefined;
        const source = selection || editor?.document.getText() || '';
        if (!source.trim()) {
            void vscode.window.showErrorMessage('No code to run. Open a file or select code before running.');
            return;
        }
        const languageId = editor?.document.languageId?.toLowerCase() ?? 'python';
        const language = languageId === 'sql'
            ? 'SQL'
            : languageId === 'scala'
                ? 'SCALA'
                : languageId === 'r'
                    ? 'R'
                    : 'PYTHON';
        if (language !== 'PYTHON') {
            void vscode.window.showErrorMessage('Only PYTHON code is supported for returning JSON results.');
            return;
        }
        const fileName = editor?.document.uri ? vscode.workspace.asRelativePath(editor.document.uri) : undefined;
        const jobName = fileName ? `Run ${fileName}` : 'Ad hoc code run';
        const cts = new vscode.CancellationTokenSource();
        try {
            const markdown = await runCodeAndGetResult(context, {
                jobName,
                sourceCode: source,
                language,
                clusterMode: 'defaultCluster',
            }, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
            void vscode.window.showInformationMessage('Databricks run completed.');
        }
        catch (err) {
            const message = formatDatabricksError(err, 'run code and get result');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.executeSqlOnCluster', async () => {
        const sql = await vscode.window.showInputBox({
            title: 'Databricks SQL (all-purpose cluster)',
            prompt: 'Enter a SQL statement to run on the default all-purpose cluster',
            ignoreFocusOut: true,
        });
        if (!sql?.trim()) {
            return;
        }
        const cts = new vscode.CancellationTokenSource();
        try {
            const markdown = await executeSqlOnCluster(context, { sql: sql.trim(), maxRows: 1000, timeoutSeconds: 60 }, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'execute SQL on cluster');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.explainSql', async () => {
        const sql = await vscode.window.showInputBox({
            title: 'EXPLAIN SQL',
            prompt: 'Enter a SQL statement to explain (EXPLAIN FORMATTED)',
            ignoreFocusOut: true,
        });
        if (!sql?.trim()) {
            return;
        }
        const cts = new vscode.CancellationTokenSource();
        try {
            const markdown = await explainSql(context, { sql: sql.trim() }, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'explain SQL');
            void vscode.window.showErrorMessage(message);
        }
        finally {
            cts.dispose();
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.executePythonOnCluster', async () => {
        const editor = vscode.window.activeTextEditor;
        const selection = editor?.selection && !editor.selection.isEmpty ? editor.document.getText(editor.selection) : undefined;
        const code = selection || editor?.document.getText() || (await vscode.window.showInputBox({
            title: 'Databricks Python (all-purpose cluster)',
            prompt: 'Enter Python code to run on the default all-purpose cluster',
            ignoreFocusOut: true,
        }));
        if (!code?.trim()) {
            return;
        }
        const cts = new vscode.CancellationTokenSource();
        try {
            const markdown = await executePythonOnCluster(context, { code: code.trim(), timeoutSeconds: 60 }, cts.token);
            const output = (0, databricksClient_1.getOutputChannel)();
            output.appendLine(markdown);
            output.show(true);
        }
        catch (err) {
            const message = formatDatabricksError(err, 'execute Python on cluster');
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
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.showClusterDefinition', async () => {
        await showClusterDefinition(context);
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
async function pickClusterFromList(client, token, options) {
    let clusters = [];
    try {
        clusters = await client.listClusters(token);
    }
    catch (err) {
        const message = formatDatabricksError(err, 'cluster list');
        void vscode.window.showErrorMessage(message);
        return undefined;
    }
    if (clusters.length === 0) {
        void vscode.window.showInformationMessage('No clusters found in the workspace.');
        return undefined;
    }
    const items = clusters.slice(0, 200).map(c => {
        const label = c.cluster_name ?? c.cluster_id ?? 'Cluster';
        const description = c.cluster_id ?? '';
        const detail = `state=${c.state ?? 'n/a'}, source=${c.cluster_source ?? 'n/a'}, runtime=${c.spark_version ?? 'n/a'}`;
        return {
            label,
            description,
            detail,
            cluster: {
                id: c.cluster_id ?? '',
                name: c.cluster_name,
                state: c.state,
                source: c.cluster_source,
            },
        };
    });
    const pick = await vscode.window.showQuickPick(items, {
        title: options.title,
        placeHolder: options.placeHolder ?? 'Select a cluster',
        ignoreFocusOut: true,
    });
    return pick?.cluster?.id ? pick.cluster : undefined;
}
async function chooseDefaultCluster(context, client, token, options) {
    const currentDefault = await (0, databricksClient_1.getDefaultCluster)(context);
    let clusters = [];
    try {
        clusters = await client.listClusters(token);
    }
    catch (err) {
        const message = formatDatabricksError(err, 'cluster list');
        void vscode.window.showErrorMessage(message);
        return undefined;
    }
    if (clusters.length === 0) {
        void vscode.window.showInformationMessage('No clusters found to set as default.');
        return undefined;
    }
    const items = clusters.map(c => {
        const label = c.cluster_name ?? c.cluster_id ?? 'Cluster';
        const description = c.cluster_id ?? '';
        const detail = `state=${c.state ?? 'n/a'}, source=${c.cluster_source ?? 'n/a'}, runtime=${c.spark_version ?? 'n/a'}`;
        return {
            label,
            description,
            detail,
            cluster: { id: c.cluster_id ?? '', name: c.cluster_name, state: c.state, source: c.cluster_source },
        };
    });
    if (options?.allowClear && currentDefault.id) {
        items.push({
            label: 'Clear default cluster',
            description: currentDefault.id,
            detail: currentDefault.name ?? '',
            clear: true,
        });
    }
    const pick = await vscode.window.showQuickPick(items, {
        title: options?.title ?? 'Select default cluster',
        placeHolder: 'Pick a cluster to set as default',
        ignoreFocusOut: true,
    });
    if (!pick) {
        return undefined;
    }
    if (pick.clear) {
        await (0, databricksClient_1.clearDefaultCluster)(context);
        return undefined;
    }
    if (!pick.cluster || !pick.cluster.id) {
        void vscode.window.showErrorMessage('Cluster selection is invalid.');
        return undefined;
    }
    await (0, databricksClient_1.setDefaultCluster)(context, pick.cluster.id, pick.cluster.name ?? pick.cluster.id);
    void vscode.window.showInformationMessage(`Default cluster set to ${pick.cluster.name ?? pick.cluster.id} (${pick.cluster.id}).`);
    return pick.cluster;
}
async function ensureDefaultClusterSelected(context, client, token) {
    const current = await (0, databricksClient_1.getDefaultCluster)(context);
    if (current.id) {
        return { id: current.id, name: current.name };
    }
    return chooseDefaultCluster(context, client, token, { allowClear: false, title: 'Select a default cluster' });
}
async function showClusterDefinition(context) {
    const cts = new vscode.CancellationTokenSource();
    try {
        const includeRawPick = await vscode.window.showQuickPick([
            { label: 'Summary only', value: false },
            { label: 'Summary + raw JSON (redacted)', value: true },
        ], { title: 'Cluster definition output', ignoreFocusOut: true });
        if (!includeRawPick) {
            return;
        }
        const client = await databricksClient_1.DatabricksClient.fromConfig(context);
        const defaultCluster = await (0, databricksClient_1.getDefaultCluster)(context);
        const picks = [];
        if (defaultCluster.id) {
            picks.push({
                label: defaultCluster.name ? `Use default: ${defaultCluster.name}` : 'Use default cluster',
                description: defaultCluster.id,
                detail: 'databricksTools.defaultClusterId',
            });
        }
        picks.push({ label: 'Choose different cluster…', description: 'Select from cluster list' });
        const clusterPick = await vscode.window.showQuickPick(picks, {
            title: 'Select cluster for definition',
            ignoreFocusOut: true,
        });
        if (!clusterPick) {
            return;
        }
        let clusterId = defaultCluster.id;
        if (clusterPick.description !== defaultCluster.id) {
            let clusters = [];
            try {
                clusters = await client.listClusters(cts.token);
            }
            catch (err) {
                const message = formatDatabricksError(err, 'cluster list');
                void vscode.window.showErrorMessage(message);
                return;
            }
            if (!clusters.length) {
                void vscode.window.showInformationMessage('No clusters found. Set a default cluster and try again.');
                return;
            }
            const items = clusters.slice(0, 200).map(c => ({
                label: c.cluster_name ?? c.cluster_id ?? 'Cluster',
                description: c.cluster_id ?? '',
                detail: `state=${c.state ?? 'n/a'}, source=${c.cluster_source ?? 'n/a'}, runtime=${c.spark_version ?? 'n/a'}`,
            }));
            const pick = await vscode.window.showQuickPick(items, {
                title: 'Choose cluster',
                placeHolder: 'Pick a Databricks cluster',
                ignoreFocusOut: true,
            });
            clusterId = pick?.description ?? defaultCluster.id;
        }
        if (!clusterId) {
            void vscode.window.showErrorMessage('No cluster selected. Set a default cluster and try again.');
            return;
        }
        const markdown = await getClusterDefinitionMarkdown(context, { clusterId, includeRawJson: includeRawPick.value }, cts.token);
        const output = (0, databricksClient_1.getOutputChannel)();
        output.appendLine('');
        output.appendLine(markdown);
        output.show(true);
    }
    catch (err) {
        const message = formatDatabricksError(err, 'cluster definition');
        void vscode.window.showErrorMessage(message);
    }
    finally {
        cts.dispose();
    }
}
async function ensureClusterReady(client, clusterId, options) {
    let initialState;
    let clusterName;
    let clusterSource;
    try {
        const details = await client.getCluster(clusterId);
        initialState = details.state?.toUpperCase();
        clusterName = details.cluster_name;
        clusterSource = details.cluster_source;
    }
    catch {
        // best-effort, ignore
    }
    const status = await client.ensureClusterRunning(clusterId, {
        poll: true,
        timeoutMs: options?.timeoutMs ?? 10 * 60 * 1000,
        pollIntervalMs: options?.pollIntervalMs ?? 10 * 1000,
    });
    let finalState = status === 'RUNNING' ? 'RUNNING' : undefined;
    try {
        const details = await client.getCluster(clusterId);
        finalState = details.state?.toUpperCase() ?? finalState;
        clusterName = clusterName ?? details.cluster_name;
        clusterSource = clusterSource ?? details.cluster_source;
    }
    catch {
        // ignore
    }
    return { status, initialState, finalState, clusterName, clusterSource };
}
async function resolveClusterForExecution(context, client, mode, existingClusterId, token) {
    let clusterId = existingClusterId;
    let clusterName;
    let clusterSource;
    if (mode === 'defaultCluster') {
        const def = await ensureDefaultClusterSelected(context, client, token);
        if (!def || !def.id) {
            throw new Error('No default cluster is configured. Please select a default cluster.');
        }
        clusterId = def.id;
        clusterName = def.name;
        clusterSource = def.source;
    }
    else {
        if (!clusterId) {
            throw new Error('existingClusterId is required when clusterMode is existingCluster.');
        }
        try {
            const details = await client.getCluster(clusterId);
            clusterName = details.cluster_name;
            clusterSource = details.cluster_source;
        }
        catch {
            // best-effort; continue
        }
    }
    const readiness = await ensureClusterReady(client, clusterId, {
        timeoutMs: 10 * 60 * 1000,
        pollIntervalMs: 10 * 1000,
    });
    if (readiness.status === 'ERROR') {
        if (mode === 'defaultCluster') {
            throw new Error('Default cluster could not be started, please check in Databricks UI.');
        }
        throw new Error('Cluster could not be started. Please verify the cluster in Databricks.');
    }
    const autoStarted = readiness.initialState ? readiness.initialState !== 'RUNNING' : false;
    return {
        clusterId: clusterId,
        clusterName: readiness.clusterName ?? clusterName,
        clusterSource: readiness.clusterSource ?? clusterSource,
        initialState: readiness.initialState,
        finalState: readiness.finalState,
        autoStarted,
    };
}
async function resolveClusterForStart(context, client, token, inputClusterId, options) {
    if (inputClusterId) {
        let name;
        try {
            const details = await client.getCluster(inputClusterId);
            name = details.cluster_name;
        }
        catch {
            // ignore
        }
        return { id: inputClusterId, name };
    }
    const currentDefault = await (0, databricksClient_1.getDefaultCluster)(context);
    if (currentDefault.id) {
        return { id: currentDefault.id, name: currentDefault.name };
    }
    const picked = await pickClusterFromList(client, token, { title: 'Select a cluster to start' });
    if (!picked) {
        return undefined;
    }
    if (options?.promptToSetDefault) {
        const setDefaultPick = await vscode.window.showQuickPick([
            { label: 'Yes', description: 'Set this as the default cluster', value: true },
            { label: 'No', description: 'Use just for now', value: false },
        ], { title: 'Set this cluster as default?', ignoreFocusOut: true });
        if (setDefaultPick?.value) {
            await (0, databricksClient_1.setDefaultCluster)(context, picked.id, picked.name ?? picked.id);
            void vscode.window.showInformationMessage(`Default cluster set to ${picked.name ?? picked.id}.`);
        }
    }
    return picked;
}
async function promptCreateJobFromCodeInput(context, client, token) {
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
        {
            label: 'Use default cluster (recommended)',
            description: 'Auto-use saved default cluster; will prompt if none is set',
            value: 'defaultCluster',
        },
        { label: 'Use existing cluster', value: 'existingCluster' },
        { label: 'Create new job cluster', value: 'newJobCluster' },
    ], { title: 'Cluster selection', placeHolder: 'Choose how to run the job', ignoreFocusOut: true });
    if (!clusterModePick) {
        return undefined;
    }
    if (clusterModePick.value === 'defaultCluster') {
        const selectedDefault = await ensureDefaultClusterSelected(context, client, token);
        if (!selectedDefault) {
            return undefined;
        }
        return {
            jobName: jobName.trim(),
            sourceCode,
            language,
            workspacePath: workspacePath?.trim() || undefined,
            clusterMode: 'defaultCluster',
        };
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
async function promptTableStatsInput(context, token) {
    const targetMode = await vscode.window.showQuickPick([
        { label: 'Unity Catalog table', description: 'catalog.schema.table', value: 'table' },
        { label: 'Delta/Volume path', description: 'dbfs:/... or /Volumes/...', value: 'path' },
    ], { title: 'Profile target', ignoreFocusOut: true });
    if (!targetMode) {
        return undefined;
    }
    let tableName;
    let path;
    if (targetMode.value === 'table') {
        const input = await vscode.window.showInputBox({
            title: 'Table name',
            prompt: 'Enter full table name, e.g., catalog.schema.table',
            ignoreFocusOut: true,
            validateInput: value => (value.trim() ? null : 'Table name is required'),
        });
        if (!input) {
            return undefined;
        }
        tableName = input.trim();
    }
    else {
        const input = await vscode.window.showInputBox({
            title: 'Table path',
            prompt: 'Enter table/storage path, e.g., dbfs:/..., abfss:/..., or /Volumes/...',
            ignoreFocusOut: true,
            validateInput: value => (value.trim() ? null : 'Table path is required'),
        });
        if (!input) {
            return undefined;
        }
        path = input.trim();
    }
    const maxColumnsInput = await vscode.window.showInputBox({
        title: 'Max columns (optional)',
        prompt: 'Maximum columns to profile (default 50)',
        ignoreFocusOut: true,
        validateInput: value => {
            if (!value.trim()) {
                return null;
            }
            return /^\d+$/.test(value.trim()) ? null : 'Enter a non-negative integer or leave empty';
        },
    });
    const maxColumns = maxColumnsInput && maxColumnsInput.trim() ? Number(maxColumnsInput.trim()) : undefined;
    const clusterModePick = await vscode.window.showQuickPick([
        { label: 'Use default cluster (recommended)', value: 'defaultCluster' },
        { label: 'Pick an existing cluster', value: 'existingCluster' },
        { label: 'Create new job cluster', value: 'newJobCluster' },
    ], { title: 'Choose cluster', ignoreFocusOut: true });
    if (!clusterModePick) {
        return undefined;
    }
    if (clusterModePick.value === 'defaultCluster') {
        return { tableName, path, clusterMode: 'defaultCluster', maxColumns };
    }
    if (clusterModePick.value === 'existingCluster') {
        const ctsPick = new vscode.CancellationTokenSource();
        try {
            const client = await databricksClient_1.DatabricksClient.fromConfig(context);
            const picked = await pickClusterFromList(client, ctsPick.token, {
                title: 'Select existing cluster for profiling',
                placeHolder: 'Cluster to run profiling job',
            });
            if (!picked?.id) {
                return undefined;
            }
            return { tableName, path, clusterMode: 'existingCluster', existingClusterId: picked.id, maxColumns };
        }
        finally {
            ctsPick.dispose();
        }
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
        tableName,
        path,
        clusterMode: 'newJobCluster',
        newClusterConfig: {
            sparkVersion: sparkVersion.trim(),
            nodeTypeId: nodeTypeId.trim(),
            numWorkers: parseOptionalPositiveInt(numWorkersInput),
            autoTerminationMinutes: parseOptionalPositiveInt(autoTermInput),
        },
        maxColumns,
    };
}
async function createAndRunJobFromCode(context, input, token) {
    const jobName = input.jobName?.trim();
    const sourceCode = input.sourceCode;
    if (!jobName || !sourceCode) {
        throw new Error('jobName and sourceCode are required to create and run a job.');
    }
    const language = (input.language ?? 'PYTHON').toUpperCase();
    const requestedClusterMode = input.clusterMode ?? 'defaultCluster';
    const effectiveToken = token ?? new vscode.CancellationTokenSource().token;
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    let newClusterConfig;
    let clusterInfo = { mode: requestedClusterMode };
    let clusterModeForJob;
    let existingClusterId = input.existingClusterId;
    if (requestedClusterMode === 'newJobCluster') {
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
        clusterModeForJob = 'newJobCluster';
    }
    else {
        const resolved = await resolveClusterForExecution(context, client, requestedClusterMode, existingClusterId, effectiveToken);
        clusterModeForJob = 'existingCluster';
        existingClusterId = resolved.clusterId;
        clusterInfo = {
            mode: requestedClusterMode,
            clusterId: resolved.clusterId,
            clusterName: resolved.clusterName,
            clusterSource: resolved.clusterSource,
            initialState: resolved.initialState,
            finalState: resolved.finalState,
            autoStarted: resolved.autoStarted,
        };
    }
    const uploadSettings = (0, databricksClient_1.getWorkspaceUploadSettings)();
    const projectName = vscode.workspace.workspaceFolders?.[0]?.name;
    const targetPath = (0, workspacePath_1.computeUploadWorkspacePath)({
        explicitWorkspacePath: input.workspacePath,
        jobName,
        language,
        defaultWorkspaceFolder: uploadSettings.folder,
        appendProjectSubfolder: uploadSettings.appendProjectSubfolder,
        projectName,
    });
    await client.importWorkspaceSource(targetPath, language, sourceCode);
    const { jobId } = await client.createJobFromNotebook(jobName, targetPath, clusterModeForJob, {
        existingClusterId,
        newClusterConfig,
    });
    const { runId } = await client.runJobNow(jobId);
    return formatCreateAndRunJobResult(jobId, runId, jobName, targetPath, clusterInfo, newClusterConfig);
}
async function runCodeAndGetResult(context, input, token) {
    const jobName = input.jobName?.trim() || 'Ad hoc code run';
    const sourceCode = input.sourceCode;
    if (!sourceCode?.trim()) {
        throw new Error('sourceCode is required to run code.');
    }
    const language = (input.language ?? 'PYTHON').toUpperCase();
    if (language !== 'PYTHON') {
        throw new Error('runCodeAndGetResult currently supports only PYTHON.');
    }
    const maxRows = normalizeRowLimit(input.maxRows);
    const requestedClusterMode = input.clusterMode ?? 'defaultCluster';
    const effectiveToken = token ?? new vscode.CancellationTokenSource().token;
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    let newClusterConfig;
    let clusterInfo = { mode: requestedClusterMode };
    let clusterModeForJob;
    let existingClusterId = input.existingClusterId;
    if (requestedClusterMode === 'newJobCluster') {
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
        clusterModeForJob = 'newJobCluster';
    }
    else {
        const resolved = await resolveClusterForExecution(context, client, requestedClusterMode, existingClusterId, effectiveToken);
        clusterModeForJob = 'existingCluster';
        existingClusterId = resolved.clusterId;
        clusterInfo = {
            mode: requestedClusterMode,
            clusterId: resolved.clusterId,
            clusterName: resolved.clusterName,
            clusterSource: resolved.clusterSource,
            initialState: resolved.initialState,
            finalState: resolved.finalState,
            autoStarted: resolved.autoStarted,
        };
    }
    const uploadSettings = (0, databricksClient_1.getWorkspaceUploadSettings)();
    const projectName = vscode.workspace.workspaceFolders?.[0]?.name;
    const targetPath = (0, workspacePath_1.computeUploadWorkspacePath)({
        explicitWorkspacePath: input.workspacePath,
        jobName,
        language,
        defaultWorkspaceFolder: uploadSettings.folder,
        appendProjectSubfolder: uploadSettings.appendProjectSubfolder,
        projectName,
    });
    const wrappedSource = buildResultNotebookSource(sourceCode, { maxRows });
    await client.importWorkspaceSource(targetPath, language, wrappedSource);
    const runSubmission = await client.submitSingleTaskRun({
        runName: jobName,
        notebookPath: targetPath,
        cluster: clusterModeForJob === 'existingCluster'
            ? { type: 'existing', id: existingClusterId }
            : { type: 'new', config: newClusterConfig },
    }, effectiveToken);
    const runId = runSubmission.runId;
    const runDetails = await waitForRunCompletion(client, runId, effectiveToken, {
        pollIntervalMs: 5000,
        timeoutMs: 15 * 60 * 1000,
    });
    if (!clusterInfo.clusterId && runDetails.cluster_instance?.cluster_id) {
        clusterInfo = {
            ...clusterInfo,
            clusterId: runDetails.cluster_instance.cluster_id,
        };
    }
    const output = await client.getRunOutput(runId, effectiveToken);
    const raw = output.notebook_output?.result ?? output.notebook_output?.error ?? output.error ?? output.error_trace;
    if (!raw) {
        return formatRunResultMarkdown({
            runId,
            jobName,
            path: targetPath,
            clusterInfo,
            resultJson: undefined,
            rawText: undefined,
            truncated: false,
            rowLimit: maxRows,
        });
    }
    const limited = (0, workspacePath_1.capJsonString)(raw, input.maxResultBytes);
    let parsed;
    let parseError;
    try {
        parsed = JSON.parse(limited.text);
    }
    catch (err) {
        parseError = err instanceof Error ? err.message : String(err);
    }
    return formatRunResultMarkdown({
        runId,
        jobName,
        path: targetPath,
        clusterInfo,
        resultJson: parsed,
        rawText: parseError ? limited.text : undefined,
        truncated: limited.truncated,
        parseError,
        rowLimit: maxRows,
    });
}
async function executeSqlOnCluster(context, input, token) {
    const sql = input.sql?.trim();
    if (!sql) {
        throw new Error('SQL statement is required.');
    }
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    const clusterId = input.clusterId?.trim() || (await (0, databricksClient_1.getDefaultCluster)(context)).id;
    if (!clusterId) {
        return 'No default all-purpose cluster configured. Please select one in the Databricks Tools view.';
    }
    let clusterName;
    try {
        const details = await client.getCluster(clusterId);
        clusterName = details.cluster_name;
    }
    catch {
        // best effort
    }
    const ready = await client.ensureClusterRunning(clusterId, { poll: true, timeoutMs: 8 * 60 * 1000, pollIntervalMs: 10 * 1000 });
    if (ready === 'ERROR') {
        throw new Error('Cluster could not be started. Please verify the default cluster in Databricks.');
    }
    const result = await client.executeClusterSql({
        sql,
        clusterId,
        maxRows: normalizeRowLimit(input.maxRows ?? 1000),
        timeoutSeconds: input.timeoutSeconds ?? 60,
    }, token);
    return formatSqlCommandResult({
        clusterId,
        clusterName,
        sql,
        result,
    });
}
async function executePythonOnCluster(context, input, token) {
    const code = input.code?.trim();
    if (!code) {
        throw new Error('Python code is required.');
    }
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    const clusterId = input.clusterId?.trim() || (await (0, databricksClient_1.getDefaultCluster)(context)).id;
    if (!clusterId) {
        return 'No default all-purpose cluster configured. Please select one in the Databricks Tools view.';
    }
    let clusterName;
    try {
        const details = await client.getCluster(clusterId);
        clusterName = details.cluster_name;
    }
    catch {
        // best effort
    }
    const ready = await client.ensureClusterRunning(clusterId, { poll: true, timeoutMs: 8 * 60 * 1000, pollIntervalMs: 10 * 1000 });
    if (ready === 'ERROR') {
        throw new Error('Cluster could not be started. Please verify the default cluster in Databricks.');
    }
    const result = await client.executeClusterPython({
        code,
        clusterId,
        timeoutSeconds: input.timeoutSeconds ?? 60,
    }, token);
    return formatPythonCommandResult({ clusterId, clusterName, code, result });
}
const TABLE_PROFILE_NOTEBOOK_SOURCE = [
    '# Databricks table layout profiler',
    'import json',
    'table_name = dbutils.widgets.get("tableName") or ""',
    'table_path = dbutils.widgets.get("tablePath") or ""',
    'history_limit_raw = dbutils.widgets.get("historyLimit") or "5"',
    'history_limit = int(history_limit_raw) if history_limit_raw else 5',
    '',
    'def quote_identifier(name: str) -> str:',
    '    parts = [p for p in name.split(".") if p]',
    "    return '.'.join(['`' + p.replace('`', '``') + '`' for p in parts])",
    '',
    'def describe_detail(target_sql: str):',
    '    return spark.sql(target_sql).first().asDict()',
    '',
    'result = {}',
    '',
    'try:',
    '    if table_name:',
    '        target = f"DESCRIBE DETAIL {quote_identifier(table_name)}"',
    '    else:',
    "        safe_path = table_path.replace('`', '``')",
    '        target = f"DESCRIBE DETAIL delta.`{safe_path}`"',
    '    result["detail"] = describe_detail(target)',
    '',
    '    if table_name and history_limit > 0:',
    '        history_q = f"DESCRIBE HISTORY {quote_identifier(table_name)} LIMIT {history_limit}"',
    '        history_rows = [json.loads(r) for r in spark.sql(history_q).toJSON().take(history_limit)]',
    '        result["history"] = history_rows',
    '',
    '    dbutils.notebook.exit(json.dumps(result, default=str))',
    'except Exception as e:',
    '    dbutils.notebook.exit(json.dumps({"error": str(e)}))',
].join('\n');
const TABLE_STATS_NOTEBOOK_SOURCE = [
    '# Databricks table stats profiler',
    'import json',
    'from pyspark.sql import functions as F',
    'from pyspark.sql import types as T',
    '',
    'table_name = dbutils.widgets.get("tableName") or ""',
    'table_path = dbutils.widgets.get("path") or ""',
    'max_cols_raw = dbutils.widgets.get("maxColumns") or "50"',
    'max_cols = int(max_cols_raw) if max_cols_raw else 50',
    '',
    'def quote_identifier(name: str) -> str:',
    '    parts = [p for p in name.split(".") if p]',
    "    return '.'.join(['`' + p.replace('`', '``') + '`' for p in parts])",
    '',
    'def load_df():',
    '    if table_name:',
    '        return spark.table(table_name)',
    '    if table_path:',
    '        return spark.read.format("delta").load(table_path)',
    '    raise ValueError("tableName or path is required")',
    '',
    'def describe_detail():',
    '    if table_name:',
    '        target = f"DESCRIBE DETAIL {quote_identifier(table_name)}"',
    '    else:',
    "        safe_path = table_path.replace('`', '``')",
    '        target = f"DESCRIBE DETAIL delta.`{safe_path}`"',
    '    return spark.sql(target).first().asDict()',
    '',
    'def is_numeric(dt):',
    '    return isinstance(dt, (T.ByteType, T.ShortType, T.IntegerType, T.LongType, T.FloatType, T.DoubleType, T.DecimalType))',
    '',
    'def is_date_like(dt):',
    '    return isinstance(dt, (T.DateType, T.TimestampType))',
    '',
    'result = {}',
    'try:',
    '    detail = describe_detail()',
    '    df = load_df()',
    '    total_cols = len(df.schema)',
    '    cols = df.schema[:max_cols]',
    '    total_rows = detail.get("numRows")',
    '    try:',
    '        total_rows = int(total_rows) if total_rows is not None else None',
    '    except Exception:',
    '        total_rows = None',
    '    if total_rows is None:',
    '        total_rows = df.count()',
    '',
    '    col_stats = []',
    '    for field in cols:',
    '        col = F.col(field.name)',
    '        agg = df.agg(',
    '            F.count(F.when(col.isNotNull(), 1)).alias("nonNull"),',
    '            F.approx_count_distinct(col).alias("approxDistinct"),',
    '            F.min(col).alias("min"),',
    '            F.max(col).alias("max")',
    '        ).first().asDict()',
    '        non_null = agg.get("nonNull") or 0',
    '        nulls = total_rows - non_null if total_rows is not None else None',
    '        null_pct = (nulls / total_rows * 100) if (total_rows and nulls is not None) else None',
    '        approx_distinct = agg.get("approxDistinct")',
    '        min_v = agg.get("min") if (is_numeric(field.dataType) or is_date_like(field.dataType)) else None',
    '        max_v = agg.get("max") if (is_numeric(field.dataType) or is_date_like(field.dataType)) else None',
    '        col_stats.append({',
    '            "name": field.name,',
    '            "type": str(field.dataType),',
    '            "nullPercent": null_pct,',
    '            "approxDistinct": approx_distinct,',
    '            "min": min_v,',
    '            "max": max_v,',
    '            "isPartition": field.name in (detail.get("partitionColumns") or []),',
    '        })',
    '',
    '    result["detail"] = detail',
    '    result["columns"] = col_stats',
    '    result["totalColumns"] = total_cols',
    '    result["maxColumns"] = max_cols',
    '    dbutils.notebook.exit(json.dumps(result, default=str))',
    'except Exception as e:',
    '    dbutils.notebook.exit(json.dumps({"error": str(e)}))',
].join('\n');
async function analyzeRunPerformance(client, runId, includeRawJson, token) {
    const run = await client.getRunDetails(runId, token);
    let cluster;
    const clusterId = run.cluster_instance?.cluster_id;
    if (clusterId) {
        try {
            cluster = await client.getCluster(clusterId);
        }
        catch {
            // best-effort only
        }
    }
    return formatRunPerformance(run, cluster, includeRawJson);
}
async function analyzeRunStages(context, input, token) {
    const runId = input.runId;
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    const run = await client.getRunDetails(runId, token);
    const output = await client.getRunOutput(runId, token).catch(() => undefined);
    const clusterId = run.cluster_instance?.cluster_id;
    const runPageUrl = run.run_page_url;
    const appId = output?.metadata?.applicationId || run.cluster_instance?.spark_context_id;
    return formatRunStages(runId, {
        run,
        clusterId,
        runPageUrl,
        appId,
        logs: output?.logs,
    });
}
async function explainSql(context, input, token) {
    const sql = input.sql?.trim();
    if (!sql) {
        throw new Error('SQL statement is required to explain.');
    }
    const wrapped = `EXPLAIN FORMATTED ${sql}`;
    return executeSqlOnCluster(context, {
        sql: wrapped,
        clusterId: input.clusterId,
        maxRows: 200,
        timeoutSeconds: 120,
    }, token);
}
async function summarizeJobHistory(context, input, token) {
    const jobId = input.jobId;
    const limit = normalizeOptionalInt(typeof input.limit === 'number' ? input.limit : undefined, 20);
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    const runs = await client.listRuns(jobId, limit, token ?? new vscode.CancellationTokenSource().token);
    return formatJobHistory(jobId, runs, limit);
}
async function profileTableLayout(context, input, token) {
    const tableName = input.tableName?.trim();
    const tablePath = input.tablePath?.trim();
    if (!tableName && !tablePath) {
        throw new Error('Provide tableName or tablePath to profile the table layout.');
    }
    const historyLimitRaw = input.historyLimit;
    const historyLimit = normalizeOptionalInt(typeof historyLimitRaw === 'number' ? historyLimitRaw : undefined, 5);
    const clusterMode = input.clusterMode ?? 'defaultCluster';
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    const resolvedCluster = await resolveClusterForExecution(context, client, clusterMode, input.existingClusterId, token ?? new vscode.CancellationTokenSource().token);
    const uploadSettings = (0, databricksClient_1.getWorkspaceUploadSettings)();
    const projectName = vscode.workspace.workspaceFolders?.[0]?.name;
    const notebookPath = (0, workspacePath_1.computeUploadWorkspacePath)({
        jobName: 'table-layout-profile',
        language: 'PYTHON',
        defaultWorkspaceFolder: uploadSettings.folder,
        appendProjectSubfolder: uploadSettings.appendProjectSubfolder,
        projectName,
    });
    await client.importWorkspaceSource(notebookPath, 'PYTHON', TABLE_PROFILE_NOTEBOOK_SOURCE);
    const baseParameters = {
        tableName: tableName ?? '',
        tablePath: tablePath ?? '',
        historyLimit: historyLimit.toString(),
    };
    const runName = tableName ? `Profile ${tableName}` : `Profile ${tablePath}`;
    const { runId } = await client.submitNotebookRun(notebookPath, resolvedCluster.clusterId, runName, baseParameters, token);
    const runDetails = await waitForRunCompletion(client, runId, token, {
        pollIntervalMs: 5000,
        timeoutMs: 12 * 60 * 1000,
    });
    const output = await client.getRunOutput(runId, token);
    const parsed = parseTableProfileOutput(output.notebook_output?.result);
    return formatTableLayoutProfile({
        targetLabel: tableName ?? tablePath ?? 'table',
        runId,
        notebookPath,
        cluster: resolvedCluster,
        runState: runDetails.state?.life_cycle_state,
        resultState: runDetails.state?.result_state,
        detail: parsed.detail,
        history: parsed.history,
        error: parsed.error,
    });
}
async function profileTableStats(context, input, token) {
    const tableName = input.tableName?.trim();
    const path = input.path?.trim();
    if (!tableName && !path) {
        throw new Error('Provide tableName or path to profile table stats.');
    }
    const maxColumns = normalizeOptionalInt(typeof input.maxColumns === 'number' ? input.maxColumns : undefined, 50);
    const requestedClusterMode = input.clusterMode ?? 'defaultCluster';
    const effectiveToken = token ?? new vscode.CancellationTokenSource().token;
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    let clusterTarget;
    let clusterInfo = {};
    if (requestedClusterMode === 'newJobCluster') {
        const cfg = input.newClusterConfig;
        if (!cfg || !cfg.sparkVersion || !cfg.nodeTypeId) {
            throw new Error('newClusterConfig.sparkVersion and nodeTypeId are required when clusterMode is newJobCluster.');
        }
        const normalizedConfig = {
            sparkVersion: cfg.sparkVersion,
            nodeTypeId: cfg.nodeTypeId,
            numWorkers: normalizeOptionalInt(cfg.numWorkers, 1),
            autoTerminationMinutes: normalizeOptionalInt(cfg.autoTerminationMinutes, 60),
        };
        clusterTarget = { type: 'new', config: normalizedConfig };
        clusterInfo = {
            clusterId: 'job-cluster',
            clusterName: 'new job cluster',
            clusterSource: 'JOB',
            autoStarted: true,
        };
    }
    else {
        const resolved = await resolveClusterForExecution(context, client, requestedClusterMode, input.existingClusterId, effectiveToken);
        clusterTarget = { type: 'existing', id: resolved.clusterId };
        clusterInfo = {
            clusterId: resolved.clusterId,
            clusterName: resolved.clusterName,
            clusterSource: resolved.clusterSource,
            initialState: resolved.initialState,
            finalState: resolved.finalState,
            autoStarted: resolved.autoStarted,
        };
    }
    const uploadSettings = (0, databricksClient_1.getWorkspaceUploadSettings)();
    const projectName = vscode.workspace.workspaceFolders?.[0]?.name;
    const notebookPath = (0, workspacePath_1.computeUploadWorkspacePath)({
        jobName: 'table-stats-profile',
        language: 'PYTHON',
        defaultWorkspaceFolder: uploadSettings.folder,
        appendProjectSubfolder: uploadSettings.appendProjectSubfolder,
        projectName,
    });
    await client.importWorkspaceSource(notebookPath, 'PYTHON', TABLE_STATS_NOTEBOOK_SOURCE);
    const baseParameters = {
        tableName: tableName ?? '',
        path: path ?? '',
        maxColumns: maxColumns.toString(),
    };
    const runName = tableName ? `Table stats: ${tableName}` : `Table stats: ${path}`;
    const { runId } = await client.submitSingleTaskRun({
        runName,
        notebookPath,
        cluster: clusterTarget,
        baseParameters,
    }, effectiveToken);
    const runDetails = await waitForRunCompletion(client, runId, effectiveToken, {
        pollIntervalMs: 5000,
        timeoutMs: 15 * 60 * 1000,
    });
    const output = await client.getRunOutput(runId, effectiveToken);
    const parsed = parseTableStatsOutput(output.notebook_output?.result);
    const clusterIdFromRun = runDetails.cluster_instance?.cluster_id;
    const clusterId = clusterIdFromRun || clusterInfo.clusterId || 'unknown';
    return formatTableStatsProfile({
        targetLabel: tableName ?? path ?? 'table',
        runId,
        notebookPath,
        cluster: {
            clusterId,
            clusterName: clusterInfo.clusterName,
            clusterSource: clusterInfo.clusterSource,
            initialState: clusterInfo.initialState,
            finalState: clusterInfo.finalState,
            autoStarted: clusterInfo.autoStarted,
        },
        runState: runDetails.state?.life_cycle_state,
        resultState: runDetails.state?.result_state,
        detail: parsed.detail,
        columns: parsed.columns,
        totalColumns: parsed.totalColumns,
        maxColumns: parsed.maxColumns,
        error: parsed.error,
    });
}
function parseTableStatsOutput(result) {
    if (!result) {
        return {};
    }
    try {
        const parsed = JSON.parse(result);
        return parsed;
    }
    catch {
        return { error: 'Profiling job returned output that could not be parsed as JSON.' };
    }
}
function parseTableProfileOutput(result) {
    if (!result) {
        return {};
    }
    try {
        const parsed = JSON.parse(result);
        return parsed;
    }
    catch {
        return { error: 'Profiling job returned output that could not be parsed as JSON.' };
    }
}
async function waitForRunCompletion(client, runId, token, options) {
    const start = Date.now();
    const pollIntervalMs = options.pollIntervalMs ?? 5000;
    const timeoutMs = options.timeoutMs ?? 10 * 60 * 1000;
    let lastState;
    while (Date.now() - start < timeoutMs) {
        const run = await client.getRunDetails(runId, token);
        const state = run.state?.life_cycle_state;
        if (state && state !== lastState) {
            (0, databricksClient_1.getOutputChannel)().appendLine(`Run ${runId} state: ${state}`);
            lastState = state;
        }
        const normalized = (state || '').toUpperCase();
        if (normalized === 'TERMINATED' || normalized === 'INTERNAL_ERROR' || normalized === 'SKIPPED') {
            return run;
        }
        if (token?.isCancellationRequested) {
            throw new Error('Profiling was cancelled.');
        }
        await delay(pollIntervalMs);
    }
    throw new Error(`Run ${runId} did not complete within ${Math.round(timeoutMs / 1000)} seconds.`);
}
function formatRunPerformance(run, cluster, includeRawJson) {
    const lines = [];
    const state = run.state?.life_cycle_state ?? 'unknown';
    const result = run.state?.result_state ?? 'unknown';
    const message = run.state?.state_message ?? '';
    const start = run.start_time ? new Date(run.start_time).toLocaleString() : 'n/a';
    const end = run.end_time ? new Date(run.end_time).toLocaleString() : 'n/a';
    const durationSeconds = run.start_time && run.end_time ? ((run.end_time - run.start_time) / 1000).toFixed(1) : 'n/a';
    const setupSeconds = run.setup_duration != null ? (run.setup_duration / 1000).toFixed(1) : 'n/a';
    const executionSeconds = run.execution_duration != null ? (run.execution_duration / 1000).toFixed(1) : 'n/a';
    const cleanupSeconds = run.cleanup_duration != null ? (run.cleanup_duration / 1000).toFixed(1) : 'n/a';
    lines.push(`# Run performance for ${run.run_id ?? 'n/a'}`);
    lines.push(`- state: ${state}`);
    lines.push(`- result: ${result}`);
    if (message) {
        lines.push(`- message: ${message}`);
    }
    lines.push(`- start: ${start}`);
    lines.push(`- end: ${end}`);
    lines.push(`- duration_s: ${durationSeconds}`);
    lines.push(`- setup_s: ${setupSeconds}, execution_s: ${executionSeconds}, cleanup_s: ${cleanupSeconds}`);
    const clusterId = run.cluster_instance?.cluster_id ?? 'n/a';
    const clusterLabel = cluster?.cluster_name ?? clusterId;
    const clusterSource = cluster?.cluster_source ? ` (${cluster.cluster_source})` : '';
    lines.push(`- cluster: ${clusterLabel}${clusterSource}`);
    if (run.tasks?.length) {
        lines.push('');
        lines.push('## Tasks');
        lines.push('| task | notebook |');
        lines.push('| --- | --- |');
        for (const t of run.tasks) {
            lines.push(`| ${t.task_key ?? 'n/a'} | ${t.notebook_task?.notebook_path ?? 'n/a'} |`);
        }
    }
    if (includeRawJson) {
        lines.push('');
        lines.push('```json');
        lines.push(JSON.stringify(run, null, 2));
        lines.push('```');
    }
    return lines.join('\n');
}
function formatRunStages(runId, args) {
    const lines = [];
    lines.push(`# Run stage analysis`);
    lines.push(`- run_id: ${runId}`);
    lines.push(`- state: ${args.run.state?.life_cycle_state ?? 'unknown'} / ${args.run.state?.result_state ?? 'unknown'}`);
    const clusterId = args.clusterId ?? 'unknown';
    lines.push(`- cluster: ${clusterId}`);
    if (args.appId) {
        lines.push(`- spark_app_id: ${args.appId}`);
    }
    if (args.runPageUrl) {
        lines.push(`- spark_ui: ${args.runPageUrl}`);
    }
    lines.push('');
    lines.push('Stage-level metrics are not exposed via the Jobs API. Open the Spark UI link above to inspect stages, tasks, and shuffle details.');
    if (args.logs?.length) {
        lines.push('');
        lines.push('## Recent driver logs (truncated)');
        const tail = args.logs.slice(-50);
        lines.push('```');
        lines.push(tail.join('\n'));
        lines.push('```');
    }
    return lines.join('\n');
}
function formatTableLayoutProfile(args) {
    const lines = [];
    lines.push(`# Table layout profile`);
    lines.push(`- target: ${args.targetLabel}`);
    lines.push(`- run_id: ${args.runId}`);
    lines.push(`- run_state: ${args.runState ?? 'unknown'} / ${args.resultState ?? 'unknown'}`);
    const clusterLabel = args.cluster.clusterName ?? args.cluster.clusterId;
    const autoStartNote = args.cluster.autoStarted ? ` (auto-started from ${args.cluster.initialState ?? 'unknown'})` : '';
    const clusterSource = args.cluster.clusterSource ? ` [${args.cluster.clusterSource}]` : '';
    lines.push(`- cluster: ${clusterLabel} (${args.cluster.clusterId})${clusterSource}${autoStartNote}`);
    lines.push(`- notebook path: \`${args.notebookPath}\``);
    if (args.error) {
        lines.push('');
        lines.push(`Profiling failed: ${args.error}`);
        return lines.join('\n');
    }
    if (!args.detail) {
        lines.push('');
        lines.push('Profiling completed but no layout detail was returned.');
    }
    if (args.detail) {
        const format = String(args.detail['format'] ?? 'n/a');
        const numFiles = args.detail['numFiles'] != null ? String(args.detail['numFiles']) : 'n/a';
        const sizeBytes = typeof args.detail['sizeInBytes'] === 'number' ? args.detail['sizeInBytes'] : undefined;
        const numRows = args.detail['numRows'] != null ? String(args.detail['numRows']) : 'n/a';
        const partitions = Array.isArray(args.detail['partitionColumns'])
            ? args.detail['partitionColumns'].map(v => String(v)).join(', ')
            : 'n/a';
        const numPartitions = args.detail['numPartitions'] != null ? String(args.detail['numPartitions']) : 'n/a';
        const location = args.detail['location'] ? String(args.detail['location']) : 'n/a';
        lines.push('');
        lines.push('## Layout');
        lines.push(`- format: ${format}`);
        lines.push(`- size: ${sizeBytes != null ? formatSizeBytes(sizeBytes) : 'n/a'}`);
        lines.push(`- numFiles: ${numFiles}`);
        lines.push(`- numRows: ${numRows}`);
        lines.push(`- partitions: ${partitions}`);
        lines.push(`- numPartitions: ${numPartitions}`);
        lines.push(`- location: ${location}`);
    }
    if (args.history && args.history.length) {
        lines.push('');
        lines.push('## Recent operations');
        lines.push('| timestamp | operation | user | read_version | operation_metrics |');
        lines.push('| --- | --- | --- | --- | --- |');
        const rows = args.history.slice(0, 10);
        for (const row of rows) {
            const ts = row['timestamp'] ? String(row['timestamp']) : 'n/a';
            const op = row['operation'] ? String(row['operation']) : 'n/a';
            const user = row['user_id'] ? String(row['user_id']) : row['user_name'] ? String(row['user_name']) : 'n/a';
            const readVersion = row['readVersion'] != null ? String(row['readVersion']) : row['read_version'] != null ? String(row['read_version']) : 'n/a';
            const metrics = row['operationMetrics'] ?? row['operation_metrics'];
            const metricsStr = metrics ? JSON.stringify(metrics) : 'n/a';
            lines.push(`| ${ts} | ${op} | ${user} | ${readVersion} | ${metricsStr} |`);
        }
    }
    return lines.join('\n');
}
function formatTableStatsProfile(args) {
    const lines = [];
    lines.push('# Table stats profile');
    lines.push(`- target: ${args.targetLabel}`);
    lines.push(`- run_id: ${args.runId}`);
    lines.push(`- run_state: ${args.runState ?? 'unknown'} / ${args.resultState ?? 'unknown'}`);
    const clusterLabel = args.cluster.clusterName ?? args.cluster.clusterId ?? 'unknown';
    const autoStartNote = args.cluster.autoStarted ? ` (auto-started from ${args.cluster.initialState ?? 'unknown'})` : '';
    const clusterSource = args.cluster.clusterSource ? ` [${args.cluster.clusterSource}]` : '';
    lines.push(`- cluster: ${clusterLabel} (${args.cluster.clusterId})${clusterSource}${autoStartNote}`);
    lines.push(`- notebook path: \`${args.notebookPath}\``);
    if (args.error) {
        lines.push('');
        lines.push(`Profiling failed: ${args.error}`);
        return lines.join('\n');
    }
    if (args.detail) {
        const numRows = args.detail['numRows'] != null ? String(args.detail['numRows']) : 'n/a';
        const numFiles = args.detail['numFiles'] != null ? String(args.detail['numFiles']) : 'n/a';
        const sizeBytes = typeof args.detail['sizeInBytes'] === 'number' ? args.detail['sizeInBytes'] : undefined;
        const partitions = Array.isArray(args.detail['partitionColumns'])
            ? args.detail['partitionColumns'].map(v => String(v)).join(', ')
            : 'n/a';
        const location = args.detail['location'] ? String(args.detail['location']) : 'n/a';
        lines.push('');
        lines.push('## Table');
        lines.push(`- format: ${String(args.detail['format'] ?? 'n/a')}`);
        lines.push(`- rows: ${numRows}`);
        lines.push(`- files: ${numFiles}`);
        lines.push(`- size: ${sizeBytes != null ? formatSizeBytes(sizeBytes) : 'n/a'}`);
        lines.push(`- partitions: ${partitions}`);
        lines.push(`- location: ${location}`);
    }
    const cols = args.columns ?? [];
    if (cols.length) {
        lines.push('');
        const maxNote = args.maxColumns && args.totalColumns && args.totalColumns > args.maxColumns
            ? ` (showing first ${args.maxColumns} of ${args.totalColumns})`
            : '';
        lines.push(`## Columns${maxNote}`);
        lines.push('| column | type | null% | approx distinct | min | max | partition |');
        lines.push('| --- | --- | --- | --- | --- | --- | --- |');
        for (const col of cols) {
            const name = String(col['name'] ?? 'n/a');
            const type = String(col['type'] ?? 'n/a');
            const nullPct = typeof col['nullPercent'] === 'number' ? `${col['nullPercent'].toFixed(2)}%` : 'n/a';
            const approxDistinct = col['approxDistinct'] != null ? String(col['approxDistinct']) : 'n/a';
            const minVal = col['min'] != null ? String(col['min']) : 'n/a';
            const maxVal = col['max'] != null ? String(col['max']) : 'n/a';
            const isPartition = col['isPartition'] ? 'yes' : 'no';
            lines.push(`| ${name} | ${type} | ${nullPct} | ${approxDistinct} | ${minVal} | ${maxVal} | ${isPartition} |`);
        }
    }
    if (!cols.length) {
        lines.push('');
        lines.push('No column stats were returned. Check table access and retry.');
    }
    return lines.join('\n');
}
function formatJobHistory(jobId, runs, limit) {
    if (!runs.length) {
        return `No runs found for job ${jobId}.`;
    }
    let success = 0;
    let failed = 0;
    const durations = [];
    for (const run of runs) {
        const result = (run.state?.result_state ?? '').toUpperCase();
        if (result === 'SUCCESS') {
            success += 1;
        }
        else if (result) {
            failed += 1;
        }
        if (run.start_time && run.end_time) {
            durations.push(run.end_time - run.start_time);
        }
    }
    const avgMs = durations.length ? durations.reduce((a, b) => a + b, 0) / durations.length : undefined;
    const minMs = durations.length ? Math.min(...durations) : undefined;
    const maxMs = durations.length ? Math.max(...durations) : undefined;
    const lines = [];
    lines.push(`# Job history for job ${jobId}`);
    lines.push(`- runs analyzed: ${runs.length} (limit ${limit})`);
    lines.push(`- success: ${success}, failed/other: ${failed}`);
    lines.push(`- success rate: ${runs.length ? ((success / runs.length) * 100).toFixed(1) + '%' : 'n/a'}`);
    lines.push(`- duration ms (min/avg/max): ${minMs != null ? minMs.toFixed(0) : 'n/a'} / ${avgMs != null ? avgMs.toFixed(0) : 'n/a'} / ${maxMs != null ? maxMs.toFixed(0) : 'n/a'}`);
    lines.push('');
    lines.push('## Recent runs');
    lines.push('| run_id | state | result | duration_ms | started |');
    lines.push('| --- | --- | --- | --- | --- |');
    for (const run of runs.slice(0, limit)) {
        const duration = run.start_time && run.end_time ? (run.end_time - run.start_time).toFixed(0) : 'n/a';
        const started = run.start_time ? new Date(run.start_time).toISOString() : 'n/a';
        lines.push(`| ${run.run_id} | ${run.state?.life_cycle_state ?? 'n/a'} | ${run.state?.result_state ?? 'n/a'} | ${duration} | ${started} |`);
    }
    return lines.join('\n');
}
async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
function formatSizeBytes(bytes) {
    if (!Number.isFinite(bytes) || bytes < 0) {
        return 'n/a';
    }
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = bytes;
    let unitIndex = 0;
    while (size >= 1024 && unitIndex < units.length - 1) {
        size /= 1024;
        unitIndex += 1;
    }
    return `${size.toFixed(1)} ${units[unitIndex]}`;
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
function formatCreateAndRunJobResult(jobId, runId, jobName, path, clusterInfo, newClusterConfig) {
    const lines = [];
    lines.push('## Job created and run started on Databricks');
    lines.push('');
    lines.push(`Job ID: \`${jobId}\``);
    lines.push(`Run ID: \`${runId}\``);
    lines.push(`Run name: \`${jobName}\``);
    lines.push(`Workspace path: \`${path}\``);
    if (clusterInfo.mode === 'newJobCluster' && newClusterConfig) {
        lines.push(`Cluster mode: newJobCluster (runtime ${newClusterConfig.sparkVersion}, node ${newClusterConfig.nodeTypeId}, workers ${newClusterConfig.numWorkers ?? 1}, auto-term ${newClusterConfig.autoTerminationMinutes ?? 60}m)`);
    }
    else {
        const label = clusterInfo.mode === 'defaultCluster' ? 'default cluster' : 'existing cluster';
        const clusterLabel = clusterInfo.clusterName ?? clusterInfo.clusterId ?? 'n/a';
        const autoStartNote = clusterInfo.autoStarted ? ` — was ${clusterInfo.initialState ?? 'not running'}, started automatically.` : '';
        const clusterLine = 'Cluster: ' +
            label +
            ' `' +
            clusterLabel +
            '` (`' +
            (clusterInfo.clusterId ?? 'n/a') +
            '`)' +
            autoStartNote;
        lines.push(clusterLine);
        if (clusterInfo.clusterSource) {
            lines.push(`- source: ${clusterInfo.clusterSource}`);
        }
        if (clusterInfo.finalState) {
            lines.push(`- final state: ${clusterInfo.finalState}`);
        }
    }
    lines.push('');
    lines.push('You can now:');
    lines.push('- Ask me to fetch run details with `getDatabricksRunDetails` or recent runs with `getDatabricksRuns`.');
    lines.push('- Open the run in the Databricks UI if you prefer (use the run ID above).');
    lines.push('- Re-run the job later via Databricks UI using the job ID above.');
    return lines.join('\n');
}
function buildResultNotebookSource(userCode, options) {
    const rowLimit = normalizeRowLimit(options.maxRows);
    const lines = [];
    lines.push('import json');
    lines.push('import traceback');
    lines.push(`_MAX_ROWS = ${rowLimit}`);
    lines.push('result = None');
    lines.push('# --- User code start ---');
    lines.push(userCode);
    lines.push('# --- User code end ---');
    lines.push('');
    lines.push('def _default_serializer(obj):');
    lines.push('    try:');
    lines.push('        from pyspark.sql import DataFrame');
    lines.push('        if isinstance(obj, DataFrame):');
    lines.push('            try:');
    lines.push('                sample = obj.limit(_MAX_ROWS)');
    lines.push('                rows = [json.loads(r) for r in sample.toJSON().take(_MAX_ROWS)]');
    lines.push("                return {'type': 'pyspark.sql.DataFrame', 'rows': rows, 'rowLimit': _MAX_ROWS}");
    lines.push('            except Exception as e:');
    lines.push("                return {'type': 'pyspark.sql.DataFrame', 'error': f'sampling failed: {e}'}");
    lines.push('    except Exception:');
    lines.push('        pass');
    lines.push('    try:');
    lines.push('        import pandas as pd');
    lines.push('        if isinstance(obj, pd.DataFrame):');
    lines.push("            return {'type': 'pandas.DataFrame', 'rows': json.loads(pd.DataFrame(obj.head(_MAX_ROWS)).to_json(orient='records')), 'rowLimit': _MAX_ROWS}");
    lines.push('    except Exception:');
    lines.push('        pass');
    lines.push('    try:');
    lines.push('        if hasattr(obj, "__dict__"):');
    lines.push('            return obj.__dict__');
    lines.push('    except Exception:');
    lines.push('        pass');
    lines.push('    return str(obj)');
    lines.push('');
    lines.push('try:');
    lines.push('    payload = json.dumps(result, default=_default_serializer)');
    lines.push('except Exception as e:');
    lines.push("    dbutils.notebook.exit(json.dumps({'error': f'Failed to serialize result: {e}'}))");
    lines.push('else:');
    lines.push('    dbutils.notebook.exit(payload)');
    return lines.join('\n');
}
function formatRunResultMarkdown(args) {
    const lines = [];
    lines.push('## Databricks Run Result');
    lines.push('');
    if (args.jobId !== undefined) {
        lines.push(`Job ID: \`${args.jobId}\``);
    }
    else {
        lines.push('Job ID: one-off run (runs/submit)');
    }
    lines.push(`Run ID: \`${args.runId}\``);
    lines.push(`Run name: \`${args.jobName}\``);
    lines.push(`Workspace path: \`${args.path}\``);
    if (args.clusterInfo.clusterId) {
        const label = args.clusterInfo.mode === 'defaultCluster' ? 'default cluster' : args.clusterInfo.mode;
        const autoNote = args.clusterInfo.autoStarted ? ' (auto-started if needed)' : '';
        lines.push(`Cluster: ${label} \`${args.clusterInfo.clusterId}\`${autoNote}`);
    }
    if (args.clusterInfo.finalState) {
        lines.push(`Cluster final state: ${args.clusterInfo.finalState}`);
    }
    if (args.rowLimit) {
        lines.push(`Row sampling limit: ${args.rowLimit}`);
    }
    lines.push('');
    lines.push('**Result (JSON)**');
    lines.push('');
    if (args.resultJson !== undefined) {
        lines.push('```json');
        lines.push(JSON.stringify(args.resultJson, null, 2));
        lines.push('```');
    }
    else if (args.rawText) {
        lines.push('```');
        lines.push(args.rawText);
        lines.push('```');
        if (args.parseError) {
            lines.push('');
            lines.push(`(Could not parse JSON: ${args.parseError})`);
        }
    }
    else {
        lines.push('_No result returned from run output._');
    }
    if (args.truncated) {
        lines.push('');
        lines.push('_Result truncated due to maxResultBytes limit._');
    }
    lines.push('');
    lines.push('You can now:');
    lines.push('- Ask for run details or logs with the run ID above.');
    lines.push('- Re-run or extend this code by modifying the source and running again.');
    return lines.join('\n');
}
function formatSqlCommandResult(args) {
    const lines = [];
    lines.push('## SQL Result (All-purpose Cluster)');
    lines.push('');
    lines.push(`Cluster: \`${args.clusterName ?? args.clusterId}\``);
    if (args.result.contextId) {
        lines.push(`Context: \`${args.result.contextId}\``);
    }
    if (args.result.commandId) {
        lines.push(`Command: \`${args.result.commandId}\``);
    }
    lines.push('');
    lines.push('**SQL**');
    lines.push('```sql');
    lines.push(args.sql);
    lines.push('```');
    lines.push('');
    if (args.result.type === 'table') {
        const table = renderMarkdownTable(args.result.columns ?? [], args.result.rows ?? []);
        lines.push(table);
        if (args.result.truncated) {
            lines.push('');
            lines.push('_Rows truncated to maxRows limit._');
        }
    }
    else if (args.result.type === 'text') {
        lines.push('```');
        lines.push(args.result.text ?? '');
        lines.push('```');
    }
    else if (args.result.type === 'error') {
        lines.push('**Error**');
        lines.push('');
        lines.push(args.result.error ?? 'Command failed.');
    }
    else {
        lines.push('_No result returned._');
    }
    return lines.join('\n');
}
function formatPythonCommandResult(args) {
    const lines = [];
    lines.push('## Python Result (All-purpose Cluster)');
    lines.push('');
    lines.push(`Cluster: \`${args.clusterName ?? args.clusterId}\``);
    if (args.result.contextId) {
        lines.push(`Context: \`${args.result.contextId}\``);
    }
    if (args.result.commandId) {
        lines.push(`Command: \`${args.result.commandId}\``);
    }
    lines.push('');
    lines.push('**Code**');
    lines.push('```python');
    lines.push(args.code);
    lines.push('```');
    lines.push('');
    if (args.result.type === 'text') {
        lines.push('```');
        lines.push(args.result.text ?? '');
        lines.push('```');
    }
    else if (args.result.type === 'error') {
        lines.push('**Error**');
        lines.push('');
        lines.push(args.result.error ?? 'Command failed.');
    }
    else if (args.result.type === 'table') {
        const table = renderMarkdownTable(args.result.columns ?? [], args.result.rows ?? []);
        lines.push(table);
    }
    else {
        lines.push('_No result returned._');
    }
    return lines.join('\n');
}
function renderMarkdownTable(columns, rows) {
    if (!columns.length) {
        return '_No columns returned._';
    }
    const header = `| ${columns.join(' | ')} |`;
    const sep = `| ${columns.map(() => '---').join(' | ')} |`;
    const body = rows.map(r => `| ${r.map(v => formatTableValue(v)).join(' | ')} |`).join('\n');
    return [header, sep, body].filter(Boolean).join('\n');
}
function formatTableValue(value) {
    if (value === null || value === undefined) {
        return '';
    }
    if (typeof value === 'object') {
        try {
            return JSON.stringify(value);
        }
        catch {
            return String(value);
        }
    }
    return String(value);
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
function formatRunDetails(run, includeRawJson) {
    const lines = [];
    const start = run.start_time ? new Date(run.start_time).toLocaleString() : 'n/a';
    const end = run.end_time ? new Date(run.end_time).toLocaleString() : 'n/a';
    const durationSeconds = run.start_time && run.end_time ? ((run.end_time - run.start_time) / 1000).toFixed(1) : 'n/a';
    const state = run.state?.life_cycle_state ?? 'unknown';
    const result = run.state?.result_state ?? 'unknown';
    const msg = run.state?.state_message ?? '';
    lines.push(`# Databricks run ${run.run_id ?? 'n/a'}`);
    lines.push(`- job_id: ${run.job_id ?? 'n/a'}`);
    lines.push(`- number_in_job: ${run.number_in_job ?? 'n/a'}`);
    lines.push(`- state: ${state}`);
    lines.push(`- result: ${result}`);
    if (msg) {
        lines.push(`- message: ${msg}`);
    }
    lines.push(`- start: ${start}`);
    lines.push(`- end: ${end}`);
    lines.push(`- duration_s: ${durationSeconds}`);
    lines.push(`- cluster_id: ${run.cluster_instance?.cluster_id ?? 'n/a'}`);
    const clusterDesc = run.cluster_spec?.new_cluster
        ? `${run.cluster_spec.new_cluster.node_type_id ?? 'node'} / spark ${run.cluster_spec.new_cluster.spark_version ?? 'n/a'}`
        : 'n/a';
    lines.push(`- cluster_spec: ${clusterDesc}`);
    if (run.tasks && run.tasks.length) {
        lines.push('');
        lines.push('## Tasks');
        lines.push('| task | notebook |');
        lines.push('| --- | --- |');
        for (const t of run.tasks) {
            lines.push(`| ${t.task_key ?? 'n/a'} | ${t.notebook_task?.notebook_path ?? 'n/a'} |`);
        }
    }
    if (includeRawJson) {
        lines.push('');
        lines.push('```json');
        lines.push(JSON.stringify(run, null, 2));
        lines.push('```');
    }
    return lines.join('\n');
}
const PERFORMANCE_SPARK_CONF_KEYS = [
    'spark.databricks.delta.optimizeWrite',
    'spark.databricks.delta.autoCompact',
    'spark.sql.shuffle.partitions',
    'spark.sql.adaptive.enabled',
    'spark.sql.adaptive.shuffle.targetPostShuffleInputSize',
    'spark.executor.memory',
    'spark.executor.memoryOverhead',
    'spark.executor.cores',
    'spark.driver.memory',
    'spark.driver.cores',
    'spark.databricks.io.cache.enabled',
];
function redactMap(input) {
    const values = {};
    const redactedKeys = [];
    const shownKeys = [];
    if (!input) {
        return { values, redactedKeys, shownKeys };
    }
    for (const [k, v] of Object.entries(input)) {
        const lower = k.toLowerCase();
        const strVal = typeof v === 'string' ? v : v === undefined || v === null ? '' : String(v);
        const looksSecret = /token|secret|password|key|credential|bearer/i.test(lower) || /token|secret|password|bearer/i.test(strVal);
        if (looksSecret || strVal.length > 80) {
            values[k] = '***redacted***';
            redactedKeys.push(k);
        }
        else {
            values[k] = strVal;
            shownKeys.push(k);
        }
    }
    return { values, redactedKeys, shownKeys };
}
function formatSelectedMap(title, entries, keys) {
    const lines = [];
    const present = keys.filter(k => entries[k] !== undefined);
    if (present.length === 0) {
        lines.push(`${title}: none found`);
        return lines;
    }
    lines.push(title + ':');
    for (const key of present) {
        lines.push(`- \`${key}\`: ${entries[key]}`);
    }
    return lines;
}
function summarizeAutoscale(cluster) {
    if (cluster.autoscale) {
        const min = cluster.autoscale.min_workers ?? '?';
        const max = cluster.autoscale.max_workers ?? '?';
        return `Autoscaling: enabled, min workers = ${min}, max workers = ${max}`;
    }
    if (cluster.num_workers != null) {
        return `Workers: ${cluster.num_workers}`;
    }
    return 'Workers: n/a';
}
function formatClusterDefinitionMarkdown(cluster, options) {
    const lines = [];
    const autoTermination = cluster.autotermination_minutes != null ? `${cluster.autotermination_minutes} minutes` : 'not configured';
    lines.push('## Cluster Definition');
    lines.push('');
    lines.push(`Cluster ID: \`${cluster.cluster_id ?? 'n/a'}\`  `);
    lines.push(`Name: \`${cluster.cluster_name ?? 'n/a'}\`  `);
    lines.push(`State: \`${cluster.state ?? 'n/a'}\`  `);
    lines.push(`Source: \`${cluster.cluster_source ?? 'unknown'}\`  ${options.usedDefaultCluster ? '(default cluster)' : ''}`);
    lines.push(`Resolved from: ${options.clusterIdSource}`);
    lines.push('');
    lines.push('### Runtime & Node Types');
    lines.push(`- Runtime: \`${cluster.spark_version ?? 'n/a'}\``);
    lines.push(`- Driver node type: \`${cluster.driver_node_type_id ?? cluster.node_type_id ?? 'n/a'}\``);
    lines.push(`- Worker node type: \`${cluster.node_type_id ?? cluster.driver_node_type_id ?? 'n/a'}\``);
    lines.push(`- ${summarizeAutoscale(cluster)}`);
    if (cluster.instance_pool_id) {
        lines.push(`- Instance pool: \`${cluster.instance_pool_id}\``);
    }
    lines.push('');
    lines.push('### Auto-Termination');
    lines.push(`- Auto-termination: ${autoTermination}`);
    lines.push('');
    const sparkLines = formatSelectedMap('### Spark Config (selected)', options.redactedSparkConf.values, PERFORMANCE_SPARK_CONF_KEYS);
    lines.push(...sparkLines);
    const redactedSparkCount = options.redactedSparkConf.redactedKeys.length;
    const otherSparkKeys = Object.keys(options.redactedSparkConf.values).filter(k => !PERFORMANCE_SPARK_CONF_KEYS.includes(k));
    if (otherSparkKeys.length || redactedSparkCount) {
        lines.push(`- Other spark_conf keys present${redactedSparkCount ? ` (${redactedSparkCount} redacted)` : ''}.`);
    }
    lines.push('');
    const envLines = formatSelectedMap('### Environment Variables (selected, non-secret)', options.redactedEnv.values, Object.keys(options.redactedEnv.values));
    lines.push(...envLines);
    if (options.redactedEnv.redactedKeys.length) {
        lines.push(`- ${options.redactedEnv.redactedKeys.length} env var(s) redacted.`);
    }
    if (options.includeRawJson) {
        lines.push('');
        lines.push('<details>');
        lines.push('<summary>Raw cluster JSON (redacted)</summary>');
        lines.push('');
        lines.push('```json');
        lines.push(JSON.stringify(cluster, null, 2));
        lines.push('```');
        lines.push('</details>');
    }
    return lines.join('\n');
}
async function getClusterDefinitionMarkdown(context, input, token) {
    const includeRawJson = input.includeRawJson ?? false;
    const requestedCluster = (input.clusterId ?? '').trim();
    const defaultCluster = await (0, databricksClient_1.getDefaultCluster)(context);
    const clusterId = requestedCluster || defaultCluster.id;
    if (!clusterId) {
        return 'No cluster selected. Set a default cluster via **Databricks Tools: Select Default Cluster** and try again.';
    }
    const clusterIdSource = requestedCluster ? 'Explicit clusterId input' : 'databricksTools.defaultClusterId';
    const client = await databricksClient_1.DatabricksClient.fromConfig(context);
    const cluster = await client.getClusterDefinition(clusterId, token);
    const redactedSparkConf = redactMap(cluster.spark_conf);
    const redactedEnv = redactMap(cluster.environment_vars);
    const redactedCluster = {
        ...cluster,
        spark_conf: redactedSparkConf.values,
        environment_vars: redactedEnv.values,
    };
    const markdown = formatClusterDefinitionMarkdown(redactedCluster, {
        includeRawJson,
        redactedSparkConf,
        redactedEnv,
        usedDefaultCluster: !requestedCluster && !!defaultCluster.id,
        defaultClusterName: defaultCluster.name,
        clusterIdSource,
    });
    return markdown;
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
function normalizeRowLimit(value) {
    const fallback = 20;
    if (value === undefined || value === null) {
        return fallback;
    }
    if (!Number.isFinite(value)) {
        return fallback;
    }
    const n = Math.max(1, Math.floor(value));
    return Math.min(n, 1000);
}
const ARTIFACT_MAPPING_FILE = 'databricks-mapping.json';
async function resolveArtifactMapping(context, input) {
    const workspaceRoot = vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
    if (!workspaceRoot) {
        throw new Error('Open a workspace to resolve artifact mappings.');
    }
    if (input.jobId == null && !input.workspacePath) {
        throw new Error('Provide jobId or workspacePath to resolve.');
    }
    const { store, mappingPath, exists } = await readArtifactMappingStore(workspaceRoot);
    const lines = [];
    lines.push('# Artifact mapping');
    lines.push(`- mapping file: \.vscode\\${ARTIFACT_MAPPING_FILE}${exists ? '' : ' (will be created after adding mappings)'}`);
    if (input.jobId != null) {
        const mapped = store.jobs?.[String(input.jobId)];
        if (mapped) {
            lines.push('');
            lines.push(`Job ${input.jobId} → \\${mapped}`);
        }
        else {
            lines.push('');
            lines.push(`No mapping found for job ${input.jobId}. Use **Databricks Tools: Add Artifact Mapping** to add one.`);
        }
    }
    if (input.workspacePath) {
        const mapped = store.workspaces?.[input.workspacePath];
        if (mapped) {
            lines.push('');
            lines.push(`Workspace path ${input.workspacePath} → \\${mapped}`);
        }
        else {
            lines.push('');
            lines.push(`No mapping found for workspace path ${input.workspacePath}. Use **Databricks Tools: Add Artifact Mapping** to add one.`);
        }
    }
    lines.push('');
    lines.push(`Mapping file location: \\${mappingPath}`);
    return lines.join('\n');
}
async function addArtifactMapping() {
    const workspaceFolder = vscode.workspace.workspaceFolders?.[0];
    if (!workspaceFolder) {
        void vscode.window.showErrorMessage('Open a workspace folder before adding an artifact mapping.');
        return;
    }
    const typePick = await vscode.window.showQuickPick([
        { label: 'Job ID → repo file', value: 'job' },
        { label: 'Workspace path → repo file', value: 'workspace' },
    ], { title: 'Artifact type', ignoreFocusOut: true });
    if (!typePick) {
        return;
    }
    let jobId;
    let workspacePathInput;
    if (typePick.value === 'job') {
        const jobInput = await vscode.window.showInputBox({
            title: 'Job ID',
            prompt: 'Enter Databricks job ID',
            ignoreFocusOut: true,
            validateInput: value => {
                if (!value.trim())
                    return 'Job ID is required';
                return /^\d+$/.test(value.trim()) ? null : 'Enter a numeric job ID';
            },
        });
        if (!jobInput)
            return;
        jobId = Number(jobInput.trim());
    }
    else {
        const wsInput = await vscode.window.showInputBox({
            title: 'Workspace notebook path',
            prompt: 'Enter workspace path, e.g. /Workspace/Users/...',
            ignoreFocusOut: true,
            validateInput: value => (value.trim() ? null : 'Workspace path is required'),
        });
        if (!wsInput)
            return;
        workspacePathInput = wsInput.trim();
    }
    const defaultPath = vscode.window.activeTextEditor
        ? vscode.workspace.asRelativePath(vscode.window.activeTextEditor.document.uri, false)
        : undefined;
    const repoPath = await vscode.window.showInputBox({
        title: 'Repository file path',
        prompt: 'Enter repo-relative file path for the artifact',
        value: defaultPath,
        ignoreFocusOut: true,
        validateInput: value => (value.trim() ? null : 'Repo path is required'),
    });
    if (!repoPath) {
        return;
    }
    const { store, mappingPath } = await readArtifactMappingStore(workspaceFolder.uri.fsPath);
    const normalizedRepoPath = repoPath.trim();
    if (jobId != null) {
        store.jobs = store.jobs ?? {};
        store.jobs[String(jobId)] = normalizedRepoPath;
    }
    else if (workspacePathInput) {
        store.workspaces = store.workspaces ?? {};
        store.workspaces[workspacePathInput] = normalizedRepoPath;
    }
    await writeArtifactMappingStore(mappingPath, store);
    void vscode.window.showInformationMessage('Artifact mapping saved.');
}
async function readArtifactMappingStore(workspaceRoot) {
    const mappingPath = path.join(workspaceRoot, '.vscode', ARTIFACT_MAPPING_FILE);
    try {
        const content = await fs_1.promises.readFile(mappingPath, 'utf8');
        const parsed = content ? JSON.parse(content) : {};
        return { store: parsed, mappingPath, exists: true };
    }
    catch (err) {
        if (err?.code === 'ENOENT') {
            return { store: {}, mappingPath, exists: false };
        }
        throw err;
    }
}
async function writeArtifactMappingStore(mappingPath, store) {
    await fs_1.promises.mkdir(path.dirname(mappingPath), { recursive: true });
    await fs_1.promises.writeFile(mappingPath, JSON.stringify(store, null, 2), 'utf8');
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
    lines.push('| cluster_id | name | state | source | size | spark_version | node_type | auto-termination |');
    lines.push('| --- | --- | --- | --- | --- | --- | --- | --- |');
    const display = clusters.slice(0, 50);
    for (const c of display) {
        const size = c.autoscale
            ? `auto ${c.autoscale.min_workers ?? '?'}-${c.autoscale.max_workers ?? '?'}`
            : `${c.num_workers ?? '?'} workers`;
        const node = c.node_type_id || c.driver_node_type_id || 'n/a';
        const auto = c.autotermination_minutes != null ? `${c.autotermination_minutes}m` : 'n/a';
        const source = c.cluster_source ?? 'n/a';
        lines.push(`| ${c.cluster_id ?? 'n/a'} | ${c.cluster_name ?? 'n/a'} | ${c.state ?? 'n/a'} | ${source} | ${size} | ${c.spark_version ?? 'n/a'} | ${node} | ${auto} |`);
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