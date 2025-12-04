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
            const markdown = this.formatRuns(input.jobId, runs, limit, includeRawJson);
            return new vscode.LanguageModelToolResult([new vscode.LanguageModelTextPart(markdown)]);
        }
        catch (err) {
            if (err instanceof databricksClient_1.ConfigCancelled) {
                return new vscode.LanguageModelToolResult([
                    new vscode.LanguageModelTextPart(`Configuration cancelled: ${err.message}`),
                ]);
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
function activate(context) {
    const tool = new DatabricksGetRunsTool(context);
    context.subscriptions.push(vscode.lm.registerTool('databricks_getRuns', tool));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.configure', async () => {
        try {
            await (0, databricksClient_1.configureConnection)(context);
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            void vscode.window.showErrorMessage(`Databricks configuration failed: ${message}`);
        }
    }));
    context.subscriptions.push(vscode.commands.registerCommand('databricksTools.clearCredentials', async () => {
        try {
            await (0, databricksClient_1.clearStoredCredentials)(context);
        }
        catch (err) {
            const message = err instanceof Error ? err.message : String(err);
            void vscode.window.showErrorMessage(`Failed to clear Databricks credentials: ${message}`);
        }
    }));
}
function deactivate() { }
//# sourceMappingURL=extension.js.map