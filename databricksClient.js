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
exports.ApiError = exports.DatabricksClient = exports.ConfigCancelled = void 0;
exports.ensureDatabricksConfig = ensureDatabricksConfig;
exports.configureConnection = configureConnection;
exports.getDefaultCluster = getDefaultCluster;
exports.setDefaultCluster = setDefaultCluster;
exports.clearDefaultCluster = clearDefaultCluster;
exports.clearStoredCredentials = clearStoredCredentials;
exports.setAuthMode = setAuthMode;
exports.getAuthStatus = getAuthStatus;
exports.exportWorkspaceSource = exportWorkspaceSource;
exports.importWorkspaceSource = importWorkspaceSource;
exports.createJobFromNotebook = createJobFromNotebook;
exports.runJobNow = runJobNow;
exports.ensureClusterRunning = ensureClusterRunning;
exports.getRunDetails = getRunDetails;
exports.getOutputChannel = getOutputChannel;
const vscode = __importStar(require("vscode"));
const util_1 = require("util");
const child_process_1 = require("child_process");
const exec = (0, util_1.promisify)(child_process_1.exec);
class ConfigCancelled extends Error {
    constructor(message) {
        super(message);
        this.name = 'ConfigCancelled';
    }
}
exports.ConfigCancelled = ConfigCancelled;
class DatabricksClient {
    host;
    token;
    constructor(host, token) {
        this.host = host;
        this.token = token;
    }
    static async fromConfig(context) {
        const { host, token } = await ensureDatabricksConfig(context);
        return new DatabricksClient(host, token);
    }
    async listRuns(jobId, limit, token) {
        const version = vscode.workspace.getConfiguration('databricksTools').get('jobsApiVersion', 'auto');
        const { data } = await this.callJobsRunsList({ jobId, limit }, version, token);
        return data.runs ?? [];
    }
    async testConnection(signal) {
        const url = `${this.host}/api/2.0/clusters/list?limit=1`;
        const controller = new AbortController();
        signal.onCancellationRequested(() => controller.abort());
        const res = await fetch(url, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${this.token}`,
            },
            signal: controller.signal,
        });
        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Databricks connection test failed ${res.status}: ${text || res.statusText}`);
        }
    }
    async testJobsApi(version, signal) {
        const { usedVersion } = await this.callJobsRunsList({ jobId: undefined, limit: 1 }, version, signal);
        return { usedVersion };
    }
    async getJobDefinition(jobId, version, token) {
        return this.callJobsGet(jobId, version, token);
    }
    async listClusters(token, options) {
        const controller = new AbortController();
        token.onCancellationRequested(() => controller.abort());
        const url = `${this.host}/api/2.0/clusters/list`;
        const res = await fetch(url, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${this.token}`,
            },
            signal: controller.signal,
        });
        const text = await res.text();
        if (!res.ok) {
            throw new ApiError(`Clusters list failed ${res.status}: ${text || res.statusText}`, res.status, '2.0', text || res.statusText);
        }
        const parsed = text ? JSON.parse(text) : { clusters: [] };
        const clusters = parsed.clusters ?? [];
        if (options?.includeInteractiveClusters === false) {
            return clusters.filter(c => (c.cluster_source ?? '').toUpperCase() !== 'UI');
        }
        return clusters;
    }
    async getCluster(clusterId) {
        const searchParams = new URLSearchParams();
        searchParams.set('cluster_id', clusterId);
        const url = `${this.host}/api/2.0/clusters/get?${searchParams.toString()}`;
        const res = await fetch(url, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${this.token}`,
            },
        });
        const text = await res.text();
        if (!res.ok) {
            let code;
            let message = text || res.statusText;
            try {
                const parsed = text ? JSON.parse(text) : {};
                code = parsed.error_code;
                if (parsed.message) {
                    message = parsed.message;
                }
            }
            catch {
                // ignore
            }
            throw new ApiError(`clusters/get failed ${res.status}: ${message}`, res.status, '2.0', text || message, code);
        }
        try {
            return text ? JSON.parse(text) : {};
        }
        catch {
            throw new Error('clusters/get returned invalid JSON.');
        }
    }
    async startCluster(clusterId, token) {
        const controller = new AbortController();
        token.onCancellationRequested(() => controller.abort());
        const url = `${this.host}/api/2.0/clusters/start`;
        const res = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${this.token}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ cluster_id: clusterId }),
            signal: controller.signal,
        });
        const text = await res.text();
        if (!res.ok) {
            throw new ApiError(`Start cluster failed ${res.status}: ${text || res.statusText}`, res.status, '2.0', text || res.statusText);
        }
    }
    async ensureClusterRunning(clusterId, options) {
        return ensureClusterRunning(this.host, this.token, clusterId, options);
    }
    async exportWorkspaceSource(path) {
        return exportWorkspaceSource(this.host, this.token, path);
    }
    async importWorkspaceSource(path, language, sourceCode) {
        return importWorkspaceSource(this.host, this.token, path, language, sourceCode);
    }
    async createJobFromNotebook(jobName, notebookPath, clusterMode, options) {
        return createJobFromNotebook(this.host, this.token, jobName, notebookPath, clusterMode, options);
    }
    async runJobNow(jobId) {
        return runJobNow(this.host, this.token, jobId);
    }
    async getRunDetails(runId) {
        return getRunDetails(this.host, this.token, runId);
    }
    async callJobsRunsList(params, version, token) {
        const controller = new AbortController();
        token.onCancellationRequested(() => controller.abort());
        const searchParams = new URLSearchParams();
        searchParams.set('limit', params.limit.toString());
        if (params.jobId !== undefined) {
            searchParams.set('job_id', params.jobId.toString());
        }
        const attempt = async (ver) => {
            const url = `${this.host}/api/${ver}/jobs/runs/list?${searchParams.toString()}`;
            const res = await fetch(url, {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${this.token}`,
                },
                signal: controller.signal,
            });
            const text = await res.text();
            if (!res.ok) {
                const err = new ApiError(`Jobs API ${ver} error ${res.status}: ${text || res.statusText}`, res.status, ver, text || res.statusText);
                throw err;
            }
            const data = text ? JSON.parse(text) : { runs: [] };
            return { data, usedVersion: ver };
        };
        const output = getOutputChannel();
        if (version === '2.0') {
            return await attempt('2.0');
        }
        if (version === '2.1') {
            return await attempt('2.1');
        }
        // auto: try 2.1 then fallback on endpoint missing
        try {
            return await attempt('2.1');
        }
        catch (err) {
            if (err instanceof ApiError && isEndpointMissing(err)) {
                if (isWrongMethodError(err)) {
                    output.appendLine("Jobs API call used POST but /jobs/runs/list only supports GET. This should not happen; please verify the extension code.");
                }
                output.appendLine('Jobs API 2.1 not available, falling back to 2.0.');
                try {
                    return await attempt('2.0');
                }
                catch (err2) {
                    if (err2 instanceof ApiError) {
                        throw new ApiError(`Jobs API 2.1 not available; 2.0 failed with ${err2.status}: ${err2.body ?? err2.message}`, err2.status, '2.0', err2.body ?? err2.message);
                    }
                    throw err2;
                }
            }
            throw err;
        }
    }
    async callJobsGet(jobId, version, token) {
        const controller = new AbortController();
        token.onCancellationRequested(() => controller.abort());
        const searchParams = new URLSearchParams();
        searchParams.set('job_id', jobId.toString());
        const attempt = async (ver) => {
            const url = `${this.host}/api/${ver}/jobs/get?${searchParams.toString()}`;
            const res = await fetch(url, {
                method: 'GET',
                headers: {
                    'Authorization': `Bearer ${this.token}`,
                },
                signal: controller.signal,
            });
            const text = await res.text();
            if (!res.ok) {
                const err = new ApiError(`Jobs API ${ver} get error ${res.status}: ${text || res.statusText}`, res.status, ver, text || res.statusText);
                throw err;
            }
            const data = text ? JSON.parse(text) : {};
            return { data, usedVersion: ver };
        };
        const output = getOutputChannel();
        if (version === '2.0') {
            return await attempt('2.0');
        }
        if (version === '2.1') {
            return await attempt('2.1');
        }
        try {
            return await attempt('2.1');
        }
        catch (err) {
            if (err instanceof ApiError && isEndpointMissing(err)) {
                output.appendLine('Jobs API get (2.1) not available, falling back to 2.0.');
                try {
                    return await attempt('2.0');
                }
                catch (err2) {
                    if (err2 instanceof ApiError) {
                        throw new ApiError(`Jobs API get 2.1 not available; 2.0 failed with ${err2.status}: ${err2.body ?? err2.message}`, err2.status, '2.0', err2.body ?? err2.message);
                    }
                    throw err2;
                }
            }
            throw err;
        }
    }
}
exports.DatabricksClient = DatabricksClient;
async function isAzureCliAvailable() {
    try {
        await exec('az --version');
        return true;
    }
    catch {
        return false;
    }
}
async function isAzureCliLoggedIn() {
    try {
        const { stdout } = await exec('az account show');
        return !!stdout;
    }
    catch {
        return false;
    }
}
async function getAzureCliToken(resource) {
    const available = await isAzureCliAvailable();
    if (!available) {
        throw new Error('Azure CLI authentication failed: Azure CLI is not installed or not on PATH. Install Azure CLI or switch auth mode to PAT.');
    }
    try {
        const { stdout } = await exec(`az account get-access-token --resource ${resource} --query accessToken -o tsv`);
        const token = stdout.trim();
        if (!token) {
            throw new Error('Azure CLI authentication failed: token is empty. Run az login or check your subscription.');
        }
        return token;
    }
    catch (err) {
        throw new Error('Azure CLI authentication failed: unable to acquire token. Ensure you are logged in with az login and have access to Databricks.');
    }
}
async function ensureDatabricksConfig(context, options) {
    const envHost = (process.env.DATABRICKS_HOST || '').trim();
    const envToken = (process.env.DATABRICKS_TOKEN || '').trim();
    const config = vscode.workspace.getConfiguration('databricksTools');
    const authMode = config.get('authMode', 'auto');
    const azureResource = config.get('azureCliResource', 'https://databricks.azure.net');
    const storedHost = (config.get('host') || '').trim();
    const storedToken = await context.secrets.get('databricksTools.token');
    // Host resolution
    let host = envHost || storedHost;
    if (!host || options?.forcePrompt) {
        const promptedHost = await promptForHost(host);
        if (promptedHost === undefined) {
            throw new ConfigCancelled('The Databricks host is missing. Ask the user to configure it by running "Databricks Tools: Configure Connection" or provide it now.');
        }
        host = promptedHost;
        await saveHostSetting(promptedHost);
    }
    if (!host) {
        throw new Error('Databricks host is missing. Set DATABRICKS_HOST or configure the extension.');
    }
    // Token resolution based on mode
    const patResolver = async (allowPrompt) => {
        if (envToken) {
            return { host: host.replace(/\/$/, ''), token: envToken, authType: 'pat', source: 'env' };
        }
        if (storedToken) {
            return { host: host.replace(/\/$/, ''), token: storedToken, authType: 'pat', source: 'secret' };
        }
        if (allowPrompt) {
            const promptedToken = await promptForToken();
            if (promptedToken === undefined) {
                throw new ConfigCancelled('The Databricks token is missing. Ask the user to provide a PAT or rerun configuration (cancelled).');
            }
            await context.secrets.store('databricksTools.token', promptedToken);
            return { host: host.replace(/\/$/, ''), token: promptedToken, authType: 'pat', source: 'prompted' };
        }
        return null;
    };
    const azureResolver = async () => {
        const token = await getAzureCliToken(azureResource);
        return { host: host.replace(/\/$/, ''), token, authType: 'azureCli', source: 'azureCli' };
    };
    if (authMode === 'pat') {
        const pat = await patResolver(true);
        if (!pat) {
            throw new Error('PAT authentication required but no token available. Set DATABRICKS_TOKEN or provide a PAT.');
        }
        return pat;
    }
    if (authMode === 'azureCli') {
        return await azureResolver();
    }
    // auto mode
    const patAuto = await patResolver(false);
    if (patAuto) {
        return patAuto;
    }
    try {
        return await azureResolver();
    }
    catch (err) {
        // fallback to PAT prompt
        const pat = await patResolver(true);
        if (pat) {
            return pat;
        }
        throw err;
    }
}
async function configureConnection(context) {
    await ensureDatabricksConfig(context, { forcePrompt: true });
    void vscode.window.showInformationMessage('Databricks connection updated for future tool runs.');
}
async function getDefaultCluster(context) {
    const config = vscode.workspace.getConfiguration('databricksTools');
    const id = (config.get('defaultClusterId') || '').trim();
    const name = (config.get('defaultClusterName') || '').trim();
    return { id: id || undefined, name: name || undefined };
}
async function setDefaultCluster(context, id, name) {
    const config = vscode.workspace.getConfiguration('databricksTools');
    await config.update('defaultClusterId', id, vscode.ConfigurationTarget.Global);
    await config.update('defaultClusterName', name, vscode.ConfigurationTarget.Global);
}
async function clearDefaultCluster(context) {
    const config = vscode.workspace.getConfiguration('databricksTools');
    await config.update('defaultClusterId', '', vscode.ConfigurationTarget.Global);
    await config.update('defaultClusterName', '', vscode.ConfigurationTarget.Global);
    void vscode.window.showInformationMessage('Default Databricks cluster cleared.');
}
async function clearStoredCredentials(context) {
    const choice = await vscode.window.showQuickPick([
        { label: 'Clear host and token', value: 'all' },
        { label: 'Clear token only', value: 'token' },
        { label: 'Cancel', value: 'cancel' },
    ], { placeHolder: 'Clear stored Databricks credentials', ignoreFocusOut: true });
    if (!choice || choice.value === 'cancel') {
        return;
    }
    if (choice.value === 'all') {
        await saveHostSetting('');
    }
    await context.secrets.delete('databricksTools.token');
    void vscode.window.showInformationMessage('Databricks stored credentials cleared.');
}
async function promptForHost(existing) {
    const input = await vscode.window.showInputBox({
        title: 'Databricks Workspace URL',
        prompt: 'Enter your Databricks workspace URL (e.g. https://adb-1234567890123456.1.azuredatabricks.net)',
        value: existing ?? '',
        ignoreFocusOut: true,
        validateInput: value => {
            const trimmed = value.trim();
            if (!trimmed) {
                return 'Workspace URL is required.';
            }
            if (!/^https?:\/\//i.test(trimmed)) {
                return 'Workspace URL should start with https://';
            }
            return null;
        },
    });
    return input?.trim();
}
async function promptForToken() {
    const input = await vscode.window.showInputBox({
        title: 'Databricks Personal Access Token',
        prompt: 'Enter your Databricks personal access token',
        password: true,
        ignoreFocusOut: true,
        validateInput: value => {
            if (!value.trim()) {
                return 'Token is required.';
            }
            return null;
        },
    });
    return input?.trim();
}
async function saveHostSetting(host) {
    const config = vscode.workspace.getConfiguration('databricksTools');
    await config.update('host', host, vscode.ConfigurationTarget.Global);
}
async function setAuthMode(mode) {
    const config = vscode.workspace.getConfiguration('databricksTools');
    await config.update('authMode', mode, vscode.ConfigurationTarget.Global);
}
async function getAuthStatus(context) {
    const envHost = (process.env.DATABRICKS_HOST || '').trim();
    const envToken = (process.env.DATABRICKS_TOKEN || '').trim();
    const config = vscode.workspace.getConfiguration('databricksTools');
    const authMode = config.get('authMode', 'auto');
    const storedHost = (config.get('host') || '').trim();
    const storedToken = await context.secrets.get('databricksTools.token');
    const azureCliAvailable = await isAzureCliAvailable();
    const azureCliLoggedIn = azureCliAvailable ? await isAzureCliLoggedIn() : false;
    return {
        hostConfigured: !!(envHost || storedHost),
        hostSource: envHost ? 'env' : storedHost ? 'settings' : null,
        authMode,
        patAvailable: !!(envToken || storedToken),
        patSource: envToken ? 'env' : storedToken ? 'secret' : null,
        azureCliAvailable,
        azureCliLoggedIn,
    };
}
class ApiError extends Error {
    status;
    version;
    body;
    errorCode;
    constructor(message, status, version, body, errorCode) {
        super(message);
        this.status = status;
        this.version = version;
        this.body = body;
        this.errorCode = errorCode;
        this.name = 'ApiError';
    }
}
exports.ApiError = ApiError;
async function exportWorkspaceSource(host, token, path) {
    const output = getOutputChannel();
    const searchParams = new URLSearchParams();
    searchParams.set('path', path);
    searchParams.set('format', 'SOURCE');
    searchParams.set('direct_download', 'false');
    const url = `${host}/api/2.0/workspace/export?${searchParams.toString()}`;
    const res = await fetch(url, {
        method: 'GET',
        headers: {
            'Authorization': `Bearer ${token}`,
        },
    });
    const text = await res.text();
    if (!res.ok) {
        let errCode;
        let message = text || res.statusText;
        try {
            const parsed = text ? JSON.parse(text) : {};
            if (parsed.error_code) {
                errCode = parsed.error_code;
            }
            if (parsed.message) {
                message = parsed.message;
            }
        }
        catch {
            // ignore JSON parse failures and rely on status/text
        }
        const suffix = errCode ? ` ${errCode}` : '';
        const bodyForLog = text || message || res.statusText;
        output.appendLine(`Workspace export failed for path ${path}: HTTP ${res.status}${suffix} ${message}`);
        throw new ApiError(`HTTP ${res.status}${suffix}: ${message || res.statusText}`.trim(), res.status, '2.0', bodyForLog, errCode);
    }
    if (!text) {
        throw new Error('Workspace export returned an empty response.');
    }
    let parsed;
    try {
        parsed = JSON.parse(text);
    }
    catch {
        throw new Error('Workspace export returned invalid JSON.');
    }
    if (!parsed.content) {
        throw new Error('Workspace export response is missing the content field.');
    }
    let decoded;
    try {
        decoded = Buffer.from(parsed.content, 'base64').toString('utf8');
    }
    catch {
        throw new Error('Failed to decode workspace export content from base64.');
    }
    return { language: parsed.language, source: decoded };
}
async function importWorkspaceSource(host, token, path, language, sourceCode) {
    const output = getOutputChannel();
    const url = `${host}/api/2.0/workspace/import`;
    const payload = {
        path,
        format: 'SOURCE',
        language,
        content: Buffer.from(sourceCode, 'utf8').toString('base64'),
        overwrite: true,
    };
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
    });
    const text = await res.text();
    if (!res.ok) {
        let errCode;
        let message = text || res.statusText;
        try {
            const parsed = text ? JSON.parse(text) : {};
            errCode = parsed.error_code;
            if (parsed.message) {
                message = parsed.message;
            }
        }
        catch {
            // ignore
        }
        const suffix = errCode ? ` ${errCode}` : '';
        output.appendLine(`Workspace import failed for path ${path}: HTTP ${res.status}${suffix} ${message}`);
        throw new ApiError(`HTTP ${res.status}${suffix}: ${message || res.statusText}`.trim(), res.status, '2.0', text || message, errCode);
    }
}
async function createJobFromNotebook(host, token, jobName, notebookPath, clusterMode, options) {
    const output = getOutputChannel();
    const url = `${host}/api/2.1/jobs/create`;
    const task = {
        task_key: 'main',
        notebook_task: { notebook_path: notebookPath },
    };
    if (clusterMode === 'existingCluster') {
        if (!options.existingClusterId) {
            throw new Error('existingClusterId is required when clusterMode is existingCluster.');
        }
        task.existing_cluster_id = options.existingClusterId;
    }
    else {
        const cfg = options.newClusterConfig;
        if (!cfg || !cfg.sparkVersion || !cfg.nodeTypeId) {
            throw new Error('newClusterConfig.sparkVersion and nodeTypeId are required when clusterMode is newJobCluster.');
        }
        task.new_cluster = {
            spark_version: cfg.sparkVersion,
            node_type_id: cfg.nodeTypeId,
            num_workers: cfg.numWorkers ?? 1,
            autotermination_minutes: cfg.autoTerminationMinutes ?? 60,
        };
    }
    const payload = {
        name: jobName,
        tasks: [task],
    };
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
    });
    const text = await res.text();
    if (!res.ok) {
        let errCode;
        let message = text || res.statusText;
        try {
            const parsed = text ? JSON.parse(text) : {};
            errCode = parsed.error_code;
            if (parsed.message) {
                message = parsed.message;
            }
        }
        catch {
            // ignore
        }
        const suffix = errCode ? ` ${errCode}` : '';
        output.appendLine(`jobs/create failed: HTTP ${res.status}${suffix} ${message}`);
        throw new ApiError(`HTTP ${res.status}${suffix}: ${message || res.statusText}`.trim(), res.status, '2.1', text || message, errCode);
    }
    let parsed = {};
    try {
        parsed = text ? JSON.parse(text) : {};
    }
    catch {
        throw new Error('jobs/create returned invalid JSON.');
    }
    if (!parsed.job_id) {
        throw new Error('jobs/create did not return job_id.');
    }
    return { jobId: parsed.job_id };
}
async function runJobNow(host, token, jobId) {
    const output = getOutputChannel();
    const url = `${host}/api/2.1/jobs/run-now`;
    const payload = { job_id: jobId };
    const res = await fetch(url, {
        method: 'POST',
        headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(payload),
    });
    const text = await res.text();
    if (!res.ok) {
        let errCode;
        let message = text || res.statusText;
        try {
            const parsed = text ? JSON.parse(text) : {};
            errCode = parsed.error_code;
            if (parsed.message) {
                message = parsed.message;
            }
        }
        catch {
            // ignore
        }
        const suffix = errCode ? ` ${errCode}` : '';
        output.appendLine(`jobs/run-now failed: HTTP ${res.status}${suffix} ${message}`);
        throw new ApiError(`HTTP ${res.status}${suffix}: ${message || res.statusText}`.trim(), res.status, '2.1', text || message, errCode);
    }
    let parsed = {};
    try {
        parsed = text ? JSON.parse(text) : {};
    }
    catch {
        throw new Error('jobs/run-now returned invalid JSON.');
    }
    if (!parsed.run_id) {
        throw new Error('jobs/run-now did not return run_id.');
    }
    return { runId: parsed.run_id };
}
async function ensureClusterRunning(host, token, clusterId, options) {
    const poll = options?.poll ?? true;
    const timeoutMs = options?.timeoutMs ?? 10 * 60 * 1000;
    const pollIntervalMs = options?.pollIntervalMs ?? 10 * 1000;
    const start = Date.now();
    const output = getOutputChannel();
    const getState = async () => {
        const searchParams = new URLSearchParams();
        searchParams.set('cluster_id', clusterId);
        const url = `${host}/api/2.0/clusters/get?${searchParams.toString()}`;
        const res = await fetch(url, {
            method: 'GET',
            headers: {
                'Authorization': `Bearer ${token}`,
            },
        });
        const text = await res.text();
        if (!res.ok) {
            let code;
            let message = text || res.statusText;
            try {
                const parsed = text ? JSON.parse(text) : {};
                code = parsed.error_code;
                if (parsed.message) {
                    message = parsed.message;
                }
            }
            catch {
                // ignore
            }
            throw new ApiError(`clusters/get failed ${res.status}: ${message}`, res.status, '2.0', text || message, code);
        }
        try {
            const parsed = text ? JSON.parse(text) : {};
            return (parsed.state || 'UNKNOWN').toUpperCase();
        }
        catch {
            return 'UNKNOWN';
        }
    };
    const triggerStart = async () => {
        const url = `${host}/api/2.0/clusters/start`;
        const res = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ cluster_id: clusterId }),
        });
        const text = await res.text();
        if (!res.ok) {
            let code;
            let message = text || res.statusText;
            try {
                const parsed = text ? JSON.parse(text) : {};
                code = parsed.error_code;
                if (parsed.message) {
                    message = parsed.message;
                }
            }
            catch {
                // ignore
            }
            throw new ApiError(`Start cluster failed ${res.status}: ${message}`, res.status, '2.0', text || message, code);
        }
    };
    const initial = await getState();
    if (initial === 'RUNNING') {
        return 'RUNNING';
    }
    output.appendLine(`Cluster ${clusterId} is ${initial}, requesting start...`);
    await triggerStart();
    if (!poll) {
        return 'RUNNING';
    }
    while (Date.now() - start < timeoutMs) {
        await sleep(pollIntervalMs);
        const state = await getState();
        output.appendLine(`Cluster ${clusterId} state: ${state}`);
        if (state === 'RUNNING') {
            return 'RUNNING';
        }
        if (state === 'ERROR' || state === 'TERMINATED') {
            return 'ERROR';
        }
    }
    output.appendLine(`Cluster ${clusterId} did not reach RUNNING within ${Math.round(timeoutMs / 1000)}s.`);
    return 'ERROR';
}
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
async function getRunDetails(host, token, runId) {
    const output = getOutputChannel();
    const searchParams = new URLSearchParams();
    searchParams.set('run_id', runId.toString());
    const url = `${host}/api/2.1/jobs/runs/get?${searchParams.toString()}`;
    const res = await fetch(url, {
        method: 'GET',
        headers: {
            'Authorization': `Bearer ${token}`,
        },
    });
    const text = await res.text();
    if (!res.ok) {
        let errCode;
        let message = text || res.statusText;
        try {
            const parsed = text ? JSON.parse(text) : {};
            errCode = parsed.error_code;
            if (parsed.message) {
                message = parsed.message;
            }
        }
        catch {
            // ignore
        }
        const suffix = errCode ? ` ${errCode}` : '';
        output.appendLine(`runs/get failed: HTTP ${res.status}${suffix} ${message}`);
        throw new ApiError(`HTTP ${res.status}${suffix}: ${message || res.statusText}`.trim(), res.status, '2.1', text || message, errCode);
    }
    let parsed = {};
    try {
        parsed = text ? JSON.parse(text) : {};
    }
    catch {
        throw new Error('runs/get returned invalid JSON.');
    }
    if (!parsed.run_id) {
        output.appendLine('runs/get returned response without run_id field.');
    }
    return parsed;
}
function isWrongMethodError(err) {
    return !!err.body && /post\s+\/jobs\/runs\/list/i.test(err.body);
}
function isEndpointMissing(err) {
    if (err.status === 404 || err.status === 405) {
        return true;
    }
    if (err.body && /endpoint not found/i.test(err.body)) {
        return true;
    }
    return false;
}
let outputChannel;
function getOutputChannel() {
    if (!outputChannel) {
        outputChannel = vscode.window.createOutputChannel('Databricks Tools');
    }
    return outputChannel;
}
//# sourceMappingURL=databricksClient.js.map