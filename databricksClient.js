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
exports.DatabricksClient = exports.ConfigCancelled = void 0;
exports.ensureDatabricksConfig = ensureDatabricksConfig;
exports.configureConnection = configureConnection;
exports.clearStoredCredentials = clearStoredCredentials;
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
        const url = `${this.host}/api/2.1/jobs/runs/list`;
        const body = { job_id: jobId, limit };
        const controller = new AbortController();
        token.onCancellationRequested(() => controller.abort());
        const res = await fetch(url, {
            method: 'POST',
            headers: {
                'Authorization': `Bearer ${this.token}`,
                'Content-Type': 'application/json',
            },
            body: JSON.stringify(body),
            signal: controller.signal,
        });
        if (!res.ok) {
            const text = await res.text();
            throw new Error(`Databricks API error ${res.status}: ${text || res.statusText}`);
        }
        const data = (await res.json());
        return data.runs ?? [];
    }
}
exports.DatabricksClient = DatabricksClient;
async function getAzureCliToken() {
    try {
        // Databricks Azure resource ID
        const resource = 'https://databricks.azure.net';
        const { stdout } = await exec(`az account get-access-token --resource ${resource} --query accessToken -o tsv`);
        const token = stdout.trim();
        return token || undefined;
    }
    catch (err) {
        console.warn('Failed to acquire Azure CLI token', err);
        return undefined;
    }
}
async function ensureDatabricksConfig(context, options) {
    const envHost = (process.env.DATABRICKS_HOST || '').trim();
    const envToken = (process.env.DATABRICKS_TOKEN || '').trim();
    const config = vscode.workspace.getConfiguration('databricksTools');
    const pref = config.get('authPreference', 'pat');
    const storedHost = (config.get('host') || '').trim();
    const storedToken = await context.secrets.get('databricksTools.token');
    if (options?.forcePrompt) {
        const host = await promptForHost(envHost || storedHost);
        if (host === undefined) {
            throw new ConfigCancelled('Configuration cancelled before host was provided.');
        }
        await saveHostSetting(host);
        const token = await promptForToken();
        if (token === undefined) {
            throw new ConfigCancelled('Configuration cancelled before token was provided.');
        }
        await context.secrets.store('databricksTools.token', token);
        return {
            host: (process.env.DATABRICKS_HOST || host).replace(/\/$/, ''),
            token: process.env.DATABRICKS_TOKEN || token,
        };
    }
    let host = envHost || storedHost;
    if (!host) {
        const promptedHost = await promptForHost(storedHost);
        if (promptedHost === undefined) {
            throw new ConfigCancelled('The Databricks host is missing. Ask the user to configure it or provide it now (configuration cancelled).');
        }
        host = promptedHost;
        await saveHostSetting(promptedHost);
    }
    let token = envToken;
    if (!token && storedToken) {
        token = storedToken;
    }
    if (!token && pref === 'azureCli') {
        token = (await getAzureCliToken()) || '';
    }
    if (!token) {
        const promptedToken = await promptForToken();
        if (promptedToken === undefined) {
            throw new ConfigCancelled('The Databricks token is missing. Ask the user to configure it by providing a PAT or rerun configuration (cancelled).');
        }
        token = promptedToken;
        await context.secrets.store('databricksTools.token', token);
    }
    if (!token) {
        throw new Error('Databricks token acquisition failed. Set DATABRICKS_TOKEN, use Azure CLI, or provide a PAT.');
    }
    return { host: host.replace(/\/$/, ''), token };
}
async function configureConnection(context) {
    await ensureDatabricksConfig(context, { forcePrompt: true });
    void vscode.window.showInformationMessage('Databricks connection updated for future tool runs.');
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
//# sourceMappingURL=databricksClient.js.map