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
exports.DatabricksViewProvider = void 0;
const vscode = __importStar(require("vscode"));
const databricksClient_1 = require("./databricksClient");
class SimpleTreeItem extends vscode.TreeItem {
    constructor(data) {
        super(data.label, vscode.TreeItemCollapsibleState.None);
        this.description = data.description;
        this.tooltip = data.tooltip;
        if (data.command) {
            this.command = data.command;
        }
        if (data.icon) {
            this.iconPath = new vscode.ThemeIcon(data.icon);
        }
    }
}
class DatabricksViewProvider {
    context;
    _onDidChangeTreeData = new vscode.EventEmitter();
    onDidChangeTreeData = this._onDidChangeTreeData.event;
    lastConnectionStatus;
    constructor(context) {
        this.context = context;
    }
    refresh(statusMessage) {
        this.lastConnectionStatus = statusMessage ?? this.lastConnectionStatus;
        this._onDidChangeTreeData.fire();
    }
    getTreeItem(element) {
        return element;
    }
    async getChildren() {
        const status = await (0, databricksClient_1.getAuthStatus)(this.context);
        const items = [];
        items.push(new SimpleTreeItem({
            label: this.lastConnectionStatus ? `Status: ${this.lastConnectionStatus}` : 'Status: Not tested',
            description: 'Click to test',
            command: { command: 'databricksTools.testConnection', title: 'Test Connection' },
            icon: this.lastConnectionStatus?.startsWith('Connected') ? 'check' : 'plug',
        }));
        items.push(new SimpleTreeItem({
            label: 'Host',
            description: status.hostConfigured ? status.hostSource === 'env' ? 'Env override' : 'Configured' : 'Not configured',
            tooltip: status.hostConfigured ? 'Databricks workspace host' : 'Host not set',
            command: { command: 'databricksTools.configure', title: 'Configure Connection' },
            icon: status.hostConfigured ? 'cloud' : 'warning',
        }));
        items.push(new SimpleTreeItem({
            label: `Auth mode: ${status.authMode}`,
            description: 'auto/pat/azureCli',
            command: { command: 'databricksTools.setAuthMode', title: 'Switch Auth Mode' },
            icon: 'key',
        }));
        items.push(new SimpleTreeItem({
            label: `PAT available: ${status.patAvailable ? 'yes' : 'no'}`,
            description: status.patSource ? `source: ${status.patSource}` : undefined,
            icon: status.patAvailable ? 'shield' : 'error',
        }));
        items.push(new SimpleTreeItem({
            label: `Azure CLI: ${status.azureCliAvailable ? 'available' : 'not found'}`,
            description: status.azureCliAvailable
                ? status.azureCliLoggedIn
                    ? 'logged in'
                    : 'not logged in'
                : 'install Azure CLI',
            icon: status.azureCliAvailable ? 'terminal' : 'warning',
            command: status.azureCliAvailable ? undefined : { command: 'databricksTools.setAuthMode', title: 'Switch Auth Mode' },
        }));
        items.push(new SimpleTreeItem({
            label: 'Configure Connection',
            command: { command: 'databricksTools.configure', title: 'Configure Connection' },
            icon: 'gear',
        }));
        items.push(new SimpleTreeItem({
            label: 'Switch Auth Mode',
            command: { command: 'databricksTools.setAuthMode', title: 'Switch Auth Mode' },
            icon: 'sync',
        }));
        items.push(new SimpleTreeItem({
            label: 'Test Connection',
            command: { command: 'databricksTools.testConnection', title: 'Test Connection' },
            icon: 'debug-start',
        }));
        items.push(new SimpleTreeItem({
            label: 'Test Jobs API',
            command: { command: 'databricksTools.testJobsApi', title: 'Test Jobs API' },
            icon: 'beaker',
        }));
        items.push(new SimpleTreeItem({
            label: 'List Clusters',
            command: { command: 'databricksTools.listClusters', title: 'List Clusters' },
            icon: 'server',
        }));
        items.push(new SimpleTreeItem({
            label: 'Start Cluster…',
            command: { command: 'databricksTools.startCluster', title: 'Start Cluster' },
            icon: 'play',
        }));
        items.push(new SimpleTreeItem({
            label: 'Get Job Definition…',
            command: { command: 'databricksTools.getJobDefinition', title: 'Get Job Definition' },
            icon: 'book',
        }));
        items.push(new SimpleTreeItem({
            label: 'Clear Credentials',
            command: { command: 'databricksTools.clearCredentials', title: 'Clear Credentials' },
            icon: 'trash',
        }));
        return items;
    }
}
exports.DatabricksViewProvider = DatabricksViewProvider;
//# sourceMappingURL=databricksView.js.map