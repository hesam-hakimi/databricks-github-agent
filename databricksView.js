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
class SectionTreeItem extends vscode.TreeItem {
    children;
    constructor(children, label) {
        super(label, vscode.TreeItemCollapsibleState.Expanded);
        this.children = children;
        this.iconPath = new vscode.ThemeIcon('folder');
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
    async getChildren(element) {
        if (element instanceof SectionTreeItem) {
            return element.children;
        }
        const status = await (0, databricksClient_1.getAuthStatus)(this.context);
        const connectionItems = [
            new SimpleTreeItem({
                label: this.lastConnectionStatus ? `Status: ${this.lastConnectionStatus}` : 'Status: Not tested',
                description: 'Click to test',
                command: { command: 'databricksTools.testConnection', title: 'Test Connection' },
                icon: this.lastConnectionStatus?.startsWith('Connected') ? 'check' : 'plug',
            }),
            new SimpleTreeItem({
                label: 'Host',
                description: status.hostConfigured ? (status.hostSource === 'env' ? 'Env override' : 'Configured') : 'Not configured',
                tooltip: status.hostConfigured ? 'Databricks workspace host' : 'Host not set',
                command: { command: 'databricksTools.configure', title: 'Configure Connection' },
                icon: status.hostConfigured ? 'cloud' : 'warning',
            }),
            new SimpleTreeItem({
                label: `Auth mode: ${status.authMode}`,
                description: 'auto/pat/azureCli',
                command: { command: 'databricksTools.setAuthMode', title: 'Switch Auth Mode' },
                icon: 'key',
            }),
            new SimpleTreeItem({
                label: `PAT available: ${status.patAvailable ? 'yes' : 'no'}`,
                description: status.patSource ? `source: ${status.patSource}` : undefined,
                icon: status.patAvailable ? 'shield' : 'error',
            }),
            new SimpleTreeItem({
                label: `Azure CLI: ${status.azureCliAvailable ? 'available' : 'not found'}`,
                description: status.azureCliAvailable
                    ? status.azureCliLoggedIn
                        ? 'logged in'
                        : 'not logged in'
                    : 'install Azure CLI',
                icon: status.azureCliAvailable ? 'terminal' : 'warning',
                command: status.azureCliAvailable ? undefined : { command: 'databricksTools.setAuthMode', title: 'Switch Auth Mode' },
            }),
            new SimpleTreeItem({
                label: 'Configure Connection',
                command: { command: 'databricksTools.configure', title: 'Configure Connection' },
                icon: 'gear',
            }),
            new SimpleTreeItem({
                label: 'Switch Auth Mode',
                command: { command: 'databricksTools.setAuthMode', title: 'Switch Auth Mode' },
                icon: 'sync',
            }),
            new SimpleTreeItem({
                label: 'Clear Credentials',
                command: { command: 'databricksTools.clearCredentials', title: 'Clear Credentials' },
                icon: 'trash',
            }),
        ];
        const toolsItems = [
            new SimpleTreeItem({
                label: 'Get Job Definition…',
                command: { command: 'databricksTools.getJobDefinition', title: 'Get Job Definition' },
                icon: 'book',
            }),
            new SimpleTreeItem({
                label: 'Get Notebook Source…',
                command: { command: 'databricksTools.getNotebookSource', title: 'Get Notebook Source' },
                icon: 'file-code',
            }),
            new SimpleTreeItem({
                label: 'Create & Run Job From Code…',
                command: { command: 'databricksTools.createJobFromCode', title: 'Create & Run Job From Code' },
                icon: 'rocket',
            }),
            new SimpleTreeItem({
                label: 'Get Job Runs…',
                command: { command: 'databricksTools.getRuns', title: 'Get Job Runs' },
                icon: 'list-ordered',
            }),
            new SimpleTreeItem({
                label: 'Get Run Details…',
                command: { command: 'databricksTools.getRunDetails', title: 'Get Run Details' },
                icon: 'search',
            }),
            new SimpleTreeItem({
                label: 'List Clusters',
                command: { command: 'databricksTools.listClusters', title: 'List Clusters' },
                icon: 'server',
            }),
            new SimpleTreeItem({
                label: 'Start Cluster…',
                command: { command: 'databricksTools.startCluster', title: 'Start Cluster' },
                icon: 'play',
            }),
        ];
        const debugItems = [
            new SimpleTreeItem({
                label: 'Test Connection',
                command: { command: 'databricksTools.testConnection', title: 'Test Connection' },
                icon: 'debug-start',
            }),
            new SimpleTreeItem({
                label: 'Test Jobs API',
                command: { command: 'databricksTools.testJobsApi', title: 'Test Jobs API' },
                icon: 'beaker',
            }),
            new SimpleTreeItem({
                label: 'Show Logs',
                command: { command: 'databricksTools.showLogs', title: 'Show Logs' },
                icon: 'output',
            }),
        ];
        return [
            new SectionTreeItem(connectionItems, 'Connection'),
            new SectionTreeItem(toolsItems, 'Tools'),
            new SectionTreeItem(debugItems, 'Debug'),
        ];
    }
}
exports.DatabricksViewProvider = DatabricksViewProvider;
//# sourceMappingURL=databricksView.js.map