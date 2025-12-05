"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_WORKSPACE_FOLDER = void 0;
exports.computeUploadWorkspacePath = computeUploadWorkspacePath;
exports.normalizeWorkspaceFolder = normalizeWorkspaceFolder;
exports.normalizeCandidateWorkspaceFolder = normalizeCandidateWorkspaceFolder;
exports.sanitizeName = sanitizeName;
exports.formatTimestamp = formatTimestamp;
exports.languageToExtension = languageToExtension;
exports.DEFAULT_WORKSPACE_FOLDER = '/Workspace/Shared/CopilotJobs';
function computeUploadWorkspacePath(options) {
    const { explicitWorkspacePath, jobName, language, defaultWorkspaceFolder, appendProjectSubfolder = false, projectName, now, } = options;
    const ext = languageToExtension(language);
    const safeJob = sanitizeName(jobName || 'job');
    const timestamp = formatTimestamp(now ?? new Date());
    if (explicitWorkspacePath && explicitWorkspacePath.trim()) {
        const trimmed = explicitWorkspacePath.trim();
        const isFile = /\.(py|sql|scala|r)$/i.test(trimmed);
        if (isFile) {
            return trimmed;
        }
        const base = trimTrailingSlash(trimmed);
        return `${base}/${safeJob}-${timestamp}.${ext}`;
    }
    const normalizedDefault = normalizeWorkspaceFolder(defaultWorkspaceFolder);
    const baseFolder = appendProjectSubfolder
        ? `${normalizedDefault}/${sanitizeName(projectName || 'default-project')}`
        : normalizedDefault;
    return `${baseFolder}/${safeJob}-${timestamp}.${ext}`;
}
function normalizeWorkspaceFolder(folder) {
    const normalized = normalizeCandidateWorkspaceFolder(folder);
    return normalized ?? exports.DEFAULT_WORKSPACE_FOLDER;
}
function normalizeCandidateWorkspaceFolder(folder) {
    if (!folder) {
        return undefined;
    }
    const trimmed = folder.trim();
    if (!trimmed) {
        return undefined;
    }
    if (!trimmed.startsWith('/Workspace/')) {
        return undefined;
    }
    const withoutTrailing = trimTrailingSlash(trimmed);
    return withoutTrailing || undefined;
}
function sanitizeName(name) {
    const cleaned = name.trim().toLowerCase().replace(/[^a-z0-9-_]+/g, '-').replace(/^-+|-+$/g, '');
    return cleaned || 'job';
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
function trimTrailingSlash(path) {
    return path.replace(/\/+$/, '');
}
//# sourceMappingURL=workspacePath.js.map