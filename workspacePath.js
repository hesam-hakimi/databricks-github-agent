"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.DEFAULT_WORKSPACE_FOLDER = void 0;
exports.computeUploadWorkspacePath = computeUploadWorkspacePath;
exports.normalizeWorkspaceFolder = normalizeWorkspaceFolder;
exports.normalizeCandidateWorkspaceFolder = normalizeCandidateWorkspaceFolder;
exports.sanitizeName = sanitizeName;
exports.formatTimestamp = formatTimestamp;
exports.languageToExtension = languageToExtension;
exports.capJsonString = capJsonString;
const buffer_1 = require("buffer");
exports.DEFAULT_WORKSPACE_FOLDER = '/Workspace/Shared/CopilotJobs';
function computeUploadWorkspacePath(options) {
    const { explicitWorkspacePath, jobName, language, defaultWorkspaceFolder, appendProjectSubfolder = false, projectName, now, } = options;
    const ext = languageToExtension(language);
    const safeJob = sanitizeName(jobName || 'job');
    const timestamp = formatTimestamp(now ?? new Date());
    const explicit = explicitWorkspacePath?.trim();
    if (explicit) {
        if (isWorkspaceFilePath(explicit)) {
            return explicit;
        }
        const normalizedFolder = normalizeCandidateWorkspaceFolder(explicit);
        if (normalizedFolder) {
            return `${normalizedFolder}/${safeJob}-${timestamp}.${ext}`;
        }
    }
    const baseDefault = ensureBaseFolder(defaultWorkspaceFolder);
    const project = appendProjectSubfolder ? sanitizeName(projectName || '') : '';
    const base = project ? `${baseDefault}/${project}` : baseDefault;
    return `${base}/${safeJob}-${timestamp}.${ext}`;
}
function normalizeWorkspaceFolder(folder) {
    return ensureBaseFolder(folder);
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
    if (isWorkspaceFilePath(trimmed)) {
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
function isWorkspaceFilePath(path) {
    if (!path.startsWith('/')) {
        return false;
    }
    if (/^[a-z]+:\/\//i.test(path)) {
        return false;
    }
    return /\.(py|sql|scala|r)$/i.test(path);
}
function ensureBaseFolder(base) {
    const normalized = normalizeCandidateWorkspaceFolder(base);
    return normalized ?? exports.DEFAULT_WORKSPACE_FOLDER;
}
function capJsonString(jsonString, maxBytes) {
    if (!maxBytes || maxBytes <= 0) {
        return { text: jsonString, truncated: false };
    }
    const buf = buffer_1.Buffer.from(jsonString, 'utf8');
    if (buf.byteLength <= maxBytes) {
        return { text: jsonString, truncated: false };
    }
    const truncated = buf.subarray(0, maxBytes);
    return { text: truncated.toString('utf8'), truncated: true };
}
//# sourceMappingURL=workspacePath.js.map