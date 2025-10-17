import * as vscode from 'vscode';
import { execFile } from 'child_process';
import { access } from 'fs/promises';
import { constants as fsConstants } from 'fs';
import { promisify } from 'util';
import { load as loadYaml } from 'js-yaml';
import * as os from 'os';
import * as path from 'path';

const execFileAsync = promisify(execFile);

export type CciViewKind = 'orgs' | 'tasks' | 'flows' | 'project' | 'services';
type CciRecord = Record<string, unknown>;
type CciItemKind =
	| 'folder'
	| 'group'
	| 'org'
	| 'task'
	| 'flow'
	| 'serviceType'
	| 'service'
	| 'message'
	| 'error';

type IconDefinition =
	| vscode.ThemeIcon
	| vscode.Uri
	| { light: vscode.Uri; dark: vscode.Uri };

interface TreeCommandSpec {
	readonly id: string;
	readonly title?: string;
}

interface CciItem {
	readonly label: string;
	readonly description?: string;
	readonly tooltip?: string | vscode.MarkdownString;
	readonly collapsibleState?: vscode.TreeItemCollapsibleState;
	readonly contextValue?: string;
	readonly iconPath?: IconDefinition;
	readonly children?: CciItem[];
	readonly workspaceFolder?: vscode.WorkspaceFolder;
	readonly data?: CciRecord;
	readonly itemKind?: CciItemKind;
	readonly command?: TreeCommandSpec;
}

interface OrgDefinitionMeta {
	alias: string;
	configPath?: string;
	isScratch?: boolean;
	sources: Set<string>;
}

interface CciFetchResult {
	readonly records: unknown[];
	readonly format: 'json' | 'text';
}

interface OrgQuickPickItem extends vscode.QuickPickItem {
	readonly alias: string;
	readonly data?: CciRecord;
	readonly orgCreated: boolean;
	readonly expired: boolean;
	readonly isScratch: boolean;
	readonly definitionOnly: boolean;
	readonly definitionMissing: boolean;
	readonly manual?: boolean;
}

interface ServiceListRow {
	type: string;
	name?: string;
	isDefault: boolean;
	description: string;
}

interface ServiceEntryMeta {
	readonly name: string;
	readonly description: string;
	readonly isDefault: boolean;
}

interface ServiceAttributeSpec {
	readonly name: string;
	readonly description?: string;
	readonly required: boolean;
	readonly sensitive: boolean;
	readonly defaultValue?: string;
	readonly defaultFactory?: string;
}

interface ServiceTypeMetadata {
	readonly description?: string;
	readonly attributes: ServiceAttributeSpec[];
}

interface ServiceTypeGroup {
	readonly type: string;
	readonly label: string;
	readonly description: string;
	readonly entries: ServiceEntryMeta[];
	readonly attributes: ServiceAttributeSpec[];
}

interface ServiceInfoEntry {
	key: string;
	value: string;
}

interface ServiceInfoResult {
	title?: string;
	entries: ServiceInfoEntry[];
}

class CciTreeItem extends vscode.TreeItem {
	public readonly children: CciTreeItem[];
	public readonly workspaceFolder?: vscode.WorkspaceFolder;
	public readonly data?: CciRecord;
	public readonly itemKind?: CciItemKind;

	constructor(item: CciItem, private readonly extensionUri: vscode.Uri) {
		super(
			item.label,
			item.collapsibleState ?? (item.children && item.children.length > 0
				? vscode.TreeItemCollapsibleState.Collapsed
				: vscode.TreeItemCollapsibleState.None)
		);
		this.description = item.description;
		this.tooltip = item.tooltip;
		this.contextValue = item.contextValue ?? item.itemKind;
		this.iconPath = item.iconPath;
		this.workspaceFolder = item.workspaceFolder;
		this.data = item.data;
		this.itemKind = item.itemKind;
		this.children = (item.children ?? []).map((child) => new CciTreeItem(child, this.extensionUri));

		if (item.command) {
			this.command = {
				command: item.command.id,
				title: item.command.title ?? toTreeItemLabelString(item.label),
				arguments: [this]
			};
		}
	}
}

class CciTreeDataProvider implements vscode.TreeDataProvider<CciTreeItem> {
	private readonly _onDidChangeTreeData = new vscode.EventEmitter<CciTreeItem | undefined>();
	readonly onDidChangeTreeData = this._onDidChangeTreeData.event;

	constructor(
		private readonly kind: CciViewKind,
		private readonly service: CciService,
		private readonly extensionUri: vscode.Uri
	) {}

	refresh(): void {
		this._onDidChangeTreeData.fire(undefined);
	}

	getTreeItem(element: CciTreeItem): vscode.TreeItem {
		return element;
	}

	async getChildren(element?: CciTreeItem): Promise<CciTreeItem[]> {
		if (element) {
			return element.children;
		}

		const projectFolders = await this.service.getProjectFolders();
		if (projectFolders.length === 0) {
			return [
				new CciTreeItem({
					label: 'No CumulusCI project found in workspace',
					description: 'Open a folder containing cumulusci.yml',
					contextValue: 'cumulusci.message',
					itemKind: 'message'
				}, this.extensionUri)
			];
		}

		try {
			const items: CciItem[] = [];
			for (const folder of projectFolders) {
				const children = await this.loadItemsForFolder(folder);
				if (projectFolders.length === 1) {
					items.push(...children);
					continue;
				}

				items.push({
					label: folder.name,
					collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
					contextValue: 'cumulusci.folder',
					itemKind: 'folder',
					children,
					workspaceFolder: folder
				});
			}

			if (items.length === 0) {
				return [
					new CciTreeItem({
						label: this.emptyStateLabel(),
						contextValue: 'cumulusci.message',
						itemKind: 'message'
					}, this.extensionUri)
				];
			}

			return items.map((item) => new CciTreeItem(item, this.extensionUri));
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			return [
				new CciTreeItem({
					label: `Failed to load ${this.kind}`,
					description: message,
					tooltip: message,
					contextValue: 'cumulusci.error',
					itemKind: 'error'
				}, this.extensionUri)
			];
		}
	}

	private emptyStateLabel(): string {
		if (this.kind === 'orgs') {
			return 'No orgs found in CCI project';
		}
		if (this.kind === 'tasks') {
			return 'No tasks defined in CCI project';
		}
		if (this.kind === 'flows') {
			return 'No flows defined in CCI project';
		}
		return 'Project info unavailable';
	}

	private loadItemsForFolder(folder: vscode.WorkspaceFolder): Promise<CciItem[]> {
		if (this.kind === 'orgs') {
			return this.service.listOrgs(folder);
		}
		if (this.kind === 'tasks') {
			return this.service.listTasks(folder);
		}
		if (this.kind === 'flows') {
			return this.service.listFlows(folder);
		}
		if (this.kind === 'services') {
			return this.service.listServices(folder);
		}
		return this.service.listProjectInfo(folder);
	}
}

interface OrgEntry {
	readonly alias: string;
	readonly data: CciRecord;
	readonly description?: string;
	readonly tooltip: vscode.MarkdownString;
	readonly contextValue: string;
	readonly iconPath: IconDefinition;
	readonly command?: TreeCommandSpec;
	readonly isScratch: boolean;
	readonly orgCreated: boolean;
	readonly expired: boolean;
	readonly definitionOnly: boolean;
	readonly definitionMissing: boolean;
}

class CciService {
	private readonly configCandidates = ['cumulusci.yml', 'cumulusci.yaml'];
	private readonly orgLabelKeys = ['alias', 'name', 'org_name', 'config_name', 'key'];
	private readonly orgDescriptionKeys = ['username', 'instance_url', 'status'];
	private readonly taskLabelKeys = ['name', 'task_name'];
	private readonly taskDescriptionKeys = ['description', 'group'];
	private readonly flowLabelKeys = ['name', 'flow'];
	private readonly flowDescriptionKeys = ['description', 'group'];
	private readonly groupKeys = ['group', 'category', 'type'];

	private readonly icons: {
		scratchActive: vscode.Uri;
		scratchInactive: vscode.Uri;
		connected: vscode.Uri;
		orgDefault: vscode.Uri;
		flow: { light: vscode.Uri; dark: vscode.Uri };
		refresh: { light: vscode.Uri; dark: vscode.Uri };
		run: { light: vscode.Uri; dark: vscode.Uri };
	};

	constructor(
		private readonly extensionUri: vscode.Uri,
		private readonly output: vscode.OutputChannel
	) {
		this.icons = {
			scratchActive: this.icon('media', 'images', 'green-circle-small.svg'),
			scratchInactive: this.icon('media', 'images', 'green-circle-dashed.svg'),
			connected: this.icon('media', 'images', 'connected-blue.svg'),
			orgDefault: this.icon('media', 'images', 'grey-ring.svg'),
			flow: {
				light: this.icon('resources', 'light', 'flow.svg'),
				dark: this.icon('resources', 'dark', 'flow.svg')
			},
			refresh: {
				light: this.icon('resources', 'light', 'refresh.svg'),
				dark: this.icon('resources', 'dark', 'refresh.svg')
			},
			run: {
				light: this.icon('resources', 'light', 'run.svg'),
				dark: this.icon('resources', 'dark', 'run.svg')
			}
		};
	}

	async getProjectFolders(): Promise<vscode.WorkspaceFolder[]> {
		const folders = vscode.workspace.workspaceFolders ?? [];
		const matches: vscode.WorkspaceFolder[] = [];
		for (const folder of folders) {
			if (await this.hasCciConfig(folder)) {
				matches.push(folder);
			}
		}
		return matches;
	}

	async listOrgs(folder: vscode.WorkspaceFolder): Promise<CciItem[]> {
		const entries = await this.getOrgEntries(folder);
		return entries.map((entry) => this.orgEntryToTreeItem(folder, entry));
	}

	async findDefaultOrg(): Promise<{ folder: vscode.WorkspaceFolder; entry: OrgEntry } | undefined> {
		const folders = await this.getProjectFolders();
		for (const folder of folders) {
			const entries = await this.getOrgEntries(folder);
			const match = entries.find((entry) => getBoolean(entry.data, ['is_default', 'default', 'isDefault']) === true);
			if (match) {
				return { folder, entry: match };
			}
		}
		return undefined;
	}

	async listTasks(folder: vscode.WorkspaceFolder): Promise<CciItem[]> {
		let { records } = await this.fetchRecords(folder, ['task', 'list'], 'tasks');
		records = await this.includeConfigDefinitions(records, folder, 'tasks');
		return this.buildGroupedItems(folder, records, {
			labelKeys: this.taskLabelKeys,
			descriptionKeys: this.taskDescriptionKeys,
			itemKind: 'task',
			contextValue: 'cumulusci.task',
			command: { id: 'cumulusci.runTask', title: 'Run Task' },
			iconPath: new vscode.ThemeIcon('tools')
		});
	}

	async listFlows(folder: vscode.WorkspaceFolder): Promise<CciItem[]> {
		let { records } = await this.fetchRecords(folder, ['flow', 'list'], 'flows');
		records = await this.includeConfigDefinitions(records, folder, 'flows');
		return this.buildGroupedItems(folder, records, {
			labelKeys: this.flowLabelKeys,
			descriptionKeys: this.flowDescriptionKeys,
			itemKind: 'flow',
			contextValue: 'cumulusci.flow',
			command: { id: 'cumulusci.runFlow', title: 'Run Flow' },
			iconPath: this.icons.flow
		});
	}

	async listServices(folder: vscode.WorkspaceFolder): Promise<CciItem[]> {
		const groups = await this.getServiceTypeGroups(folder);
		if (groups.length === 0) {
			return [
				{
					label: 'No services reported by CCI',
					description: 'Run cci service connect to add one',
					contextValue: 'cumulusci.message',
					itemKind: 'message'
				}
			];
		}

		return groups.map((group) => {
			const tooltip = group.description ? group.description : undefined;
			const children: CciItem[] = [];
			for (const entry of group.entries) {
				children.push({
					label: entry.name,
					description: entry.isDefault ? 'Default' : undefined,
					tooltip: entry.description,
					contextValue: entry.isDefault ? 'cumulusci.service.default' : 'cumulusci.service',
					itemKind: 'service',
					iconPath: entry.isDefault ? new vscode.ThemeIcon('star-full') : new vscode.ThemeIcon('key'),
					data: {
						type: group.type,
						typeLabel: group.label,
						name: entry.name,
						description: entry.description,
						isDefault: entry.isDefault,
						attributes: group.attributes
					},
					workspaceFolder: folder,
					command: { id: 'cumulusci.service.showInfo', title: 'Show Service Info' }
				});
			}
			if (children.length === 0) {
				children.push({
					label: 'No service connected',
					description: 'Run "cci service connect" to configure',
					contextValue: 'cumulusci.message',
					itemKind: 'message',
					workspaceFolder: folder
				});
			}

			return {
				label: group.label,
				description: children.some((child) => child.itemKind === 'service') ? undefined : 'Not configured',
				tooltip,
				contextValue: 'cumulusci.serviceType',
				itemKind: 'serviceType',
				iconPath: new vscode.ThemeIcon('plug'),
				children,
				workspaceFolder: folder,
				data: {
					type: group.type,
					label: group.label,
					description: group.description,
					attributes: group.attributes
				}
			};
		});
	}

	async getServiceTypeGroups(folder: vscode.WorkspaceFolder): Promise<ServiceTypeGroup[]> {
		const stdout = await this.runCciCommand(folder, ['service', 'list', '--plain']);
		const rows = this.parseServiceListOutput(stdout);
		if (rows.length === 0) {
			return [];
		}
		const metadata = await this.fetchServiceTypeMetadata(folder);
		return this.groupServiceRows(rows, metadata);
	}

	async listProjectInfo(folder: vscode.WorkspaceFolder): Promise<CciItem[]> {
		const rawOutput = await this.runCciCommand(folder, ['project', 'info']);
		const parsed = this.parseProjectInfoOutput(rawOutput);
		const cleaned = this.stripAnsiSequences(rawOutput).trim();
		if (!parsed) {
			if (!cleaned) {
				return [];
			}
			return [
				{
					label: 'Unable to parse project info output',
					description: 'See tooltip for raw content',
					tooltip: cleaned,
					itemKind: 'error',
					contextValue: 'cumulusci.error'
				}
			];
		}

		const items = this.buildProjectInfoItems(parsed);
		if (items.length === 0) {
			return [];
		}
		return items;
	}

	async getOrgQuickPickItems(folder: vscode.WorkspaceFolder): Promise<OrgQuickPickItem[]> {
		const entries = await this.getOrgEntries(folder);
		return entries
			.filter((entry) => !entry.definitionMissing)
			.map((entry) => ({
				label: entry.alias,
				description: entry.description,
				detail: this.describeOrgEntry(entry),
				alias: entry.alias,
				data: entry.data,
				orgCreated: entry.orgCreated,
				expired: entry.expired,
				isScratch: entry.isScratch,
				definitionOnly: entry.definitionOnly,
				definitionMissing: entry.definitionMissing
			}))
			.sort((a, b) => a.label.localeCompare(b.label));
	}

	async runCciCommand(folder: vscode.WorkspaceFolder, args: string[]): Promise<string> {
		const commandText = ['cci', ...args.map(quoteArg)].join(' ');
		const label = this.describeWorkspace(folder);
		this.output.appendLine(`${label}$ ${commandText}`);
		const started = Date.now();
		try {
			const { stdout } = await execFileAsync('cci', args, {
				cwd: folder.uri.fsPath,
				env: {
					...process.env,
					CUMULUSCI_DISABLE_GIST_LOGGER: '1'
				}
			});
			this.logCommandResult(label, stdout, Date.now() - started);
			return stdout;
		} catch (error) {
			if ((error as NodeJS.ErrnoException).code === 'ENOENT') {
				const message = 'CumulusCI CLI (cci) not found. To use this extension, first install the CumulusCI CLI.\n\nInstall CumulusCI CLI by following the instructions at https://cumulusci.readthedocs.io/en/latest/install.html and ensure that the "cci" command is available in your system PATH.';
				this.output.appendLine(`${label}! ${message}`);
				throw new Error(message);
			}
			const stderr = (error as { stderr?: string }).stderr ?? (error as Error).message;
			const sanitized = this.stripAnsiSequences(stderr ?? '').trim();
			if (sanitized.length > 0) {
				this.output.appendLine(`${label}! ${sanitized}`);
			}
			throw new Error(sanitized || 'Unknown CCI CLI error');
		}
	}

	async getOrgInfo(folder: vscode.WorkspaceFolder, alias: string): Promise<CciRecord> {
		const stdout = await this.runCciCommand(folder, ['org', 'info', alias, '--json']);
		const data = this.parseJsonObject(stdout);
		if (!data) {
			throw new Error('Failed to parse CCI org info output as JSON.');
		}
		return data;
	}

	async getServiceInfo(
		folder: vscode.WorkspaceFolder,
		type: string,
		name?: string
	): Promise<ServiceInfoResult> {
		const args = ['service', 'info'];
		args.push(type);
		if (name && name.trim().length > 0) {
			args.push(name.trim());
		}
		const stdout = await this.runCciCommand(folder, args);
		return this.parseServiceInfoOutput(stdout);
	}

	private parseServiceListOutput(output: string): ServiceListRow[] {
		const cleaned = this.stripAnsiSequences(output);
		const trimmed = cleaned.trim();
		if (!trimmed) {
			return [];
		}

		const lines = cleaned.split(/\r?\n/);
		const headerIndex = lines.findIndex((line) =>
			line.includes('Default') && line.includes('Type') && line.includes('Name') && line.includes('Description')
		);
		if (headerIndex === -1) {
			return [];
		}

		const headerLine = lines[headerIndex];
		const defaultStart = headerLine.indexOf('Default');
		const typeStart = headerLine.indexOf('Type', Math.max(defaultStart, 0));
		const nameStart = headerLine.indexOf('Name', Math.max(typeStart, 0));
		const descriptionStart = headerLine.indexOf('Description', Math.max(nameStart, 0));
		if (typeStart < 0 || nameStart < 0 || descriptionStart < 0) {
			return [];
		}

		const separatorIndex = lines.findIndex(
			(line, index) => index > headerIndex && /[\u2500\-]{3,}/.test(line.replace(/\s+/g, ''))
		);
		const firstDataIndex = separatorIndex >= 0 ? separatorIndex + 1 : headerIndex + 1;
		const rows: ServiceListRow[] = [];
		let current: ServiceListRow | undefined;

		for (let i = firstDataIndex; i < lines.length; i += 1) {
			const line = lines[i];
			if (!line) {
				continue;
			}
			const trimmedLine = line.trim();
			if (!trimmedLine) {
				continue;
			}
			if (/^[\u2500\-]{3,}$/.test(trimmedLine.replace(/\s+/g, ''))) {
				continue;
			}

			const defaultCol = line.slice(0, typeStart).trim();
			const typeCol = line.slice(typeStart, nameStart).trim();
			const nameCol = line.slice(nameStart, descriptionStart).trim();
			const descCol = descriptionStart < line.length ? line.slice(descriptionStart).trim() : '';

			const isContinuation = !defaultCol && !typeCol && !nameCol && descCol.length > 0;
			if (isContinuation && current) {
				current.description = this.appendWrappedText(current.description, descCol);
				continue;
			}

			if (!typeCol && !nameCol && !defaultCol) {
				continue;
			}

			const row: ServiceListRow = {
				type: typeCol,
				name: nameCol || undefined,
				isDefault: defaultCol.includes('✔'),
				description: descCol
			};
			rows.push(row);
			current = row;
		}

		return rows.map((row) => ({
			...row,
			type: row.type.trim(),
			description: this.normalizeServiceDescription(row.description)
		}));
	}

	private async fetchServiceTypeMetadata(
		folder: vscode.WorkspaceFolder
	): Promise<Map<string, ServiceTypeMetadata>> {
		try {
			const stdout = await this.runCciCommand(folder, ['service', 'list', '--json']);
			const parsed = this.parseJsonObject(stdout);
			if (!parsed) {
				return new Map();
			}
			const metadata = new Map<string, ServiceTypeMetadata>();
			for (const [type, value] of Object.entries(parsed)) {
				const record = toRecord(value);
				if (!record) {
					continue;
				}
				const attributesRecord = toRecord(record.attributes);
				const attributes: ServiceAttributeSpec[] = [];
				if (attributesRecord) {
					for (const [name, attrSpec] of Object.entries(attributesRecord)) {
						const attrRecord = toRecord(attrSpec);
						if (!attrRecord) {
							continue;
						}
						const description = pickFirstString(attrRecord, ['description']);
						const required = getBoolean(attrRecord, ['required']) ?? false;
						const sensitive = getBoolean(attrRecord, ['sensitive']) ?? false;
						const defaultValue = pickFirstString(attrRecord, ['default']);
						const defaultFactory = pickFirstString(attrRecord, ['default_factory']);
						attributes.push({
							name,
							description,
							required,
							sensitive,
							defaultValue,
							defaultFactory
						});
					}
					attributes.sort((a, b) => a.name.localeCompare(b.name));
				}
				const description = pickFirstString(record, ['description']) ?? '';
				metadata.set(type, {
					description,
					attributes
				});
			}
			return metadata;
		} catch (error) {
			if (this.didFailDueToJsonFlag(error)) {
				return new Map();
			}
			return new Map();
		}
	}

	private groupServiceRows(
		rows: ServiceListRow[],
		metadata: Map<string, ServiceTypeMetadata>
	): ServiceTypeGroup[] {
		const buckets = new Map<string, { description: string; entries: ServiceEntryMeta[] }>();
		for (const row of rows) {
			const key = row.type.trim();
			if (!key) {
				continue;
			}
			let bucket = buckets.get(key);
			if (!bucket) {
				bucket = { description: '', entries: [] };
				buckets.set(key, bucket);
			}
			const description = this.normalizeServiceDescription(row.description);
			if (description && bucket.description.length === 0) {
				bucket.description = description;
			}
			if (row.name) {
				bucket.entries.push({
					name: row.name,
					description: description,
					isDefault: row.isDefault
				});
			}
		}

		const groups: ServiceTypeGroup[] = [];
		for (const [type, bucket] of buckets.entries()) {
			const entries = bucket.entries
				.slice()
				.sort((a, b) => {
					if (a.isDefault !== b.isDefault) {
						return a.isDefault ? -1 : 1;
					}
					return a.name.localeCompare(b.name);
				});
			const meta = metadata.get(type);
			const description = bucket.description || meta?.description || '';
			groups.push({
				type,
				label: formatServiceTypeLabel(type),
				description,
				entries,
				attributes: meta?.attributes ?? []
			});
		}

		groups.sort((a, b) => a.label.localeCompare(b.label));
		return groups;
	}

	private parseServiceInfoOutput(output: string): ServiceInfoResult {
		const cleaned = this.stripAnsiSequences(output);
		const lines = cleaned.split(/\r?\n/);
		const result: ServiceInfoResult = { entries: [] };

		const headerIndex = lines.findIndex((line) => line.includes('Key') && line.includes('Value'));
		if (headerIndex === -1) {
			const fallbackTitle = lines.map((line) => line.trim()).find((line) => line.length > 0);
			if (fallbackTitle) {
				result.title = this.normalizeServiceDescription(fallbackTitle);
			}
			return result;
		}

		for (let i = 0; i < headerIndex; i += 1) {
			const candidate = lines[i]?.trim();
			if (candidate && !/^[\u2500\-]{3,}$/.test(candidate.replace(/\s+/g, ''))) {
				result.title = this.normalizeServiceDescription(candidate);
			}
		}

		const headerLine = lines[headerIndex];
		const keyStart = headerLine.indexOf('Key');
		const valueStart = headerLine.indexOf('Value', Math.max(keyStart, 0));
		if (keyStart < 0 || valueStart < 0) {
			return result;
		}

		const separatorIndex = lines.findIndex(
			(line, index) => index > headerIndex && /[\u2500\-]{3,}/.test(line.replace(/\s+/g, ''))
		);
		const firstDataIndex = separatorIndex >= 0 ? separatorIndex + 1 : headerIndex + 1;
		let current: ServiceInfoEntry | undefined;
		const entries: ServiceInfoEntry[] = [];

		for (let i = firstDataIndex; i < lines.length; i += 1) {
			const line = lines[i];
			if (!line) {
				continue;
			}
			const trimmedLine = line.trim();
			if (!trimmedLine) {
				continue;
			}
			if (/^[\u2500\-]{3,}$/.test(trimmedLine.replace(/\s+/g, ''))) {
				continue;
			}

			const key = line.slice(keyStart, valueStart).trim();
			const value = valueStart < line.length ? line.slice(valueStart).trim() : '';

			if (key) {
				current = { key, value: this.normalizeServiceDescription(value) };
				entries.push(current);
				continue;
			}

			if (current && value) {
				current.value = this.appendWrappedText(current.value, value);
			}
		}

		result.entries = entries.map((entry) => ({
			key: entry.key,
			value: this.normalizeServiceDescription(entry.value)
		}));
		return result;
	}

	private appendWrappedText(existing: string, addition: string): string {
		const base = this.normalizeServiceDescription(existing);
		const extra = this.normalizeServiceDescription(addition);
		if (!base) {
			return extra;
		}
		if (!extra) {
			return base;
		}
		return `${base} ${extra}`.trim();
	}

	private normalizeServiceDescription(value: string): string {
		if (!value) {
			return '';
		}
		return value.replace(/\s+/g, ' ').trim();
	}

	private parseProjectInfoOutput(output: string): Record<string, unknown> | undefined {
		const cleaned = this.stripAnsiSequences(output);
		const trimmed = cleaned.trim();
		if (!trimmed) {
			return undefined;
		}

		const attempts = new Set<string>();
		attempts.add(trimmed);
		const lines = trimmed.split(/\r?\n/);
		const firstNameIndex = lines.findIndex((line) => line.trim().toLowerCase().startsWith('name:'));
		if (firstNameIndex > 0) {
			const candidate = lines.slice(firstNameIndex).join('\n').trim();
			if (candidate.length > 0) {
				attempts.add(candidate);
			}
		}
		const firstYamlIndex = lines.findIndex((line) => /^[A-Za-z0-9_-]+\s*:\s*/.test(line.trim()));
		if (firstYamlIndex > 0) {
			const candidate = lines.slice(firstYamlIndex).join('\n').trim();
			if (candidate.length > 0) {
				attempts.add(candidate);
			}
		}

		for (const candidate of attempts) {
			const parsed = this.tryParseProjectInfo(candidate);
			if (parsed) {
				return parsed;
			}
			const normalized = this.normalizeProjectInfoYaml(candidate);
			if (normalized !== candidate) {
				const normalizedParsed = this.tryParseProjectInfo(normalized);
				if (normalizedParsed) {
					return normalizedParsed;
				}
			}
		}
		return undefined;
	}

	private buildProjectInfoItems(record: Record<string, unknown>): CciItem[] {
		const entries = Object.entries(record);
		entries.sort((a, b) => {
			if (a[0] === 'name') {
				return -1;
			}
			if (b[0] === 'name') {
				return 1;
			}
			return a[0].localeCompare(b[0]);
		});
		return entries.map(([key, value]) => this.buildProjectInfoItem(key, value));
	}

	private buildProjectInfoItem(
		key: string,
		value: unknown,
		options: { parentIsArray?: boolean } = {}
	): CciItem {
		const contextBase = options.parentIsArray ? 'cumulusci.project.arrayItem' : 'cumulusci.project.field';

		if (this.isPlainObject(value)) {
			const childEntries = Object.entries(value as Record<string, unknown>);
			if (childEntries.length === 0) {
				return this.buildProjectLeafItem(key, '(empty object)', contextBase, value);
			}
			childEntries.sort((a, b) => a[0].localeCompare(b[0]));
			const children = childEntries.map(([childKey, childValue]) =>
				this.buildProjectInfoItem(childKey, childValue)
			);
			return {
				label: key,
				itemKind: 'group',
				contextValue: `${contextBase}.group`,
				collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
				iconPath: new vscode.ThemeIcon('symbol-structure'),
				children,
				data: { key, value }
			};
		}

		if (Array.isArray(value)) {
			if (value.length === 0) {
				return this.buildProjectLeafItem(key, '(empty list)', contextBase, value);
			}
			const children = value.map((entry, index) =>
				this.buildProjectInfoItem(`Item ${index + 1}`, entry, { parentIsArray: true })
			);
			return {
				label: key,
				description: `${value.length} item${value.length === 1 ? '' : 's'}`,
				itemKind: 'group',
				contextValue: `${contextBase}.array`,
				collapsibleState: vscode.TreeItemCollapsibleState.Collapsed,
				iconPath: new vscode.ThemeIcon('list-unordered'),
				children,
				data: { key, value }
			};
		}

		return this.buildProjectLeafItem(key, value, contextBase, value);
	}

	private buildProjectLeafItem(
		key: string,
		value: unknown,
		contextBase: string,
		rawValue: unknown
	): CciItem {
		const { display, tooltip } = this.formatProjectScalarValue(value);
		const label = `${key}: ${display}`;
		return {
			label,
			tooltip: tooltip ? `${key}: ${tooltip}` : label,
			itemKind: 'message',
			contextValue: `${contextBase}.leaf`,
			iconPath: new vscode.ThemeIcon('symbol-string'),
			data: { key, value: rawValue }
		};
	}

	private tryParseProjectInfo(text: string): Record<string, unknown> | undefined {
		try {
			const parsed = loadYaml(text) as unknown;
			if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
				return parsed as Record<string, unknown>;
			}
		} catch (error) {
			// continue trying other attempts
		}
		return undefined;
	}

	private normalizeProjectInfoYaml(source: string): string {
		const lines = source.split(/\r?\n/);
		const normalized = lines.map((line) => {
			const match = line.match(/^(\s*[^:#\n]+?):\s*(.*)$/);
			if (!match) {
				return line;
			}
			const [, key, valuePart] = match;
			if (valuePart.length === 0) {
				return line;
			}
			const trimmedValue = valuePart.trim();
			if (trimmedValue.length === 0) {
				return `${key}: ''`;
			}
			if (/^[>|-]/.test(trimmedValue)) {
				return line;
			}
			if (/^["']/.test(trimmedValue)) {
				return line;
			}
			if (trimmedValue.endsWith(':') && !trimmedValue.includes(' ')) {
				return line;
			}
			if (/^\d+\s*:$/.test(trimmedValue)) {
				return line;
			}
			const needsQuoting = /[:#%]/.test(trimmedValue) || /^https?:/i.test(trimmedValue);
			if (!needsQuoting) {
				return line;
			}
			const escaped = trimmedValue.replace(/\\/g, '\\\\').replace(/"/g, '\\"');
			return `${key}: "${escaped}"`;
		});
		return normalized.join('\n');
	}

	private formatProjectScalarValue(value: unknown): { display: string; tooltip: string } {
		if (value === null) {
			return { display: 'null', tooltip: 'null' };
		}
		if (value === undefined) {
			return { display: '–', tooltip: 'No value set' };
		}
		if (typeof value === 'string') {
			const trimmed = value.trim();
			if (trimmed.length === 0) {
				return { display: '–', tooltip: 'Empty string' };
			}
			return {
				display: trimmed.replace(/\r?\n/g, ' ⏎ '),
				tooltip: trimmed
			};
		}
		if (typeof value === 'number' || typeof value === 'boolean') {
			const text = String(value);
			return { display: text, tooltip: text };
		}
		return {
			display: JSON.stringify(value),
			tooltip: JSON.stringify(value, null, 2)
		};
	}

	private stripAnsiSequences(value: string): string {
		return value.replace(/\u001b\[[0-9;?]*[A-Za-z]/g, '');
	}

	private describeWorkspace(folder: vscode.WorkspaceFolder): string {
		const name = folder.name || folder.uri.fsPath;
		return `[${name}]`;
	}

	private logCommandResult(label: string, stdout: string, durationMs: number): void {
		const trimmed = this.stripAnsiSequences(stdout).trim();
		if (trimmed.length > 0) {
			const lines = trimmed.split(/\r?\n/);
			const limit = 20;
			for (const line of lines.slice(0, limit)) {
				this.output.appendLine(`${label}> ${line}`);
			}
			if (lines.length > limit) {
				this.output.appendLine(`${label}> … (${lines.length - limit} more lines)`);
			}
		}
		const seconds = Math.max(durationMs / 1000, 0).toFixed(1);
		this.output.appendLine(`${label}✓ Completed in ${seconds}s`);
	}

	private isPlainObject(value: unknown): value is Record<string, unknown> {
		return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
	}

	private async findCciConfigUri(folder: vscode.WorkspaceFolder): Promise<vscode.Uri | undefined> {
		for (const candidate of this.configCandidates) {
			const fileUri = vscode.Uri.joinPath(folder.uri, ...candidate.split('/'));
			try {
				await access(fileUri.fsPath, fsConstants.R_OK);
				return fileUri;
			} catch (error) {
				// continue searching
			}
		}
		return undefined;
	}

	private async getOrgDefinitions(folder: vscode.WorkspaceFolder): Promise<Map<string, OrgDefinitionMeta>> {
		const definitions = new Map<string, OrgDefinitionMeta>();
		for (const meta of await this.collectOrgsDirectoryDefinitions(folder)) {
			this.mergeOrgDefinition(definitions, meta);
		}
		for (const meta of await this.collectConfigScratchDefinitions(folder)) {
			this.mergeOrgDefinition(definitions, meta);
		}
		return definitions;
	}

	private mergeOrgDefinition(target: Map<string, OrgDefinitionMeta>, meta: OrgDefinitionMeta): void {
		const key = aliasKey(meta.alias);
		const existing = target.get(key);
		if (existing) {
			if (!existing.configPath && meta.configPath) {
				existing.configPath = meta.configPath;
			}
			existing.isScratch = existing.isScratch ?? meta.isScratch;
			meta.sources.forEach((source) => existing.sources.add(source));
			return;
		}
		target.set(key, {
			alias: normalizeAlias(meta.alias),
			configPath: meta.configPath,
			isScratch: meta.isScratch,
			sources: new Set(meta.sources)
		});
	}

	private async collectOrgsDirectoryDefinitions(folder: vscode.WorkspaceFolder): Promise<OrgDefinitionMeta[]> {
		const results: OrgDefinitionMeta[] = [];
		const orgsUri = vscode.Uri.joinPath(folder.uri, 'orgs');
		try {
			const entries = await vscode.workspace.fs.readDirectory(orgsUri);
			for (const [name, type] of entries) {
				if (type !== vscode.FileType.File) {
					continue;
				}
				const match = name.match(/^(.*)\.(ya?ml|json)$/i);
				if (!match) {
					continue;
				}
				const alias = normalizeAlias(match[1]);
				if (!alias) {
					continue;
				}
				const relativePath = `orgs/${name}`;
				results.push({
					alias,
					configPath: relativePath,
					isScratch: true,
					sources: new Set<string>([relativePath])
				});
			}
		} catch (error) {
			// Directory may not exist; ignore.
		}
		return results;
	}

	private async collectConfigScratchDefinitions(folder: vscode.WorkspaceFolder): Promise<OrgDefinitionMeta[]> {
		const results: OrgDefinitionMeta[] = [];
		const configUri = await this.findCciConfigUri(folder);
		if (!configUri) {
			return results;
		}

		try {
			const raw = await vscode.workspace.fs.readFile(configUri);
			const text = Buffer.from(raw).toString('utf8');
			const parsed = loadYaml(text) as unknown;
			if (!parsed || typeof parsed !== 'object') {
				return results;
			}
			const orgsSection = (parsed as Record<string, unknown>).orgs;
			if (!orgsSection || typeof orgsSection !== 'object') {
				return results;
			}
			const scratchSection = (orgsSection as Record<string, unknown>).scratch;
			if (!scratchSection) {
				return results;
			}
			const configFileName = configUri.path.split('/').pop() ?? 'cumulusci.yml';
			const configLabel = vscode.workspace.asRelativePath(configUri, false) || configFileName;

			const addDefinition = (alias: string, value: unknown) => {
				const normalizedAlias = normalizeAlias(alias);
				if (!normalizedAlias) {
					return;
				}
				const sources = new Set<string>([configLabel]);
				let configPath: string | undefined;
				if (typeof value === 'string') {
					configPath = value;
				} else if (value && typeof value === 'object') {
					const recordValue = value as Record<string, unknown>;
					const candidate = recordValue.config_file;
					if (typeof candidate === 'string' && candidate.trim().length > 0) {
						configPath = candidate.trim();
					}
				}
				if (configPath) {
					sources.add(configPath);
				}
				results.push({
					alias: normalizedAlias,
					configPath,
					isScratch: true,
					sources
				});
			};

			if (Array.isArray(scratchSection)) {
				for (const entry of scratchSection) {
					if (typeof entry === 'string') {
						addDefinition(entry, undefined);
					} else if (entry && typeof entry === 'object') {
						for (const [alias, value] of Object.entries(entry as Record<string, unknown>)) {
							addDefinition(alias, value);
						}
					}
				}
			} else if (typeof scratchSection === 'object') {
				for (const [alias, value] of Object.entries(scratchSection as Record<string, unknown>)) {
					addDefinition(alias, value);
				}
			}
		} catch (error) {
			// Ignore YAML parsing errors
		}

		return results;
	}

	private icon(...segments: string[]): vscode.Uri {
		return vscode.Uri.joinPath(this.extensionUri, ...segments);
	}

	private async hasCciConfig(folder: vscode.WorkspaceFolder): Promise<boolean> {
		return (await this.findCciConfigUri(folder)) !== undefined;
	}

	private async fetchRecords(
		folder: vscode.WorkspaceFolder,
		baseArgs: string[],
		arrayKey: string
	): Promise<CciFetchResult> {
		try {
			const stdout = await this.runCciCommand(folder, [...baseArgs, '--json']);
			const records = this.parseJsonRecords(stdout, arrayKey);
			if (records.length > 0) {
				return { records, format: 'json' };
			}
		} catch (error) {
			if (!this.didFailDueToJsonFlag(error)) {
				throw error;
			}
		}

		const stdout = await this.runCciCommand(folder, baseArgs);
		return { records: this.parseTextRecords(stdout), format: 'text' };
	}

	private async includeConfigDefinitions(
		records: unknown[],
		folder: vscode.WorkspaceFolder,
		section: 'tasks' | 'flows'
	): Promise<unknown[]> {
		const labelKeys = section === 'tasks' ? this.taskLabelKeys : this.flowLabelKeys;
		const configDefinitions = await this.loadConfigDefinitionsMap(folder, section);
		const enriched: CciRecord[] = [];

		for (const record of records) {
			const normalized = this.normalizeRecord(record, labelKeys);
			const name = normalized ? pickFirstString(normalized, labelKeys) : undefined;
			if (name) {
				const key = name.toLowerCase();
			const config = configDefinitions.get(key);
			if (config && normalized) {
				this.mergeConfigDefinition(normalized, config);
				configDefinitions.delete(key);
			}
			}
			if (normalized) {
				enriched.push(normalized);
			}
		}

		for (const config of configDefinitions.values()) {
			enriched.push({ ...config });
		}

		return enriched;
	}

	private async loadProjectConfigSection(
		folder: vscode.WorkspaceFolder,
		section: 'tasks' | 'flows'
	): Promise<CciRecord[]> {
		const uri = await this.findCciConfigUri(folder);
		if (!uri) {
			return [];
		}
		return this.loadConfigSectionFromSource(uri, section, 'project');
	}

	private async loadGlobalConfigSection(section: 'tasks' | 'flows'): Promise<CciRecord[]> {
		const configPath = path.join(os.homedir(), '.cumulusci', 'cumulusci.yml');
		try {
			await access(configPath, fsConstants.R_OK);
		} catch (error) {
			return [];
		}
		const uri = vscode.Uri.file(configPath);
		return this.loadConfigSectionFromSource(uri, section, 'global');
	}

	private async loadConfigSectionFromSource(
		uri: vscode.Uri,
		section: 'tasks' | 'flows',
		scope: 'project' | 'global'
	): Promise<CciRecord[]> {
		try {
			const raw = await vscode.workspace.fs.readFile(uri);
			const text = Buffer.from(raw).toString('utf8');
			const parsed = loadYaml(text) as unknown;
			if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
				return [];
			}
			const sectionValue = (parsed as Record<string, unknown>)[section];
			return this.convertConfigSectionToRecords(sectionValue, scope);
		} catch (error) {
			return [];
		}
	}

	private convertConfigSectionToRecords(
		sectionValue: unknown,
		scope: 'project' | 'global'
	): CciRecord[] {
		if (!sectionValue || typeof sectionValue !== 'object' || Array.isArray(sectionValue)) {
			return [];
		}
		const records: CciRecord[] = [];
		for (const [name, value] of Object.entries(sectionValue as Record<string, unknown>)) {
			const record: CciRecord = {
				name,
				configSource: scope
			};

			if (typeof value === 'string') {
				record.description = value;
			} else if (value && typeof value === 'object') {
				const obj = value as Record<string, unknown>;
				if (typeof obj.description === 'string') {
					record.description = obj.description;
				}
				if (typeof obj.group === 'string') {
					record.group = obj.group;
				}
				record.configDefinition = obj;
			}
			if (record.group === undefined) {
				record.group = scope === 'global' ? 'Workspace Config' : 'Project Config';
			}
			records.push(record);
		}
		return records;
	}

	private async loadConfigDefinitionsMap(
		folder: vscode.WorkspaceFolder,
		section: 'tasks' | 'flows'
	): Promise<Map<string, CciRecord>> {
		const combined = [
			...(await this.loadProjectConfigSection(folder, section)),
			...(await this.loadGlobalConfigSection(section))
		];
		const map = new Map<string, CciRecord>();
		for (const record of combined) {
			const name = typeof record.name === 'string' ? record.name.trim() : undefined;
			if (!name) {
				continue;
			}
			map.set(name.toLowerCase(), record);
		}
		return map;
	}

	private normalizeRecord(record: unknown, labelKeys: string[]): CciRecord | undefined {
		const existing = toRecord(record);
		if (existing) {
			return { ...existing };
		}
		if (typeof record === 'string') {
			const trimmed = record.trim();
			if (trimmed.length === 0) {
				return undefined;
			}
			return { name: trimmed };
		}
		return undefined;
	}

	private mergeConfigDefinition(target: CciRecord, config: CciRecord): void {
		target.configSource = config.configSource ?? target.configSource;
		if (config.configDefinition) {
			target.configDefinition = config.configDefinition;
		}
		if (config.group && (!target.group || target.group === 'Ungrouped')) {
			target.group = config.group;
		}
		if (config.description && !target.description) {
			target.description = config.description;
		}
	}

	private extractNameFromRecord(record: unknown, labelKeys: string[]): string | undefined {
		const objectRecord = toRecord(record);
		if (objectRecord) {
			const name = pickFirstString(objectRecord, labelKeys);
			if (name) {
				return name;
			}
		}
		if (typeof record === 'string') {
			const trimmed = record.trim();
			if (trimmed.length > 0) {
				return trimmed;
			}
		}
		return undefined;
	}

	private parseJsonRecords(stdout: string, arrayKey: string): unknown[] {
		const trimmed = stdout.trim();
		if (!trimmed) {
			return [];
		}

		const attempts: string[] = [trimmed];
		const objectStart = trimmed.indexOf('{');
		const arrayStart = trimmed.indexOf('[');
		const candidates = [objectStart, arrayStart]
			.filter((index) => index >= 0)
			.sort((a, b) => a - b);
		for (const index of candidates) {
			const candidate = trimmed.slice(index).trim();
			if (candidate.length > 0) {
				attempts.push(candidate);
			}
		}

		for (const text of attempts) {
			try {
				const json = JSON.parse(text) as unknown;
				return this.normalizeJsonRecords(json, arrayKey);
			} catch (error) {
				// Try next candidate
			}
		}

		return [];
	}

	private parseJsonObject(stdout: string): CciRecord | undefined {
		const trimmed = stdout.trim();
		if (!trimmed) {
			return undefined;
		}

		const attempts: string[] = [trimmed];
		const firstBrace = trimmed.indexOf('{');
		if (firstBrace > 0) {
			attempts.push(trimmed.slice(firstBrace).trim());
		}

		for (const text of attempts) {
			try {
				const parsed = JSON.parse(text) as unknown;
				const record = toRecord(parsed);
				if (record) {
					return record;
				}
			} catch (error) {
				// ignore and try the next candidate
			}
		}

		return undefined;
	}

	private parseTextRecords(stdout: string): unknown[] {
		return stdout
			.split(/\r?\n/)
			.map((line) => line.trim())
			.filter((line) => line.length > 0 && !/^[-=]+$/.test(line) && !/^Name\s+/i.test(line));
	}

	private normalizeJsonRecords(value: unknown, arrayKey: string): unknown[] {
		if (Array.isArray(value)) {
			return value;
		}

		if (value && typeof value === 'object') {
			const record = value as Record<string, unknown>;
			const preferred = record[arrayKey];
			if (Array.isArray(preferred)) {
				return preferred;
			}
			if (preferred && typeof preferred === 'object' && !Array.isArray(preferred)) {
				return this.mapEntriesToRecords(preferred as Record<string, unknown>);
			}

			for (const entry of Object.values(record)) {
				if (Array.isArray(entry)) {
					return entry;
				}
			}

			if (this.isAliasRecordMap(record)) {
				return this.mapEntriesToRecords(record);
			}

			return [record];
		}

		return [];
	}

	private isAliasRecordMap(map: Record<string, unknown>): boolean {
		const entries = Object.values(map);
		if (entries.length === 0) {
			return false;
		}
		return entries.every((entry) => entry && typeof entry === 'object' && !Array.isArray(entry));
	}

	private mapEntriesToRecords(map: Record<string, unknown>): unknown[] {
		return Object.entries(map).map(([key, value]) => {
			if (value && typeof value === 'object' && !Array.isArray(value)) {
				const recordValue: Record<string, unknown> = {
					...(value as Record<string, unknown>)
				};
				const aliasFromValue = typeof recordValue.alias === 'string' ? recordValue.alias.trim() : '';
				const alias = aliasFromValue.length > 0 ? aliasFromValue : key;
				recordValue.alias = alias;
				if (typeof recordValue.name !== 'string' || recordValue.name.trim().length === 0) {
					recordValue.name = alias;
				}
				const configValue = typeof recordValue.config === 'string' && recordValue.config.trim().length > 0
					? recordValue.config.trim()
					: undefined;
				if (recordValue.config_name === undefined) {
					recordValue.config_name = configValue ?? alias;
				}
				if (recordValue.key === undefined) {
					recordValue.key = alias;
				}
				if (recordValue.orgCreated === undefined && typeof recordValue.expired === 'boolean') {
					recordValue.orgCreated = !recordValue.expired;
				}
				return recordValue;
			}

			return { alias: key, value };
		});
	}

	private didFailDueToJsonFlag(error: unknown): boolean {
		const message = error instanceof Error ? error.message : String(error);
		return /--json/i.test(message) || /no such option/i.test(message);
	}

	private async getOrgEntries(folder: vscode.WorkspaceFolder): Promise<OrgEntry[]> {
		const { records } = await this.fetchRecords(folder, ['org', 'list'], 'orgs');
		const definitions = await this.getOrgDefinitions(folder);
		const merged = new Map<string, CciRecord>();

		for (const record of records) {
			const base = toRecord(record);
			let working: CciRecord | undefined;
			if (base) {
				working = { ...base };
			} else if (typeof record === 'string' && record.trim().length > 0) {
				const aliasText = normalizeAlias(record.trim());
				if (aliasText.length === 0) {
					continue;
				}
				working = { alias: aliasText, name: aliasText };
			}
			if (!working) {
				continue;
			}

			let aliasValue = pickFirstString(working, this.orgLabelKeys);
			if (!aliasValue && typeof working.alias === 'string') {
				aliasValue = working.alias;
			}
			if (!aliasValue && typeof working.name === 'string') {
				aliasValue = working.name;
			}
			if (!aliasValue) {
				continue;
			}
			const normalizedAlias = normalizeAlias(aliasValue);
			working.alias = normalizedAlias;
			if (typeof working.name !== 'string' || working.name.trim().length === 0) {
				working.name = normalizedAlias;
			}
			if (working.config_name === undefined) {
				working.config_name = typeof working.config === 'string' ? working.config : normalizedAlias;
			}

			const key = aliasKey(normalizedAlias);
			const definition = definitions.get(key);
			const sourceSet = new Set<string>(toStringArray(working.definitionSources));
			if (definition) {
				if (!working.config && definition.configPath) {
					working.config = definition.configPath;
				}
				if (definition.isScratch !== undefined) {
					working.is_scratch = definition.isScratch;
				}
				definition.sources.forEach((source) => sourceSet.add(source));
				definitions.delete(key);
			} else if (working.is_scratch === true) {
				working.definitionMissing = true;
			}
			working.definitionOnly = false;
			if (sourceSet.size > 0) {
				working.definitionSources = Array.from(sourceSet);
			} else {
				delete working.definitionSources;
			}
			merged.set(key, working);
		}

		for (const definition of definitions.values()) {
			const normalizedAlias = normalizeAlias(definition.alias);
			if (!normalizedAlias) {
				continue;
			}
			const key = aliasKey(normalizedAlias);
			if (merged.has(key)) {
				continue;
			}
			const sources = Array.from(definition.sources);
			const stub: CciRecord = {
				alias: normalizedAlias,
				name: normalizedAlias,
				config: definition.configPath,
				config_name: definition.configPath ?? normalizedAlias,
				is_scratch: definition.isScratch ?? true,
				orgCreated: false,
				expired: false,
				definitionOnly: true
			};
			if (sources.length > 0) {
				stub.definitionSources = sources;
			}
			merged.set(key, stub);
		}

		const entries: OrgEntry[] = [];
		for (const record of merged.values()) {
			const entry = this.createOrgEntry(record);
			if (entry) {
				entries.push(entry);
			}
		}

		entries.sort((a, b) => a.alias.localeCompare(b.alias));
		return entries;
	}

	private createOrgEntry(record: CciRecord): OrgEntry | undefined {
		const data: CciRecord = { ...record };
		const alias = pickFirstString(data, this.orgLabelKeys) ?? pickFirstString(data, Object.keys(data));
		if (!alias) {
			return undefined;
		}
		data.alias = alias;
		if (typeof data.name !== 'string' || data.name.trim().length === 0) {
			data.name = alias;
		}
		const configName = pickFirstString(data, ['config', 'config_name']);
		if (data.config_name === undefined) {
			data.config_name = configName ?? alias;
		}
		const isDefault = getBoolean(data, ['is_default', 'default', 'isDefault']) ?? false;
		const isScratch = getBoolean(data, ['scratch', 'scratch_org', 'is_scratch', 'isScratch']) ?? false;
		const expired = getBoolean(data, ['expired']) ?? false;
		const baseActive = getBoolean(data, ['orgCreated', 'active', 'is_active']);
		const dayCount = pickFirstString(data, ['days']);
		const domain = pickFirstString(data, ['domain', 'instance_url', 'login_url']);
		const definitionSources = toStringArray(data.definitionSources);
		const definitionOnly = typeof data.definitionOnly === 'boolean' ? data.definitionOnly : false;
		const definitionMissing = getBoolean(data, ['definitionMissing']) ?? false;

		const hasDomain = typeof domain === 'string' && domain.length > 0;
		let orgCreated: boolean;
		if (definitionMissing || definitionOnly) {
			orgCreated = false;
		} else if (isScratch) {
			orgCreated = baseActive ?? (hasDomain && !expired);
		} else {
			orgCreated = baseActive ?? hasDomain;
		}
		if (!hasDomain) {
			orgCreated = false;
		}
		data.orgCreated = orgCreated;
		data.expired = expired;
		data.definitionOnly = definitionOnly;
		if (definitionSources.length > 0) {
			data.definitionSources = definitionSources;
		}
		if (definitionMissing) {
			data.definitionMissing = true;
		}

		const description = this.buildOrgDescription({
			isScratch,
			expired,
			isDefault,
			dayCount,
			orgCreated,
			definitionOnly,
			definitionMissing
		});
		const tooltip = this.createOrgTooltip({
			alias,
			configName,
			isDefault,
			isScratch,
			expired,
			dayCount,
			domain,
			data,
			orgCreated,
			definitionSources,
			definitionOnly,
			definitionMissing
		});

		const contextValue = definitionMissing ? 'cumulusci.org.disabled' : 'cumulusci.org';
		const command = definitionMissing
			? undefined
			: { id: 'cumulusci.org.showActions', title: 'Org Actions' };

		return {
			alias,
			data,
			description,
			tooltip,
			contextValue,
			iconPath: this.getOrgIcon(data),
			command,
			isScratch,
			orgCreated,
			expired,
			definitionOnly,
			definitionMissing
		};
	}

	private orgEntryToTreeItem(folder: vscode.WorkspaceFolder, entry: OrgEntry): CciItem {
		return {
			label: entry.alias,
			description: entry.description,
			tooltip: entry.tooltip,
			contextValue: entry.contextValue,
			itemKind: 'org',
			workspaceFolder: folder,
			data: entry.data,
			iconPath: entry.iconPath,
			command: entry.command
		};
	}

	private describeOrgEntry(entry: OrgEntry): string {
		const parts: string[] = [];
		parts.push(entry.isScratch ? 'Scratch org' : 'Connected org');
		if (entry.definitionMissing) {
			parts.push('Missing definition');
		} else if (entry.definitionOnly) {
			parts.push('Definition only (will create on run)');
		} else if (entry.isScratch) {
			parts.push(entry.orgCreated ? 'Active' : 'Not created');
			if (entry.expired) {
				parts.push('Expired');
			}
		} else {
			parts.push(entry.orgCreated ? 'Ready' : 'Unavailable');
		}
		const instanceUrl = pickFirstString(entry.data, ['instance_url', 'domain', 'login_url']);
		if (instanceUrl) {
			parts.push(instanceUrl);
		}
		return parts.join(' • ');
	}

	private buildOrgDescription(options: {
		readonly isScratch: boolean;
		readonly expired: boolean;
		readonly isDefault: boolean;
		readonly dayCount?: string;
		readonly orgCreated: boolean;
		readonly definitionOnly: boolean;
		readonly definitionMissing: boolean;
	}): string | undefined {
		const parts: string[] = [];
		if (options.isScratch) {
			if (options.definitionMissing) {
				parts.push('Scratch definition missing');
			} else if (options.definitionOnly) {
				parts.push('Scratch (definition)');
			} else if (!options.orgCreated) {
				parts.push(options.expired ? 'Scratch (expired)' : 'Scratch (not created)');
			} else {
				parts.push(options.expired ? 'Scratch (expired)' : 'Scratch');
			}
			if (options.orgCreated && !options.expired) {
				const formatted = this.formatScratchDayDisplay(options.dayCount);
				if (formatted) {
					parts.push(formatted);
				}
			}
		} else {
			parts.push(options.orgCreated ? 'Connected' : 'Connected (not linked)');
		}
		if (options.isDefault) {
			parts.push('Default');
		}
		return parts.length > 0 ? parts.join(' • ') : undefined;
	}

	private createOrgTooltip(options: {
		readonly alias: string;
		readonly configName?: string;
		readonly isDefault: boolean;
		readonly isScratch: boolean;
		readonly expired: boolean;
		readonly dayCount?: string;
		readonly domain?: string;
		readonly data: CciRecord;
		readonly orgCreated: boolean;
		readonly definitionSources: string[];
		readonly definitionOnly: boolean;
		readonly definitionMissing: boolean;
	}): vscode.MarkdownString {
		const tooltip = new vscode.MarkdownString();
		tooltip.isTrusted = false;
		tooltip.appendMarkdown(`**${options.alias}**\n\n`);
		const lines: string[] = [
			`- Config: ${options.configName ?? 'n/a'}`,
			`- Default org: ${options.isDefault ? 'Yes' : 'No'}`,
			`- Type: ${options.isScratch ? 'Scratch org' : 'Connected org'}`,
			`- Created: ${options.orgCreated ? 'Yes' : 'No'}`
		];
		if (options.isScratch) {
			if (options.definitionMissing) {
				lines.push('- Scratch status: Definition missing');
			} else if (options.definitionOnly) {
				lines.push('- Scratch status: Definition only');
			} else {
				lines.push(`- Scratch status: ${options.expired ? 'Expired' : options.orgCreated ? 'Active' : 'Not created'}`);
				if (options.orgCreated && !options.expired) {
					const formattedDays = this.formatScratchDayDisplay(options.dayCount);
					if (formattedDays) {
						lines.push(`- ${formattedDays}`);
					}
				}
			}
		}
		if (options.domain && options.domain.trim().length > 0) {
			lines.push(`- Domain: ${options.domain}`);
		}
		if (options.definitionMissing) {
			lines.push('- Definition status: Missing local definition file');
		}
		if (options.definitionSources.length > 0) {
			lines.push(`- Defined in: ${options.definitionSources.join(', ')}`);
		}
		tooltip.appendMarkdown(lines.join('\n'));
		tooltip.appendMarkdown('\n');
		tooltip.appendCodeblock(JSON.stringify(options.data, null, 2), 'json');
		return tooltip;
	}

	private formatScratchDayDisplay(value: string | undefined): string | undefined {
		if (!value) {
			return undefined;
		}
		const trimmed = value.trim();
		if (!trimmed) {
			return undefined;
		}
		const { remaining, total } = parseScratchDayCount(trimmed);
		const summary = formatScratchDaySummary(remaining, total);
		return summary ?? trimmed;
	}

	private buildGroupedItems(
		folder: vscode.WorkspaceFolder,
		records: unknown[],
		options: {
			labelKeys: string[];
			descriptionKeys: string[];
			itemKind: CciItemKind;
			contextValue: string;
			command: TreeCommandSpec;
			iconPath: IconDefinition;
		}
	): CciItem[] {
		const groups = new Map<string, CciItem[]>();

		for (const record of records) {
			const data = toRecord(record);
			const label = data
				? pickFirstString(data, options.labelKeys) ?? pickFirstString(data, Object.keys(data)) ?? 'Unnamed'
				: typeof record === 'string'
					? record
					: 'Unnamed';

			const description = data ? pickFirstString(data, options.descriptionKeys) ?? undefined : undefined;
			const configSource = data && typeof data.configSource === 'string' ? data.configSource : undefined;
			const sourceDescription = configSource === 'project'
				? 'Project config'
				: configSource === 'global'
					? 'Workspace config'
					: undefined;
			const combinedDescription = [description, sourceDescription]
				.filter((part) => part && part.length > 0)
				.join(' • ');
			const tooltip = data ? createTooltip(data) : undefined;

			const iconPath = configSource === 'project'
				? new vscode.ThemeIcon('notebook')
				: configSource === 'global'
					? new vscode.ThemeIcon('globe')
					: options.iconPath;
			const item: CciItem = {
				label,
				description: combinedDescription.length > 0 ? combinedDescription : undefined,
				tooltip,
				contextValue: options.contextValue,
				itemKind: options.itemKind,
				workspaceFolder: folder,
				data,
				iconPath,
				command: options.command
			};

			let groupKey: string;
			if (configSource === 'project' || configSource === 'global') {
				// groupKey = configSource === 'project' ? 'Local Config' : 'Workspace Config';
				// based on itemKind we say Local Kind or Workspace Kind
				groupKey = configSource === 'project' ? `Local ${options.itemKind}s` : `Workspace ${options.itemKind}s`;
			} else {
				const groupName = data ? pickFirstString(data, this.groupKeys) : undefined;
				groupKey = groupName && groupName.trim().length > 0 ? groupName.trim() : 'Ungrouped';
			}
			if (!groups.has(groupKey)) {
				groups.set(groupKey, []);
			}
			groups.get(groupKey)?.push(item);
		}

		if (groups.size === 1 && groups.has('Ungrouped')) {
			return (groups.get('Ungrouped') ?? []).sort((a, b) => a.label.localeCompare(b.label));
		}

		// const orderedCategories = ['Local Config', 'Workspace Config'];
		// dynamic based on itemKind categories
		const orderedCategories = [`Local ${options.itemKind}s`, `Workspace ${options.itemKind}s`];
		return Array.from(groups.entries())
			.sort(([a], [b]) => {
				const indexA = orderedCategories.indexOf(a);
				const indexB = orderedCategories.indexOf(b);
				if (indexA !== -1 || indexB !== -1) {
					if (indexA === -1) {
						return 1;
					}
					if (indexB === -1) {
						return -1;
					}
					return indexA - indexB;
				}
				return a.localeCompare(b);
			})
			.map(([label, children]) => ({
				label,
				collapsibleState: vscode.TreeItemCollapsibleState.Expanded,
				contextValue: 'cumulusci.group',
				itemKind: 'group',
				children: children.sort((a, b) => a.label.localeCompare(b.label)),
				workspaceFolder: folder
			}));
	}

	private getOrgIcon(data: CciRecord): IconDefinition {
		if (getBoolean(data, ['definitionMissing']) === true) {
			return new vscode.ThemeIcon('circle-slash');
		}
		const isScratch = getBoolean(data, ['scratch', 'scratch_org', 'is_scratch', 'isScratch']) ?? false;
		if (isScratch) {
			const expired = getBoolean(data, ['expired']);
			const active = getBoolean(data, ['orgCreated', 'active', 'is_active']);
			const missingDefinition = getBoolean(data, ['definitionMissing']);
			if (expired === true || active === false) {
				return this.icons.scratchInactive;
			}
			if (missingDefinition) {
				return this.icons.scratchInactive;
			}
			return this.icons.scratchActive;
		}

		if (getBoolean(data, ['connected', 'is_connected']) === true) {
			return this.icons.connected;
		}

		return this.icons.orgDefault;
	}
}

function toRecord(value: unknown): CciRecord | undefined {
	if (!value || typeof value !== 'object' || Array.isArray(value)) {
		return undefined;
	}
	return value as CciRecord;
}

function pickFirstString(record: CciRecord | undefined, keys: string[]): string | undefined {
	if (!record) {
		return undefined;
	}

	for (const key of keys) {
		const value = record[key];
		if (typeof value === 'string' && value.trim().length > 0) {
			return value.trim();
		}
	}
	return undefined;
}

function normalizeAlias(alias: string): string {
	return alias.trim();
}

function aliasKey(alias: string): string {
	return normalizeAlias(alias).toLowerCase();
}

function toStringArray(value: unknown): string[] {
	if (Array.isArray(value)) {
		return value
			.map((entry) => (typeof entry === 'string' ? entry.trim() : ''))
			.filter((entry) => entry.length > 0);
	}
	if (typeof value === 'string' && value.trim().length > 0) {
		return [value.trim()];
	}
	return [];
}

function getBoolean(record: CciRecord | undefined, keys: string[]): boolean | undefined {
	if (!record) {
		return undefined;
	}

	for (const key of keys) {
		const value = record[key];
		if (typeof value === 'boolean') {
			return value;
		}
		if (typeof value === 'string') {
			const normalized = value.trim().toLowerCase();
			if (['true', 'yes', '1'].includes(normalized)) {
				return true;
			}
			if (['false', 'no', '0'].includes(normalized)) {
				return false;
			}
		}
		if (typeof value === 'number') {
			return value !== 0;
		}
	}
	return undefined;
}

function toBoolean(record: CciRecord | undefined, keys: string[]): boolean {
	if (!record) {
		return false;
	}

	for (const key of keys) {
		const value = record[key];
		if (typeof value === 'boolean') {
			return value;
		}
		if (typeof value === 'string') {
			const normalized = value.trim().toLowerCase();
			if (['true', 'yes', '1'].includes(normalized)) {
				return true;
			}
		}
		if (typeof value === 'number') {
			if (value !== 0) {
				return true;
			}
		}
	}
	return false;
}

function createTooltip(data: CciRecord): vscode.MarkdownString {
	const tooltip = new vscode.MarkdownString();
	tooltip.isTrusted = false;
	tooltip.appendCodeblock(JSON.stringify(data, null, 2), 'json');
	return tooltip;
}


function formatOrgInfoMarkdown(alias: string, data: CciRecord): string {
	const summaryRows: Array<[string, string]> = [];
	const username = pickFirstString(data, ['username', 'user', 'email_address']);
	if (username) {
		summaryRows.push(['Username', username]);
	}
	const instanceUrl = pickFirstString(data, ['instance_url', 'domain', 'login_url']);
	if (instanceUrl) {
		summaryRows.push(['Instance URL', instanceUrl]);
	}
	const orgId = pickFirstString(data, ['org_id', 'id']);
	if (orgId) {
		summaryRows.push(['Org ID', orgId]);
	}
	const configFile = pickFirstString(data, ['config_file', 'config']);
	if (configFile) {
		summaryRows.push(['Definition File', configFile]);
	}
	const orgType = pickFirstString(data, ['org_type']);
	if (orgType) {
		summaryRows.push(['Org Type', orgType]);
	}
	const scratch = getBoolean(data, ['scratch', 'is_scratch']);
	if (scratch !== undefined) {
		summaryRows.push(['Scratch Org', formatBoolean(scratch)]);
	}
	const created = getBoolean(data, ['created', 'orgCreated']);
	if (created !== undefined) {
		summaryRows.push(['Created', formatBoolean(created)]);
	}
	const days = toDisplayValue(data['days']);
	if (days) {
		summaryRows.push(['Days Remaining', days]);
	}
	const dateCreated = pickFirstString(data, ['date_created']);
	if (dateCreated) {
		summaryRows.push(['Date Created', dateCreated]);
	}
	const sfdxAlias = pickFirstString(data, ['sfdx_alias']);
	if (sfdxAlias) {
		summaryRows.push(['SFDX Alias', sfdxAlias]);
	}
	const namespace = pickFirstString(data, ['namespace']);
	if (namespace) {
		summaryRows.push(['Namespace', namespace]);
	}

	const lines: string[] = [`# Org: ${escapeMarkdown(alias)}`, ''];
	if (summaryRows.length > 0) {
		lines.push('## Summary', '', '| Field | Value |', '| --- | --- |');
		for (const [field, value] of summaryRows) {
			lines.push(`| ${escapeMarkdown(field)} | ${escapeMarkdown(value)} |`);
		}
		lines.push('');
	}

	lines.push('## Raw JSON', '', '```json');
	lines.push(JSON.stringify(data, null, 2));
	lines.push('```');

	return lines.join('\n');
}

function formatServiceInfoMarkdown(
	serviceLabel: string,
	serviceName: string | undefined,
	info: ServiceInfoResult
): string {
	const titleParts = [serviceLabel];
	if (serviceName) {
		titleParts.push(serviceName);
	}
	const heading = titleParts.join(' • ');
	const lines: string[] = [`# Service: ${escapeMarkdown(heading)}`, ''];
	if (info.title && info.title !== heading) {
		lines.push(`_CCI reference:_ ${escapeMarkdown(info.title)}`, '');
	}
	if (info.entries.length === 0) {
		lines.push('_No attributes reported by CCI._');
		return lines.join('\n');
	}
	lines.push('| Field | Value |', '| --- | --- |');
	for (const entry of info.entries) {
		lines.push(`| ${escapeMarkdown(entry.key)} | ${escapeMarkdown(entry.value)} |`);
	}
	return lines.join('\n');
}

function escapeMarkdown(value: string): string {
	return value.replace(/[|\\`*_{}\[\]()#+\-!.]/g, (match) => `\\${match}`).replace(/\n/g, '<br/>');
}

function formatBoolean(value: boolean): string {
	return value ? 'Yes' : 'No';
}

function pluralizeDay(count: number): string {
	return count === 1 ? 'day' : 'days';
}

function parseScratchDayCount(value: string | undefined): {
	remaining?: number;
	total?: number;
	used?: number;
} {
	if (!value) {
		return {};
	}
	const trimmed = value.trim();
	if (!trimmed) {
		return {};
	}
	const fractionMatch = trimmed.match(/^(\d+)\s*\/\s*(\d+)$/);
	if (fractionMatch) {
		const used = Number.parseInt(fractionMatch[1], 10);
		const total = Number.parseInt(fractionMatch[2], 10);
		if (Number.isFinite(used) && Number.isFinite(total) && total > 0) {
			return { used, total, remaining: Math.max(total - used, 0) };
		}
	}
	const numeric = Number(trimmed);
	if (Number.isFinite(numeric)) {
		const total = Math.max(0, Math.floor(numeric));
		return { total, remaining: total };
	}
	return {};
}

function formatScratchDaySummary(remaining?: number, total?: number): string | undefined {
	if (total === undefined && remaining === undefined) {
		return undefined;
	}
	if (total === undefined) {
		if (remaining === undefined) {
			return undefined;
		}
		return `${remaining} ${pluralizeDay(remaining)} left`;
	}
	if (total === 0) {
		return 'Expired';
	}
	if (remaining === undefined) {
		return `${total} ${pluralizeDay(total)} left`;
	}
	if (remaining === total) {
		return `${remaining} ${pluralizeDay(remaining)} left`;
	}
	return `${remaining}/${total} ${pluralizeDay(total)} left`;
}

function formatServiceTypeLabel(type: string): string {
	const trimmed = type.trim();
	if (!trimmed) {
		return 'Service';
	}
	const parts = trimmed.split(/[_-]+/).filter((part) => part.length > 0);
	if (parts.length === 0) {
		return capitalizeServiceSegment(trimmed);
	}
	return parts.map(capitalizeServiceSegment).join(' ');
}

function capitalizeServiceSegment(segment: string): string {
	const lower = segment.toLowerCase();
	switch (lower) {
		case 'oauth2':
			return 'OAuth2';
		case 'github':
			return 'GitHub';
		case 'devhub':
			return 'Dev Hub';
		case 'metaci':
			return 'MetaCI';
		case 'metadeploy':
			return 'MetaDeploy';
		case 'sfdx':
			return 'SFDX';
		default:
			return segment.charAt(0).toUpperCase() + segment.slice(1);
	}
}

function toDisplayValue(value: unknown): string | undefined {
	if (value === undefined || value === null) {
		return undefined;
	}
	if (typeof value === 'string') {
		const trimmed = value.trim();
		return trimmed.length > 0 ? trimmed : undefined;
	}
	if (typeof value === 'number' || typeof value === 'boolean') {
		return String(value);
	}
	if (Array.isArray(value)) {
		return value
			.map((entry) => toDisplayValue(entry) ?? '')
			.filter((entry) => entry.length > 0)
			.join(', ');
	}
	if (typeof value === 'object') {
		return JSON.stringify(value);
	}
	return undefined;
}

function parseTaskOptionArgs(input: string): string[] {
	const entries = input
		.split(/\r?\n/)
		.flatMap((line) => line.split(','))
		.map((entry) => entry.trim())
		.filter((entry) => entry.length > 0);
	const args: string[] = [];
	for (const entry of entries) {
		args.push('-o');
		args.push(entry);
	}
	return args;
}

function splitShellArgs(input: string): string[] {
	const trimmed = input.trim();
	if (!trimmed) {
		return [];
	}
	const regex = /"([^"\\]*(?:\\.[^"\\]*)*)"|'([^'\\]*(?:\\.[^'\\]*)*)'|[^\s]+/g;
	const args: string[] = [];
	let match: RegExpExecArray | null;
	while ((match = regex.exec(trimmed)) !== null) {
		if (match[1] !== undefined) {
			args.push(match[1].replace(/\\"/g, '"'));
			continue;
		}
		if (match[2] !== undefined) {
			args.push(match[2].replace(/\\'/g, "'"));
			continue;
		}
		args.push(match[0]);
	}
	return args;
}

function quoteArg(arg: string): string {
	if (!arg) {
		return '""';
	}
	if (/[^A-Za-z0-9_@%+=:,./-]/.test(arg)) {
		return `"${arg.replace(/(["\\$`])/g, '\\$1')}"`;
	}
	return arg;
}

function resolveTreeItem(target: unknown): CciTreeItem | undefined {
	if (target instanceof CciTreeItem) {
		return target;
	}
	if (Array.isArray(target) && target[0] instanceof CciTreeItem) {
		return target[0];
	}
	return undefined;
}

function requireWorkspaceFolder(item: CciTreeItem): vscode.WorkspaceFolder {
	const folder = item.workspaceFolder;
	if (!folder) {
		throw new Error('Workspace folder not found for this item.');
	}
	return folder;
}

function ensureItemKind(item: CciTreeItem, expected: CciItemKind, commandName: string): void {
	if (item.itemKind !== expected) {
		throw new Error(`${commandName} can only be used from a ${expected} item.`);
	}
}

function toTreeItemLabelString(label: string | vscode.TreeItemLabel | undefined): string {
	if (!label) {
		return '';
	}
	if (typeof label === 'string') {
		return label;
	}
	return label.label;
}

function toDescriptionString(
	description: string | boolean | vscode.TreeItemLabel | undefined
): string | undefined {
	if (typeof description === 'string') {
		return description;
	}
	if (typeof description === 'boolean') {
		return description ? 'true' : 'false';
	}
	if (description && typeof description === 'object') {
		return description.label;
	}
	return undefined;
}

export function activate(context: vscode.ExtensionContext) {
	const output = vscode.window.createOutputChannel('CumulusCI');
	context.subscriptions.push(output);

	const service = new CciService(context.extensionUri, output);

	const defaultOrgStatus = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Left, 100);
	defaultOrgStatus.name = 'CumulusCI Default Org';
	defaultOrgStatus.command = 'cumulusci.status.showDefaultOrg';
	defaultOrgStatus.text = '$(organization) Loading…';
	defaultOrgStatus.tooltip = 'Resolving default CCI org…';
	defaultOrgStatus.show();
	context.subscriptions.push(defaultOrgStatus);

	const expiryStateKey = 'cumulusci.expiryNotified';

	const handleExpiryNotification = async (folder: vscode.WorkspaceFolder, entry: OrgEntry) => {
		const alias = entry.alias;
		const data = entry.data ?? {};
		const expiryState = (context.workspaceState.get<Record<string, number>>(expiryStateKey) ?? {}) as Record<string, number>;
		const isScratch = getBoolean(data, ['is_scratch', 'scratch', 'scratch_org', 'isScratch']) ?? false;
		const expired = getBoolean(data, ['expired']) ?? false;
		const dayCount = pickFirstString(data, ['days']);
		const { remaining, total } = parseScratchDayCount(dayCount);
		const summary = formatScratchDaySummary(remaining, total) ?? 'unknown remaining time';
		let changed = false;

		const clearState = () => {
			if (alias in expiryState) {
				delete expiryState[alias];
				changed = true;
			}
		};

		if (!isScratch) {
			clearState();
		} else if (expired) {
			if (expiryState[alias] !== -1) {
				expiryState[alias] = -1;
				changed = true;
				const choice = await vscode.window.showErrorMessage(
					`Default scratch org ${alias} has expired (${summary ?? 'expired'}).`,
					'Open Org Info',
					'Change Default'
				);
				if (choice === 'Open Org Info') {
					await showOrgInfoDocument(folder, alias);
				} else if (choice === 'Change Default') {
					await vscode.commands.executeCommand('cumulusci.status.showDefaultOrg');
				}
			}
		} else if (
			remaining !== undefined &&
			total !== undefined &&
			remaining <= 2
		) {
			if (expiryState[alias] !== remaining) {
				expiryState[alias] = remaining;
				changed = true;
				const friendlySummary = summary ?? `${remaining} ${pluralizeDay(remaining)} remaining`;
				const message = remaining === 0
					? `Default scratch org ${alias} expires today (${friendlySummary}).`
					: `Default scratch org ${alias} will expire soon (${friendlySummary}).`;
				const choice = await vscode.window.showWarningMessage(
					message,
					'Open Org Info',
					'Change Default'
				);
				if (choice === 'Open Org Info') {
					await showOrgInfoDocument(folder, alias);
				} else if (choice === 'Change Default') {
					await vscode.commands.executeCommand('cumulusci.status.showDefaultOrg');
				}
			}
		} else {
			clearState();
		}

		if (changed) {
			await context.workspaceState.update(expiryStateKey, expiryState);
		}
	};

	const updateDefaultOrgStatus = async () => {
		try {
			const info = await service.findDefaultOrg();
			if (!info) {
				output.appendLine('[status] No default org found; showing placeholder');
				defaultOrgStatus.text = '$(organization) No default org';
				defaultOrgStatus.tooltip = 'No default org set. Click to choose one.';
				const existingState = context.workspaceState.get<Record<string, number>>(expiryStateKey) ?? {};
				if (Object.keys(existingState).length > 0) {
					await context.workspaceState.update(expiryStateKey, {});
				}
				defaultOrgStatus.show();
				return;
			}
			const alias = info.entry.alias;
			const description = info.entry.description ?? '';
			const tooltipLines = [`Default CCI Org: ${alias}`, `Workspace: ${info.folder.name}`];
			if (description) {
				tooltipLines.push(description);
			}
			defaultOrgStatus.text = `$(organization) Default CCI Org: ${alias}`;
			defaultOrgStatus.tooltip = tooltipLines.join('\n');
			defaultOrgStatus.show();
			output.appendLine(`[status] Default org set to ${alias}`);
			await handleExpiryNotification(info.folder, info.entry);
		} catch (error) {
			const message = error instanceof Error ? error.message : String(error);
			output.appendLine(`[status] Failed to resolve default org: ${message}`);
			defaultOrgStatus.text = '$(organization) Org status unavailable';
			defaultOrgStatus.tooltip = message;
			defaultOrgStatus.show();
		}
	};

	const providers: Record<CciViewKind, CciTreeDataProvider> = {
		orgs: new CciTreeDataProvider('orgs', service, context.extensionUri),
		tasks: new CciTreeDataProvider('tasks', service, context.extensionUri),
		flows: new CciTreeDataProvider('flows', service, context.extensionUri),
		project: new CciTreeDataProvider('project', service, context.extensionUri),
		services: new CciTreeDataProvider('services', service, context.extensionUri)
	};

	const showOrgInfoDocument = async (folder: vscode.WorkspaceFolder, alias: string) => {
		const info = await service.getOrgInfo(folder, alias);
		const markdown = formatOrgInfoMarkdown(alias, info);
		const document = await vscode.workspace.openTextDocument({ language: 'markdown', content: markdown });
		await vscode.window.showTextDocument(document, { preview: true });
	};

	const pickOrgForExecution = async (
		folder: vscode.WorkspaceFolder,
		prompt: string
	): Promise<OrgQuickPickItem | undefined> => {
		const options = await service.getOrgQuickPickItems(folder);
		if (options.length === 0) {
			const manual = await vscode.window.showInputBox({
				prompt: 'Enter the org alias to use',
				placeHolder: 'Org alias',
				ignoreFocusOut: true
			});
			const trimmed = manual?.trim();
			if (!trimmed) {
				return undefined;
			}
			return {
				label: trimmed,
				description: undefined,
				detail: 'Custom alias',
				alias: trimmed,
				orgCreated: false,
				expired: false,
				isScratch: false,
				definitionOnly: false,
				definitionMissing: false,
				manual: true
			};
		}

		const active = options.filter((option) => option.orgCreated && !option.expired);
		if (
			active.length === 1 &&
			options.every((option) => option === active[0] || !option.orgCreated || option.expired)
		) {
			return active[0];
		}

		const manualOption: OrgQuickPickItem = {
			label: '$(edit) Enter another alias…',
			description: '',
			detail: 'Provide an alias not listed above',
			alias: '',
			orgCreated: false,
			expired: false,
			isScratch: false,
			definitionOnly: false,
			definitionMissing: false,
			manual: true
		};

		const choice = await vscode.window.showQuickPick<OrgQuickPickItem>(
			[...options, manualOption],
			{
				placeHolder: prompt,
				matchOnDetail: true
			}
		);
		if (!choice) {
			return undefined;
		}
		if (choice.manual) {
			const manual = await vscode.window.showInputBox({
				prompt: 'Enter the org alias to use',
				placeHolder: 'Org alias',
				ignoreFocusOut: true
			});
			const trimmed = manual?.trim();
			if (!trimmed) {
				return undefined;
			}
			return { ...choice, alias: trimmed, manual: false };
		}
		return choice;
	};

	type WorkspaceFolderPick = vscode.QuickPickItem & { folder: vscode.WorkspaceFolder };

	const pickProjectFolder = async (
		preferred?: vscode.WorkspaceFolder
	): Promise<vscode.WorkspaceFolder | undefined> => {
		if (preferred) {
			return preferred;
		}
		const projectFolders = await service.getProjectFolders();
		if (projectFolders.length === 0) {
			vscode.window.showWarningMessage('No CumulusCI project detected in the current workspace.');
			return undefined;
		}
		if (projectFolders.length === 1) {
			return projectFolders[0];
		}
		const choice = await vscode.window.showQuickPick<WorkspaceFolderPick>(
			projectFolders.map((folder) => ({
				label: folder.name,
				description: folder.uri.fsPath,
				folder
			})),
			{
				placeHolder: 'Select a CCI project folder',
				matchOnDescription: true
			}
		);
		return choice?.folder;
	};

	const collectFlowRunOptions = async (): Promise<string[] | undefined> => {
		const flagChoices = [
			{ label: 'Delete org after run', description: '(--delete-org)', flag: '--delete-org' },
			{ label: 'Enable debugger on error', description: '(--debug)', flag: '--debug' },
			{ label: 'Disable prompts', description: '(--no-prompt)', flag: '--no-prompt' }
		];
		const flagSelection = await vscode.window.showQuickPick(flagChoices, {
			placeHolder: 'Optional flags for this flow (press Enter to continue, Esc to cancel)',
			canPickMany: true,
			ignoreFocusOut: true
		});
		if (flagSelection === undefined) {
			return undefined;
		}
		const args: string[] = flagSelection.map((choice) => choice.flag);

		const taskOptions = await vscode.window.showInputBox({
			prompt: 'Task options (-o task__option value). Enter one per line (optional).',
			placeHolder: "deploy_post__path ./src/…",
			ignoreFocusOut: true
		});
		if (taskOptions === undefined) {
			return undefined;
		}
		args.push(...parseTaskOptionArgs(taskOptions));

		const extraArgs = await vscode.window.showInputBox({
			prompt: 'Additional CLI arguments (optional).',
			placeHolder: '--delete-org --no-prompt',
			ignoreFocusOut: true
		});
		if (extraArgs === undefined) {
			return undefined;
		}
		args.push(...splitShellArgs(extraArgs));

		return args;
	};

	context.subscriptions.push(
		vscode.window.registerTreeDataProvider('cumulusci.orgs', providers.orgs),
		vscode.window.registerTreeDataProvider('cumulusci.tasks', providers.tasks),
		vscode.window.registerTreeDataProvider('cumulusci.flows', providers.flows),
		vscode.window.registerTreeDataProvider('cumulusci.project', providers.project),
		vscode.window.registerTreeDataProvider('cumulusci.services', providers.services)
	);

	void updateDefaultOrgStatus();

	const refresh = (kind: CciViewKind) => {
		providers[kind].refresh();
		if (kind === 'orgs') {
			void updateDefaultOrgStatus();
		}
	};

	context.subscriptions.push(
		vscode.commands.registerCommand('cumulusci.refreshOrgs', () => refresh('orgs')),
		vscode.commands.registerCommand('cumulusci.refreshTasks', () => refresh('tasks')),
		vscode.commands.registerCommand('cumulusci.refreshFlows', () => refresh('flows')),
		vscode.commands.registerCommand('cumulusci.refreshProjectInfo', () => refresh('project')),
		vscode.commands.registerCommand('cumulusci.refreshServices', () => refresh('services'))
	);

	context.subscriptions.push(
		vscode.commands.registerCommand('cumulusci.status.showDefaultOrg', async () => {
			try {
				const folders = await service.getProjectFolders();
				if (folders.length === 0) {
					vscode.window.showWarningMessage('No CumulusCI projects detected in the current workspace.');
					return;
				}

				const defaultInfo = await service.findDefaultOrg();
				const picks: Array<
					vscode.QuickPickItem & {
						action: 'set' | 'clear';
						alias?: string;
						folder?: vscode.WorkspaceFolder;
					}
				> = [];

				for (const folder of folders) {
					const orgs = await service.getOrgQuickPickItems(folder);
					for (const org of orgs) {
						const isDefault =
							defaultInfo !== undefined &&
							defaultInfo.entry.alias === org.alias &&
							defaultInfo.folder.uri.fsPath === folder.uri.fsPath;
						picks.push({
							label: org.label,
							description: folder.name,
							detail: isDefault ? 'Current default' : org.detail,
							action: 'set',
							alias: org.alias,
							folder
						});
					}
				}

				if (defaultInfo) {
					picks.unshift({
						label: '$(circle-slash) Clear default org',
						description: defaultInfo.folder.name,
						detail: `Currently ${defaultInfo.entry.alias}`,
						action: 'clear',
						alias: defaultInfo.entry.alias,
						folder: defaultInfo.folder
					});
				}

				if (picks.length === 0) {
					vscode.window.showInformationMessage('No orgs available to set as default.');
					return;
				}

				const choice = await vscode.window.showQuickPick(picks, {
					placeHolder: defaultInfo
						? 'Select an org to make default or clear the current default'
						: 'Select an org to set as the CCI default',
					matchOnDescription: true,
					matchOnDetail: true
				});
				if (!choice) {
					return;
				}

				if (choice.action === 'clear') {
					if (!choice.folder) {
						return;
					}
					await vscode.window.withProgress(
						{ location: vscode.ProgressLocation.Window, title: 'Clearing CCI default org…' },
						async () => {
							await service.runCciCommand(choice.folder!, ['org', 'default', '--unset']);
						}
					);
					vscode.window.showInformationMessage('Cleared the CCI default org.');
					refresh('orgs');
					return;
				}

				if (!choice.folder || !choice.alias) {
					return;
				}

				await vscode.window.withProgress(
					{ location: vscode.ProgressLocation.Window, title: `Setting default org to ${choice.alias}…` },
					async () => {
						await service.runCciCommand(choice.folder!, ['org', 'default', choice.alias!]);
					}
				);
				vscode.window.showInformationMessage(`Set ${choice.alias} as the CCI default org.`);
				refresh('orgs');
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.service.showInfo', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'service', 'Show Service Info');
				const folder = requireWorkspaceFolder(item);
				const data = (item.data ?? {}) as CciRecord;
				const type = pickFirstString(data, ['type']);
				if (!type) {
					throw new Error('Service type is missing for this item.');
				}
				const name = pickFirstString(data, ['name']);
				const typeLabel = pickFirstString(data, ['typeLabel']) ?? formatServiceTypeLabel(type);
				const info = await service.getServiceInfo(folder, type, name);
				const markdown = formatServiceInfoMarkdown(typeLabel, name, info);
				const document = await vscode.workspace.openTextDocument({ language: 'markdown', content: markdown });
				await vscode.window.showTextDocument(document, { preview: true });
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.service.connect', async (target?: unknown) => {
			try {
				const item = resolveTreeItem(target);
				const folder = await pickProjectFolder(item?.workspaceFolder);
				if (!folder) {
					return;
				}
				const data = (item?.data ?? {}) as CciRecord;
				let type = pickFirstString(data, ['type']);
				let typeLabel = pickFirstString(data, ['typeLabel']);
				let attributes = Array.isArray(data.attributes)
					? ((data.attributes as unknown[]) as ServiceAttributeSpec[])
					: [];
				if (!type) {
					const groups = await service.getServiceTypeGroups(folder);
					if (groups.length === 0) {
						vscode.window.showWarningMessage('CCI did not report any service types to connect.');
						return;
					}
					type ServiceTypePick = vscode.QuickPickItem & {
						value: string;
						attributes: ServiceAttributeSpec[];
					};
					const pick = await vscode.window.showQuickPick<ServiceTypePick>(
						groups.map((group) => ({
							label: group.label,
							description: group.description || undefined,
							detail:
								group.entries.length > 0
									? `${group.entries.length} configured`
								:
									undefined,
							value: group.type,
							attributes: group.attributes
						})) as ServiceTypePick[],
						{
							placeHolder: 'Select a service type to connect',
							matchOnDescription: true
						}
					);
					if (!pick) {
						return;
					}
					type = pick.value;
					typeLabel = pick.label;
					attributes = pick.attributes;
				}
				if (!type) {
					return;
				}
				if (attributes.length === 0) {
					const groups = await service.getServiceTypeGroups(folder);
					const match = groups.find((group) => group.type === type);
					if (match) {
						attributes = match.attributes;
						if (!typeLabel) {
							typeLabel = match.label;
						}
					}
				}
				if (!typeLabel) {
					typeLabel = formatServiceTypeLabel(type);
				}
				const nameInput = await vscode.window.showInputBox({
					prompt: 'Optional service name (press Enter to accept the default)',
					placeHolder: 'Leave blank to let CCI decide',
					ignoreFocusOut: true
				});
				if (nameInput === undefined) {
					return;
				}
				const trimmedName = nameInput.trim();
				const toggleChoices = [
					{ label: 'Set as global default (--default)', description: 'Applies to all projects', flag: '--default' },
					{ label: 'Set as project default (--project)', description: 'Scoped to this workspace', flag: '--project' }
				];
				const toggleSelection = await vscode.window.showQuickPick(toggleChoices, {
					placeHolder: 'Select optional flags for this service (Esc to skip)',
					canPickMany: true,
					ignoreFocusOut: true
				});
				if (toggleSelection === undefined) {
					return;
				}
				const commandParts = ['cci', 'service', 'connect', type];
				if (trimmedName.length > 0) {
					commandParts.push(quoteArg(trimmedName));
				}
				if (toggleSelection && toggleSelection.length > 0) {
					for (const choice of toggleSelection) {
						if (choice.flag) {
							commandParts.push(choice.flag);
						}
					}
				}
				const hasAttributes = attributes.length > 0;
				if (hasAttributes) {
					for (const attribute of attributes) {
						const autoProvided = Boolean(attribute.defaultValue || attribute.defaultFactory);
						const required = attribute.required && !autoProvided;
						const description = attribute.description ?? `Enter value for ${attribute.name}`;
						const promptPrefix = required
							? 'Required'
							: autoProvided
							? 'Optional (leave blank to let CCI provide a default)'
							: 'Optional';
						let value: string | undefined;
						for (;;) {
							const input = await vscode.window.showInputBox({
								prompt: `${promptPrefix}: ${description}`,
								placeHolder: autoProvided ? 'Leave blank to use CLI default' : undefined,
								ignoreFocusOut: true,
								value: attribute.sensitive ? undefined : attribute.defaultValue,
								password: attribute.sensitive
							});
							if (input === undefined) {
								return;
							}
							const trimmed = input.trim();
							if (trimmed.length === 0) {
								if (attribute.defaultValue !== undefined) {
									value = attribute.defaultValue;
									break;
								}
								if (required) {
									await vscode.window.showErrorMessage(
										`A value is required for ${attribute.name}.`
									);
									continue;
								}
								break;
							}
							value = trimmed;
							break;
						}
						if (value !== undefined) {
							commandParts.push(`--${attribute.name}`);
							commandParts.push(quoteArg(value));
						}
					}
				}
				const label = typeLabel ?? formatServiceTypeLabel(type);
				const terminal = vscode.window.createTerminal({ name: `CCI: Connect ${label}`, cwd: folder.uri });
				terminal.show(true);
				terminal.sendText(commandParts.join(' '), true);
				if (hasAttributes) {
					vscode.window.showInformationMessage(
						`Opened a terminal to run "${commandParts.join(' ')}". Review or adjust values in the terminal if needed.`
					);
				} else {
					vscode.window.showInformationMessage(
						`Opened a terminal to run "${commandParts.join(' ')}".`
					);
				}
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.service.setDefault', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'service', 'Set Service Default');
				const folder = requireWorkspaceFolder(item);
				const data = (item.data ?? {}) as CciRecord;
				const type = pickFirstString(data, ['type']);
				const name = pickFirstString(data, ['name']);
				if (!type || !name) {
					throw new Error('Service type or name is missing.');
				}
				const scope = await vscode.window.showQuickPick(
					[
						{ label: 'Global default', description: 'Applies to all projects', flag: undefined },
						{ label: 'Project default', description: 'Applies only to this workspace', flag: '--project' }
					],
					{ placeHolder: 'Select default scope' }
				);
				if (!scope) {
					return;
				}
				const args = ['service', 'default'];
				if (scope.flag) {
					args.push(scope.flag);
				}
				args.push(type, name);
				const result = await service.runCciCommand(folder, args);
				if (result.trim().length > 0) {
					output.appendLine(result.trim());
					output.show(true);
				}
				refresh('services');
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.service.remove', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'service', 'Remove Service');
				const folder = requireWorkspaceFolder(item);
				const data = (item.data ?? {}) as CciRecord;
				const type = pickFirstString(data, ['type']);
				const name = pickFirstString(data, ['name']);
				if (!type || !name) {
					throw new Error('Service type or name is missing.');
				}
				const label = toTreeItemLabelString(item.label) || name;
				const confirmed = await vscode.window.showWarningMessage(
					`Remove service "${label}" (${type})?`,
					{ modal: true },
					'Remove'
				);
				if (confirmed !== 'Remove') {
					return;
				}
				await service.runCciCommand(folder, ['service', 'remove', type, name]);
				vscode.window.showInformationMessage(`Removed service "${label}" (${type}).`);
				refresh('services');
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.service.rename', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'service', 'Rename Service');
				const folder = requireWorkspaceFolder(item);
				const data = (item.data ?? {}) as CciRecord;
				const type = pickFirstString(data, ['type']);
				const currentName = pickFirstString(data, ['name']);
				if (!type || !currentName) {
					throw new Error('Service type or name is missing.');
				}
				const newNameInput = await vscode.window.showInputBox({
					prompt: 'Enter the new service name',
					value: currentName,
					ignoreFocusOut: true
				});
				if (newNameInput === undefined) {
					return;
				}
				const newName = newNameInput.trim();
				if (!newName || newName === currentName) {
					return;
				}
				await service.runCciCommand(folder, ['service', 'rename', type, currentName, newName]);
				refresh('services');
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		})
	);

	context.subscriptions.push(
		vscode.commands.registerCommand('cumulusci.runTask', async (target?: unknown) => {
			try {
				const item = resolveTreeItem(target);
				if (!item) {
					return;
				}
				ensureItemKind(item, 'task', 'Run Task');
				const folder = requireWorkspaceFolder(item);
				const fallbackName = toTreeItemLabelString(item.label) || 'task';
				const name = pickFirstString(item.data, ['name', 'task_name']) ?? fallbackName;
				const terminalName = `CCI Task: ${name}`;
				let terminal = vscode.window.terminals.find((t) => t.name === terminalName);
				if (!terminal) {
					terminal = vscode.window.createTerminal({ name: terminalName, cwd: folder.uri });
				}
				terminal.show();
				terminal.sendText(`cci task run ${name} `, false);
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.copyTaskCommand', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'task', 'Copy Task Command');
				const fallbackName = toTreeItemLabelString(item.label) || 'task';
				const name = pickFirstString(item.data, ['name', 'task_name']) ?? fallbackName;
				await vscode.env.clipboard.writeText(`cci task run ${name}`);
				vscode.window.showInformationMessage(`Copied: cci task run ${name}`);
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.runFlow', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'flow', 'Run Flow');
				const folder = requireWorkspaceFolder(item);
				const fallbackFlowName = toTreeItemLabelString(item.label) || 'flow';
				const flowName = pickFirstString(item.data, ['name', 'flow']) ?? fallbackFlowName;
				const selection = await pickOrgForExecution(folder, 'Select an org to run this flow against');
				if (!selection) {
					return;
				}
				const alias = selection.alias.trim();
				if (!alias) {
					return;
				}
				const flowOptions = await collectFlowRunOptions();
				if (flowOptions === undefined) {
					return;
				}
				const commandParts = ['cci', 'flow', 'run', flowName, '--org', alias, ...flowOptions];
				const commandText = commandParts.map(quoteArg).join(' ');

				await vscode.window.withProgress(
					{
						location: vscode.ProgressLocation.Notification,
						title: `Running flow ${flowName} against ${alias}`
					},
					async () => {
						const terminalName = `CCI Flow: ${flowName}`;
						let terminal = vscode.window.terminals.find((t) => t.name === terminalName);
						if (!terminal) {
							terminal = vscode.window.createTerminal({ name: terminalName, cwd: folder.uri });
						}
						terminal.show();
						terminal.sendText(commandText);
					}
				);
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.copyFlowCommand', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'flow', 'Copy Flow Command');
				const fallbackFlowName = toTreeItemLabelString(item.label) || 'flow';
				const flowName = pickFirstString(item.data, ['name', 'flow']) ?? fallbackFlowName;
				await vscode.env.clipboard.writeText(`cci flow run ${flowName} --org <org>`);
				vscode.window.showInformationMessage(`Copied: cci flow run ${flowName} --org <org>`);
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.org.showActions', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
				try {
					ensureItemKind(item, 'org', 'Org Actions');
					const folder = requireWorkspaceFolder(item);
					const fallbackAlias = toTreeItemLabelString(item.label) || 'org';
					const alias = pickFirstString(item.data, ['alias', 'name', 'org_name', 'config_name']) ?? fallbackAlias;
					const descriptionText = toDescriptionString(item.description);
					const orgDescription = descriptionText ? ` (${descriptionText})` : '';

				const basePick: OrgQuickPickItem = {
					label: alias,
					description: descriptionText,
					detail: undefined,
					alias,
					data: item.data,
					orgCreated: getBoolean(item.data, ['orgCreated']) ?? false,
					expired: getBoolean(item.data, ['expired']) ?? false,
					isScratch: getBoolean(item.data, ['scratch', 'scratch_org', 'is_scratch', 'isScratch']) ?? false,
					definitionOnly: getBoolean(item.data, ['definitionOnly']) ?? false,
					definitionMissing: getBoolean(item.data, ['definitionMissing']) ?? false,
					manual: false
				};

				const picks: Array<OrgQuickPickItem & { action: 'open' | 'info' | 'default' | 'copy' }>
					= [
						{
							...basePick,
							label: 'Open in Browser',
							detail: `cci org browser ${alias}`,
							action: 'open'
						},
						{
							...basePick,
							label: 'Show Org Info',
							detail: `cci org info ${alias}`,
							action: 'info'
						},
						{
							...basePick,
							label: 'Set as Default Org',
							detail: `cci org default ${alias}`,
							action: 'default'
						},
						{
							...basePick,
							label: 'Copy Org Alias',
							detail: alias,
							action: 'copy'
						}
					];

				const pick = await vscode.window.showQuickPick(picks, {
					placeHolder: `Choose an action for ${alias}${orgDescription}`
				});
				if (!pick) {
					return;
				}

				switch (pick.action) {
					case 'open': {
						output.appendLine(`$ cci org browser ${alias}`);
						await service.runCciCommand(folder, ['org', 'browser', alias]);
						break;
					}
					case 'info': {
						await showOrgInfoDocument(folder, alias);
						break;
					}
					case 'default': {
						output.appendLine(`$ cci org default ${alias}`);
						const result = await service.runCciCommand(folder, ['org', 'default', alias]);
						if (result.trim().length > 0) {
							output.appendLine(result.trim());
							output.show(true);
						}
						refresh('orgs');
						break;
					}
					case 'copy': {
						await vscode.env.clipboard.writeText(alias);
						vscode.window.showInformationMessage(`Copied org alias: ${alias}`);
						break;
					}
				}
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.org.openInBrowser', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
				try {
					ensureItemKind(item, 'org', 'Open Org in Browser');
					const folder = requireWorkspaceFolder(item);
					const fallbackAlias = toTreeItemLabelString(item.label) || 'org';
					const alias = pickFirstString(item.data, ['alias', 'name', 'org_name', 'config_name']) ?? fallbackAlias;
				output.appendLine(`$ cci org browser ${alias}`);
				await service.runCciCommand(folder, ['org', 'browser', alias]);
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.org.showInfo', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'org', 'Show Org Info');
				const folder = requireWorkspaceFolder(item);
				const fallbackAlias = toTreeItemLabelString(item.label) || 'org';
				const alias = pickFirstString(item.data, ['alias', 'name', 'org_name', 'config_name']) ?? fallbackAlias;
				output.appendLine(`$ cci org info ${alias} --json`);
				await showOrgInfoDocument(folder, alias);
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.org.setDefault', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'org', 'Set Default Org');
				const folder = requireWorkspaceFolder(item);
				const fallbackAlias = toTreeItemLabelString(item.label) || 'org';
				const alias = pickFirstString(item.data, ['alias', 'name', 'org_name', 'config_name']) ?? fallbackAlias;
				output.appendLine(`$ cci org default ${alias}`);
				const result = await service.runCciCommand(folder, ['org', 'default', alias]);
				if (result.trim().length > 0) {
					output.appendLine(result.trim());
					output.show(true);
				}
				refresh('orgs');
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		}),
		vscode.commands.registerCommand('cumulusci.copyOrgAlias', async (target?: unknown) => {
			const item = resolveTreeItem(target);
			if (!item) {
				return;
			}
			try {
				ensureItemKind(item, 'org', 'Copy Org Alias');
				const fallbackAlias = toTreeItemLabelString(item.label) || 'org';
				const alias = pickFirstString(item.data, ['alias', 'name', 'org_name', 'config_name']) ?? fallbackAlias;
				await vscode.env.clipboard.writeText(alias);
				vscode.window.showInformationMessage(`Copied org alias: ${alias}`);
			} catch (error) {
				vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
			}
		})
	);

	context.subscriptions.push(
		vscode.commands.registerCommand('cumulusci.helloWorld', () => {
			vscode.window.showInformationMessage('Hello World from cumulusci!');
		})
	);

	context.subscriptions.push(
		vscode.workspace.onDidChangeWorkspaceFolders(() => {
			refresh('orgs');
			refresh('tasks');
			refresh('flows');
			refresh('project');
			refresh('services');
		})
	);
}

export function deactivate() {}
