# CumulusCI Explorer for VS Code

CumulusCI Explorer is a Visual Studio Code extension that helps Salesforce developers and admins work with [CumulusCI](https://cumulusci.readthedocs.io/) projects without leaving the editor.
The sidebar explorer surfaces orgs, tasks, flows, project metadata, and connected services so you can inspect and run CCI commands with a couple of clicks.

## Features
- Activity Bar view container that organizes CumulusCI information into **Orgs**, **Tasks**, **Flows**, **Project Info**, and **Services** tree views.
- Status bar integration that highlights the current default org and warns when scratch orgs approach expiration.
- One-click commands to run tasks and flows, copy CLI invocations, and open org or service details.
- Rich parsing of `cci project info`, `cci org list`, `cci task list`, `cci flow list`, and `cci service list` output, including configuration-only entries from `cumulusci.yml`.
- Context menu actions to connect, inspect, set default, rename, or remove services directly from the explorer.

## Prerequisites
- [Visual Studio Code](https://code.visualstudio.com/)
- [CumulusCI CLI](https://cumulusci.readthedocs.io/en/latest/install.html) installed and available on your system `PATH`
- A workspace folder that contains a valid `cumulusci.yml` (the extension only activates for CCI projects)

## Development Setup
1. Install dependencies: `pnpm install`
2. Build the extension: `pnpm run compile`
3. Press `F5` in VS Code to launch a new Extension Development Host with CumulusCI Explorer enabled.
4. Open a CumulusCI project in the development host to populate the explorer views.

To produce a production build, run `pnpm run package` and use the generated `.vsix` from the `dist/` folder.

> Marketplace installation instructions will be added after the extension is published.

## Explorer Views
The **CumulusCI Explorer** view container appears in the VS Code Activity Bar after the extension activates.

### Orgs
- Lists connected and scratch orgs reported by `cci org list`
- Highlights default orgs, connection status, and scratch expiry information
- Context menu options include `Org Actions…`, `Open Org in Browser`, `Show Org Info`, `Set Org as Default`, and `Copy Org Alias`

### Tasks
- Groups tasks by their configured category/group
- Provides inline actions to `Run Task` in a dedicated terminal and `Copy Task Command`

### Flows
- Displays flows defined in your project configuration and the CLI
- Supports running a flow against a chosen org with optional flags (delete org, debug, no prompt, task options)
- Offers quick copy of the CLI invocation via `Copy Flow Command`

### Project Info
- Mirrors `cci project info` output in a browsable tree structure
- Shows nested configuration objects, arrays, and scalar values with helpful tooltips

### Services
- Groups service connections by type (e.g., `connected_app`, `devhub`, `github`)
- Flags default entries and exposes management commands:
  - `Connect Service`
  - `Show Service Info`
  - `Set Service Default`
  - `Rename Service`
  - `Remove Service`

## Command Palette Entries
All explorer actions are available via the VS Code Command Palette (`⌘⇧P` / `Ctrl+Shift+P`). Notable commands include:
- `CumulusCI: Refresh Orgs`
- `CumulusCI: Refresh Tasks`
- `CumulusCI: Refresh Flows`
- `CumulusCI: Refresh Project Info`
- `CumulusCI: Refresh Services`
- `CumulusCI: Connect Service`
- `CumulusCI: Run Task`
- `CumulusCI: Run Flow`
- `CumulusCI: Show Service Info`
- `CumulusCI: Open Org in Browser`
- `CumulusCI: Show Org Info`
- `CumulusCI: Set Org as Default`

The status bar item `Default CCI Org` is also interactive—click it to pick or clear the workspace default.
Notifications surface when the default scratch org is near expiry or already expired.

## Development
- Type-check: `pnpm run check-types`
- Lint: `pnpm run lint`
- Run extension tests: `pnpm run test`
- Continuous build (watch mode): `pnpm run watch`

Before publishing, run `pnpm run package` to rebuild the `dist/` artifacts.

## Contributing
Issues and pull requests are welcome. Please run `pnpm run lint`, `pnpm run check-types`, and `pnpm run test` before submitting changes. When adding features, accompany them with extension tests under `src/test/` that cover the new behavior.

## License
- The extension source code is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
- Image assets under `media/` and `resources/` are copied from [SFDO-Tooling/cci-vscode](https://github.com/SFDO-Tooling/cci-vscode)
  and remain subject to their original [license terms](https://github.com/SFDO-Tooling/cci-vscode/blob/main/LICENSE).
