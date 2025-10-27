# CumulusCI Explorer for VS Code

[![Visual Studio Marketplace Version](https://img.shields.io/visual-studio-marketplace/v/dipakparmar.cumulusci?color=blue)](https://marketplace.visualstudio.com/items?itemName=dipakparmar.cumulusci)
![Visual Studio Marketplace Installs](https://img.shields.io/visual-studio-marketplace/i/dipakparmar.cumulusci?label=VSCode%20Marketplace%20Installs&color=rightgreen)
[![Open VSX Downloads](https://img.shields.io/open-vsx/dt/dipakparmar/cumulusci?label=Open%20VSX%20Downloads)](https://open-vsx.org/extension/dipakparmar/cumulusci)
[![GitHub Release](https://img.shields.io/github/v/release/dipakparmar/cumulusci-vscode?include_prereleases&label=GitHub%20Release)](https://github.com/dipakparmar/cumulusci-vscode/releases)
[![License](https://img.shields.io/github/license/dipakparmar/cumulusci-vscode)](LICENSE)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg?style=flat-square)](http://makeapullrequest.com)

**One-click insights into your CumulusCI projects—manage orgs, tasks, flows, and services without leaving VS Code.**

CumulusCI Explorer streamlines day-to-day Salesforce DX and CumulusCI workflows by surfacing the
commands you already know inside an intuitive Activity Bar experience. Inspect project metadata, run
automation, connect services, and keep tabs on scratch org health directly from the editor.

## Acknowledgments

This project was inspired by the original [SFDO-Tooling/cci-vscode](https://github.com/SFDO-Tooling/cci-vscode) extension, which was archived in September 2025. Thanks to the original contributors: [Brandon Parker](https://github.com/TheBitShepherd), [David Glick](https://github.com/davisagli), [James Estevez](https://github.com/jstvz), and [Tim Barton](https://github.com/timbarton1966) for pioneering CumulusCI integration with VS Code.

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Explorer Panels](#explorer-panels)
  - [Orgs](#orgs)
  - [Tasks](#tasks)
  - [Flows](#flows)
  - [Project Info](#project-info)
  - [Services](#services)
- [Command Palette](#command-palette)
- [Status Bar & Notifications](#status-bar--notifications)
- [Integrations](#integrations)
- [Learn More](#learn-more)
- [Telemetry](#telemetry)
- [Contributing](#contributing)
- [License & Credits](#license--credits)

## Installation

To get started:

1. Install [Visual Studio Code](https://code.visualstudio.com/) 1.105.0 or later.
2. Install the [CumulusCI CLI](https://cumulusci.readthedocs.io/en/latest/install.html) and ensure the `cci`
   command is available on your `PATH`.
3. Install the extension from the source that suits your workflow:
   - [Visual Studio Marketplace](https://marketplace.visualstudio.com/items?itemName=dipakparmar.cumulusci)
   - [Open VSX Registry](https://open-vsx.org/extension/dipakparmar/cumulusci)
   - Download the latest `.vsix` from the [GitHub releases page](https://github.com/dipakparmar/cumulusci-vscode/releases).
4. If you download the `.vsix`, run `Extensions: Install from VSIX...` in VS Code and select the file.
5. Open a workspace folder that contains a `cumulusci.yml`. The explorer activates automatically when it detects a CCI project.

## Quick Start

1. Connect an org with `cci org connect` or define scratch orgs in your project.
2. Launch the **CumulusCI Explorer** view container in the Activity Bar.
3. Use the **Orgs** panel to inspect connected orgs, set your default, or open org info.
4. Expand **Tasks** or **Flows** to run automations with a single click or copy the exact CLI invocation.
5. Pin the status bar item to monitor the default org and receive expiry alerts while you work.

## Explorer Panels

### Orgs

- Mirrors `cci org list` and highlights scratch orgs that are about to expire.
- Shows default org badges and connection status at a glance.
- Inline menu lets you open the org in a browser, inspect org metadata, set the default org, copy aliases, or open the full
  `Org Actions…` picker for advanced commands.

### Tasks

- Groups tasks by their configured category so you can browse automation recipes logically.
- Provides `Run Task` to launch the task in a dedicated terminal with streamed output.
- Offers `Copy Task Command` for quick sharing in chats, docs, or CI logs.

### Flows

- Surfaces flows from both the CLI and your `cumulusci.yml`.
- Supports running flows against a specific org with optional flags for debugging, deletion, or no-prompt execution.
- Makes it easy to share the full flow invocation with `Copy Flow Command`.

### Project Info

- Renders the full output of `cci project info` in a navigable tree so you can drill into package, dependency, and
  dataset metadata without parsing JSON.
- Preserves the original structure of nested arrays and objects, with tooltips for scalar values.

### Services

- Groups configured services (e.g., `connected_app`, `devhub`, `github`, `metaci`) alongside the description from your project.
- Marks default entries and exposes actions to connect, show details, set or clear the default, rename, or remove services.
- Taps into the metadata described in `docs/cci-service-support.md` to explain which fields are required for each service type.

## Command Palette

Every explorer control is available from the Command Palette (`⌘⇧P` / `Ctrl+Shift+P`). The most used commands include:

- `CumulusCI: Refresh Orgs`
- `CumulusCI: Refresh Tasks`
- `CumulusCI: Refresh Flows`
- `CumulusCI: Refresh Project Info`
- `CumulusCI: Refresh Services`
- `CumulusCI: Run Task`
- `CumulusCI: Run Flow`
- `CumulusCI: Connect Service`
- `CumulusCI: Show Org Info`
- `CumulusCI: Open Org in Browser`
- `CumulusCI: Set Org as Default`

## Status Bar & Notifications

- The `Default CCI Org` status bar item keeps your active org within reach—click it to change or clear the workspace default.
- Scratch org expiry warnings surface as lightweight VS Code notifications so you can refresh or recreate orgs before they lapse.
- When an org is missing or expired, the extension guides you toward remediation by linking to the relevant `cci` commands.

## Learn More

- Explore the [CumulusCI documentation](https://cumulusci.readthedocs.io/) for CLI usage and best practices.

## Telemetry

CumulusCI Explorer does not collect telemetry. The extension only shells out to local `cci` commands and displays their results
inside VS Code.

## Contributing

Issues and pull requests are welcome! Before submitting, please:

- Run `pnpm run lint`, `pnpm run check-types`, and `pnpm run test`.
- Add or update tests under `src/test/` when introducing new behavior.
- Rebuild the production bundle with `pnpm run package` if you are updating artifacts in `dist/`.

## License & Credits

- Source code is available under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
- CumulusCI logos and icons in `media/` and `resources/` are from the original [SFDO-Tooling/cci-vscode](https://github.com/SFDO-Tooling/cci-vscode) project.
