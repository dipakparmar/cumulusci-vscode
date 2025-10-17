# Repository Guidelines

## Project Structure & Module Organization
- `src/extension.ts` holds the main VS Code extension logic; supporting utilities live alongside it under `src/`.
- Prebuilt artifacts live in `dist/`; rebuild them with the packaging command before publishing.
- Static assets (SVGs, PNGs, GIFs) are organized under `media/` and `resources/` with light/dark variants for theme-aware icons.
- Extension tests belong in `src/test/`; replicate the existing layout when adding new suites.

## Build, Test, and Development Commands
- `pnpm run lint` — runs ESLint on `src/` to enforce TypeScript style rules.
- `pnpm run compile` — type-checks, lints, and runs the esbuild bundler for a development-ready build.
- `pnpm run package` — produces an optimized production bundle in `dist/`; execute before checking in artifact updates.
- `pnpm run test` — launches the VS Code extension test runner.

## Coding Style & Naming Conventions
- TypeScript is required; prefer modern language features and VS Code APIs over custom shims.
- Maintain 4-space indentation (default in existing files) and keep lines under ~120 characters where practical.
- Exported symbols use `PascalCase` for classes/types and `camelCase` for functions/constants; context keys follow the `cumulusci.*` convention seen in `extension.ts`.
- Always run ESLint (`pnpm run lint`) and check-types (`pnpm run check-types`) before submitting; fix issues rather than disabling rules.

## Testing Guidelines
- Build new tests with the VS Code Test Runner (`@vscode/test-electron`) under `src/test/`.
- Name files with the `.test.ts` suffix and use descriptive `suite`/`test` labels matching the feature under test.
- Ensure new functionality has coverage; run `pnpm run test` locally and include failures or flaky behavior in the PR discussion.

## Commit & Pull Request Guidelines
- Use concise, imperative commit messages following conventional commit (e.g., `feat: add new feature`) and group related changes together; avoid large mixed commits.
- Pull requests should describe the change, note any new commands or configuration steps, and reference GitHub issues or tickets when applicable.
- Attach screenshots or screen recordings when UI behavior changes (tree views, commands, icons) to aid reviewers.

## Security & Configuration Tips
- Do not commit secrets or user-specific paths; prefer environment variables for credentials referenced in `pnpm run` scripts.
- Update `package.json` and `pnpm-lock.yaml` together when modifying dependencies, and verify that `dist/` matches the rebuilt output.
