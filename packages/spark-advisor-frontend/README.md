[↩ spark-advisor](../../README.md)

# spark-advisor-frontend

React 19 SPA dashboard for spark-advisor. Real-time task monitoring, analysis submission, and statistics visualization. Part of the spark-advisor ecosystem.

## Tech Stack

| Tool | Role |
|------|------|
| React 19 | UI framework |
| TypeScript | Type safety |
| Vite 6 | Build tool with HMR |
| TanStack Query v5 | Server state management with caching and polling |
| Recharts | SVG charts for statistics |
| React Router 7 | Client-side routing |

## Development

```bash
# Install dependencies
npm install

# Start dev server (port 5173, proxies /api to gateway:8080)
npm run dev

# Production build
npm run build

# Type check
npm run typecheck
```

Or via Make:

```bash
make frontend-dev      # Start Vite dev server
make frontend-build    # Production build
```

## Pages

| Page | Route | Description |
|------|-------|-------------|
| Tasks | / | Task list with filtering, grouping by app, severity badges |
| Task Detail | /tasks/:id | Issues, stages, config comparison, rerun button |
| Analyze | /analyze | Submit new analysis with History Server autocomplete |
| Statistics | /statistics | KPI cards, daily volume chart, top issues, rule distribution |

## Project Structure

```
src/
├── pages/              # Page components (TasksPage, TaskDetailPage, AnalyzePage, StatisticsPage)
├── components/         # Reusable UI components
│   ├── layout/         # PageHeader, navigation
│   ├── task/           # OverviewTab, StagesTab, ConfigTab
│   └── ui/             # Badge, FilterBar, Pagination, SummaryStrip, ToastContainer
├── hooks/              # React hooks (useTasks, useAnalyze, useTaskWebSocket, useToast)
├── lib/                # Utilities (api client, types, formatters, notifications)
└── styles/             # Global CSS
```

## Real-time Updates

WebSocket connection to gateway (`/api/v1/ws/tasks`) with automatic reconnection (exponential backoff). Toast notifications on task completion/failure. Polling fallback for active tasks.

## Screenshots

See the [main README](../../README.md#web-dashboard) for dashboard screenshots.

## See also

- Gateway API: [../spark-advisor-gateway/README.md](../spark-advisor-gateway/README.md)
- Main project: [../../README.md](../../README.md)
- Development guide: [../../docs/development.md](../../docs/development.md)
