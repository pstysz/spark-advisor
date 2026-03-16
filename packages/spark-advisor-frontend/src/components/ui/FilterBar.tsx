import type { TaskStatus, DataSource } from "@/lib/types";

export interface TaskFilters {
  status?: TaskStatus;
  data_source?: DataSource;
  app_id?: string;
}

interface FilterBarProps {
  filters: TaskFilters;
  onFilterChange: (filters: TaskFilters) => void;
}

export function FilterBar({ filters, onFilterChange }: FilterBarProps) {
  return (
    <div className="filters-bar">
      <div className="filter-group">
        <label className="filter-label">Status</label>
        <select
          className="filter-select"
          value={filters.status ?? ""}
          onChange={(e) =>
            onFilterChange({ ...filters, status: (e.target.value || undefined) as TaskStatus | undefined })
          }
        >
          <option value="">All statuses</option>
          <option value="completed">Completed</option>
          <option value="running">Running</option>
          <option value="pending">Pending</option>
          <option value="failed">Failed</option>
        </select>
      </div>
      <div className="filter-group">
        <label className="filter-label">Source</label>
        <select
          className="filter-select"
          value={filters.data_source ?? ""}
          onChange={(e) =>
            onFilterChange({ ...filters, data_source: (e.target.value || undefined) as DataSource | undefined })
          }
        >
          <option value="">All sources</option>
          <option value="hs_manual">HS Manual</option>
          <option value="hs_poller">HS Poller</option>
          <option value="file">File</option>
          <option value="k8s">K8s</option>
        </select>
      </div>
      <div className="filter-group">
        <label className="filter-label">App ID</label>
        <input
          className="filter-input"
          type="text"
          placeholder="Search by Application ID..."
          value={filters.app_id ?? ""}
          onChange={(e) => onFilterChange({ ...filters, app_id: e.target.value || undefined })}
        />
      </div>
      {(filters.status || filters.data_source || filters.app_id) && (
        <button className="btn-ghost" onClick={() => onFilterChange({})}>
          Clear filters
        </button>
      )}
    </div>
  );
}
