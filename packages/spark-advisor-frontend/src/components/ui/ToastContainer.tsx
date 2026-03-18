import { useToasts, dismissToast, type Toast } from "@/hooks/useToast";

const icons: Record<string, string> = {
  success: "M20 6 9 17l-5-5",
  error: "M18 6 6 18M6 6l12 12",
  warning: "M12 9v4m0 4h.01M10.29 3.86l-8.6 14.86A2 2 0 003.42 21h17.16a2 2 0 001.73-2.98l-8.6-14.86a2 2 0 00-3.46 0z",
  info: "M12 16v-4m0-4h.01M22 12a10 10 0 11-20 0 10 10 0 0120 0z",
};

const colors: Record<string, string> = {
  success: "var(--status-completed, #22c55e)",
  error: "var(--severity-critical, #ef4444)",
  warning: "var(--severity-warning, #f59e0b)",
  info: "var(--accent, #3b82f6)",
};

function ToastItem({ t }: { t: Toast }) {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 10,
        padding: "12px 16px",
        background: "var(--card-bg, #1e1e2e)",
        border: `1px solid ${colors[t.type]}`,
        borderLeft: `4px solid ${colors[t.type]}`,
        borderRadius: 8,
        boxShadow: "0 4px 12px rgba(0,0,0,0.3)",
        minWidth: 300,
        maxWidth: 450,
        animation: "toast-slide-in 0.3s ease-out",
      }}
    >
      <svg
        width="18"
        height="18"
        viewBox="0 0 24 24"
        fill="none"
        stroke={colors[t.type]}
        strokeWidth={2}
        strokeLinecap="round"
        strokeLinejoin="round"
        style={{ flexShrink: 0 }}
      >
        <path d={icons[t.type]} />
      </svg>
      <span style={{ fontSize: 13, color: "var(--text-primary, #e0e0e0)", flex: 1, lineHeight: 1.4 }}>
        {t.message}
      </span>
      <button
        onClick={() => dismissToast(t.id)}
        style={{
          background: "none",
          border: "none",
          color: "var(--text-muted, #888)",
          cursor: "pointer",
          padding: 2,
          flexShrink: 0,
        }}
      >
        <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
          <path d="M18 6 6 18M6 6l12 12" />
        </svg>
      </button>
    </div>
  );
}

export function ToastContainer() {
  const toasts = useToasts();

  if (toasts.length === 0) return null;

  return (
    <div
      style={{
        position: "fixed",
        top: 16,
        right: 16,
        zIndex: 9999,
        display: "flex",
        flexDirection: "column",
        gap: 8,
      }}
    >
      {toasts.map((t) => (
        <ToastItem key={t.id} t={t} />
      ))}
    </div>
  );
}
