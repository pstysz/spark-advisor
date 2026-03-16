import { Area, AreaChart, ResponsiveContainer, XAxis, YAxis, Tooltip } from "recharts";
import type { SeverityTrendEntry } from "@/lib/types";

export function SeverityTrendChart({ data }: { data: SeverityTrendEntry[] }) {
  const formatted = data.map((d) => ({
    ...d,
    label: new Date(d.date).toLocaleDateString("en-US", { month: "short", day: "numeric" }),
  }));

  return (
    <div className="chart-container">
      <ResponsiveContainer width="100%" height={140}>
        <AreaChart data={formatted} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
          <XAxis dataKey="label" tick={{ fill: "#5a5a6e", fontSize: 11 }} axisLine={false} tickLine={false} />
          <YAxis tick={{ fill: "#5a5a6e", fontSize: 11 }} axisLine={false} tickLine={false} />
          <Tooltip
            contentStyle={{ background: "#1a1a2e", border: "1px solid #2a2a3e", borderRadius: 8, fontSize: 12 }}
            labelStyle={{ color: "#8888a0" }}
          />
          <Area type="monotone" dataKey="critical" stroke="#ef4444" fill="none" strokeWidth={2} />
          <Area type="monotone" dataKey="warning" stroke="#f59e0b" fill="none" strokeWidth={2} />
          <Area type="monotone" dataKey="info" stroke="#3b82f6" fill="none" strokeWidth={2} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
