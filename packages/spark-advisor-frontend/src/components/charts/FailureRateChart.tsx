import { Area, AreaChart, ResponsiveContainer, XAxis, YAxis, Tooltip } from "recharts";
import type { FailureRateTrendEntry } from "@/lib/types";

export function FailureRateChart({ data }: { data: FailureRateTrendEntry[] }) {
  const formatted = data.map((d) => ({
    ...d,
    label: new Date(d.date).toLocaleDateString("en-US", { month: "short", day: "numeric" }),
    ratePercent: (d.rate * 100).toFixed(1),
  }));

  return (
    <div className="chart-container">
      <ResponsiveContainer width="100%" height={140}>
        <AreaChart data={formatted} margin={{ top: 4, right: 4, bottom: 0, left: -20 }}>
          <defs>
            <linearGradient id="failGrad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="#ef4444" stopOpacity={0.2} />
              <stop offset="100%" stopColor="#ef4444" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <XAxis dataKey="label" tick={{ fill: "#5a5a6e", fontSize: 11 }} axisLine={false} tickLine={false} />
          <YAxis tick={{ fill: "#5a5a6e", fontSize: 11 }} axisLine={false} tickLine={false} />
          <Tooltip
            contentStyle={{ background: "#1a1a2e", border: "1px solid #2a2a3e", borderRadius: 8, fontSize: 12 }}
            labelStyle={{ color: "#8888a0" }}
            formatter={(value: number) => [`${(value * 100).toFixed(1)}%`, "Failure Rate"]}
          />
          <Area type="monotone" dataKey="rate" stroke="#ef4444" fill="url(#failGrad)" strokeWidth={2} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
