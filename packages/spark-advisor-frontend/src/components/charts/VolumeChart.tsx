import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import type { DailyVolumeEntry } from "@/lib/types";

interface VolumeChartProps {
  data: DailyVolumeEntry[];
}

export function VolumeChart({ data }: VolumeChartProps) {
  return (
    <ResponsiveContainer width="100%" height={220}>
      <AreaChart data={data}>
        <defs>
          <linearGradient id="volumeGrad" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#f97316" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#f97316" stopOpacity={0} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="#1e1e2e" />
        <XAxis
          dataKey="date"
          tick={{ fill: "#5a5a6e", fontSize: 11 }}
          tickFormatter={(v: string) => v.slice(5)}
          axisLine={{ stroke: "#1e1e2e" }}
        />
        <YAxis tick={{ fill: "#5a5a6e", fontSize: 11 }} axisLine={{ stroke: "#1e1e2e" }} />
        <Tooltip
          contentStyle={{ background: "#12121a", border: "1px solid #1e1e2e", borderRadius: 8, fontSize: 13 }}
          labelStyle={{ color: "#8b8b9e" }}
          itemStyle={{ color: "#e2e2e8" }}
        />
        <Area type="monotone" dataKey="count" stroke="#f97316" fill="url(#volumeGrad)" strokeWidth={2} />
      </AreaChart>
    </ResponsiveContainer>
  );
}
