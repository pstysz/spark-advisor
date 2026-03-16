import { useEffect, useRef, useCallback } from "react";
import { useQueryClient } from "@tanstack/react-query";

interface WsMessage {
  event: "status" | "ping";
  task_id?: string;
  data?: Record<string, unknown>;
}

export function useTaskWebSocket(taskIds?: string[]) {
  const queryClient = useQueryClient();
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const retriesRef = useRef(0);

  const connect = useCallback(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const params = taskIds?.length ? `?task_ids=${taskIds.join(",")}` : "";
    const url = `${protocol}//${window.location.host}/api/v1/ws/tasks${params}`;

    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      retriesRef.current = 0;
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data as string) as WsMessage;
        if (msg.event === "status" && msg.task_id) {
          void queryClient.invalidateQueries({ queryKey: ["task", msg.task_id] });
          void queryClient.invalidateQueries({ queryKey: ["tasks"] });
          void queryClient.invalidateQueries({ queryKey: ["stats"] });
        }
      } catch {
        // ignore
      }
    };

    ws.onclose = () => {
      const delay = Math.min(1000 * 2 ** retriesRef.current, 30000);
      retriesRef.current++;
      reconnectTimeoutRef.current = setTimeout(connect, delay);
    };
  }, [queryClient, taskIds]);

  useEffect(() => {
    connect();
    return () => {
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
      wsRef.current?.close();
    };
  }, [connect]);
}
