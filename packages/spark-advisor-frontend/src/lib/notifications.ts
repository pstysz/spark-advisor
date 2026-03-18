import type { QueryClient } from "@tanstack/react-query";
import { toast } from "@/hooks/useToast";

export interface WsMessage {
  event: string;
  task_id?: string;
  data?: Record<string, unknown>;
}

export type MessageHandler = (msg: WsMessage, queryClient: QueryClient) => void;

const handlers: MessageHandler[] = [];

export function registerHandler(handler: MessageHandler) {
  handlers.push(handler);
}

export function handleMessage(msg: WsMessage, queryClient: QueryClient) {
  for (const handler of handlers) {
    handler(msg, queryClient);
  }
}

registerHandler((msg, queryClient) => {
  if (msg.event !== "status" || !msg.task_id) return;

  void queryClient.invalidateQueries({ queryKey: ["task", msg.task_id] });
  void queryClient.invalidateQueries({ queryKey: ["tasks"] });
  void queryClient.invalidateQueries({ queryKey: ["stats"] });

  const appId = (msg.data?.app_id as string) ?? msg.task_id;
  const status = msg.data?.status as string | undefined;

  if (status === "completed") {
    toast.success(`Analysis completed for ${appId}`);
  } else if (status === "failed") {
    const error = (msg.data?.error as string) ?? "Unknown error";
    toast.error(`Analysis failed for ${appId}: ${error}`);
  }
});
