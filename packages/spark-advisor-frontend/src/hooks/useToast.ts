import { useCallback, useSyncExternalStore } from "react";

export type ToastType = "success" | "error" | "info" | "warning";

export interface Toast {
  id: number;
  type: ToastType;
  message: string;
}

let nextId = 0;
let toasts: Toast[] = [];
const listeners = new Set<() => void>();

function emit() {
  for (const l of listeners) l();
}

function addToast(type: ToastType, message: string, duration = 5000) {
  const id = ++nextId;
  toasts = [...toasts, { id, type, message }];
  emit();
  setTimeout(() => {
    toasts = toasts.filter((t) => t.id !== id);
    emit();
  }, duration);
}

export function dismissToast(id: number) {
  toasts = toasts.filter((t) => t.id !== id);
  emit();
}

export const toast = {
  success: (msg: string) => addToast("success", msg),
  error: (msg: string) => addToast("error", msg, 8000),
  info: (msg: string) => addToast("info", msg),
  warning: (msg: string) => addToast("warning", msg, 6000),
};

export function useToasts(): Toast[] {
  return useSyncExternalStore(
    useCallback((cb: () => void) => { listeners.add(cb); return () => listeners.delete(cb); }, []),
    () => toasts,
  );
}
