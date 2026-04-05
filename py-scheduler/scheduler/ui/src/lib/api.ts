export async function fetchJson(url: string, options?: RequestInit) {
  const response = await fetch(url, options);
  const body = await response.text();
  let data: any = {};
  try {
    data = body ? JSON.parse(body) : {};
  } catch {
    data = {};
  }
  if (!response.ok) {
    throw new Error(data.error || body || `HTTP ${response.status}`);
  }
  return data;
}

export function escapeHtml(value: unknown): string {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

export function chipColor(chipType: string): string {
  const key = String(chipType || "").toUpperCase();
  if (key === "H200") return "var(--chip-h200)";
  if (key === "H100") return "var(--chip-h100)";
  if (key === "A100") return "var(--chip-a100)";
  if (key === "L40S") return "var(--chip-l40s)";
  return "var(--chip-default)";
}
