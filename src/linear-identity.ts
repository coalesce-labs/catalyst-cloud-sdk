import type { TenantClientFailure } from "./tenant-client.js";

export type MemberLinearIdentity =
  | { resolution: "unattempted" | "no_email" | "no_match" }
  | { resolution: "resolved"; linearUserId: string; resolvedAt: number }
  | { resolution: "manual"; linearUserId: string; resolvedAt: number; resolvedBy: string | null };
export interface LinearIdentityOption {
  id: string;
  name: string | null;
  displayName: string | null;
  avatarUrl: string | null;
}
export interface LinearIdentityView {
  identity: MemberLinearIdentity;
  /** Absent means no choice was offered, including a temporarily unreadable roster. */
  options?: LinearIdentityOption[];
}
export type LinearIdentityResult =
  | ({ outcome: "ok" } & LinearIdentityView)
  | { outcome: "conflict"; status: 409; reason: "already_resolved" | "already_claimed" | "identity_changed" }
  | TenantClientFailure;

function record(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
function nullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}
function identity(value: unknown): MemberLinearIdentity | null {
  if (!record(value)) return null;
  const resolution = value["resolution"];
  if (resolution === "unattempted" || resolution === "no_email" || resolution === "no_match") return { resolution };
  const linearUserId = value["linearUserId"], resolvedAt = value["resolvedAt"];
  if (typeof linearUserId !== "string" || !linearUserId || typeof resolvedAt !== "number" || !Number.isFinite(resolvedAt)) return null;
  if (resolution === "resolved") return { resolution, linearUserId, resolvedAt };
  const resolvedBy = value["resolvedBy"];
  if (resolution === "manual" && nullableString(resolvedBy)) return { resolution, linearUserId, resolvedAt, resolvedBy };
  return null;
}
export function parseLinearIdentityView(value: unknown): LinearIdentityView | null {
  if (!record(value)) return null;
  const member = identity(value["identity"]);
  if (!member) return null;
  if (!("options" in value)) return { identity: member };
  const raw = value["options"];
  if (!Array.isArray(raw)) return null;
  const options: LinearIdentityOption[] = [];
  for (const option of raw) {
    if (!record(option)) return null;
    const id = option["id"], name = option["name"], displayName = option["displayName"], avatarUrl = option["avatarUrl"];
    if (typeof id !== "string" || !id || !nullableString(name) || !nullableString(displayName) || !nullableString(avatarUrl)) return null;
    options.push({ id, name, displayName, avatarUrl });
  }
  return { identity: member, options };
}
