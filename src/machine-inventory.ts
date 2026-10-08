export interface Machine {
  id: string;
  name: string;
  ownership: "self_hosted" | "catalyst";
  teams: string[];
  slots: number;
  inUse: number;
  free: number;
  lastCheckInAtMs: number | null;
  status: "live" | "stale";
  removable: boolean;
  renameable: boolean;
}
export interface MachineInventory {
  account: string;
  readAtMs: number;
  machines: Machine[];
  capacity: {
    slots: number;
    inUse: number;
    free: number;
    selfHostedSlots: number;
    catalystSlots: number;
  };
}
const object = (value: unknown): value is Record<string, unknown> =>
  value !== null && typeof value === "object" && !Array.isArray(value);
const count = (value: unknown): value is number =>
  typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
function machine(value: unknown): value is Machine {
  return (
    object(value) &&
    typeof value.id === "string" &&
    typeof value.name === "string" &&
    (value.ownership === "self_hosted" || value.ownership === "catalyst") &&
    Array.isArray(value.teams) &&
    value.teams.every((team: unknown) => typeof team === "string") &&
    count(value.slots) &&
    count(value.inUse) &&
    count(value.free) &&
    (value.lastCheckInAtMs === null || count(value.lastCheckInAtMs)) &&
    (value.status === "live" || value.status === "stale") &&
    typeof value.removable === "boolean" &&
    typeof value.renameable === "boolean"
  );
}
export function isMachineInventory(value: unknown): value is MachineInventory {
  if (
    !object(value) ||
    typeof value.account !== "string" ||
    !count(value.readAtMs) ||
    !Array.isArray(value.machines) ||
    !value.machines.every(machine) ||
    !object(value.capacity)
  )
    return false;
  return (
    count(value.capacity.slots) &&
    count(value.capacity.inUse) &&
    count(value.capacity.free) &&
    count(value.capacity.selfHostedSlots) &&
    count(value.capacity.catalystSlots)
  );
}
export interface MachineMutation {
  ok: true;
  hostId: string;
  name?: string;
}
export function isMachineMutation(value: unknown): value is MachineMutation {
  return (
    object(value) &&
    value.ok === true &&
    typeof value.hostId === "string" &&
    (value.name === undefined || typeof value.name === "string")
  );
}
