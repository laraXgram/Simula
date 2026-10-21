/** A round avatar with initials, colored by id like Telegram's peer colors. */
const COLORS = [
  ["#ff885e", "#ff516a"], // red
  ["#ffcd6a", "#ffa85c"], // orange
  ["#82b1ff", "#665fff"], // violet
  ["#a0de7e", "#54cb68"], // green
  ["#53edd6", "#28c9b7"], // cyan
  ["#72d5fd", "#2a9ef1"], // blue
  ["#e0a2f3", "#d669ed"], // pink
];

/** Telegram picks a peer's color by its id. */
export function colorIndex(id: string): number {
  const digits = id.replace(/\D/g, "");
  if (!digits) return 5;
  return Number(BigInt(digits) % 7n);
}

export function initialsOf(name: string): string {
  const words = name.split(/\s+/).filter(Boolean);
  return (
    words
      .slice(0, 2)
      .map((word) => [...word][0])
      .join("")
      .toUpperCase() || "?"
  );
}

interface Props {
  id: string;
  name: string;
  size?: number;
}

export function Avatar({ id, name, size = 46 }: Props) {
  const [from, to] = COLORS[colorIndex(id)] ?? COLORS[5];
  return (
    <span
      className="avatar"
      style={{
        inlineSize: size,
        blockSize: size,
        fontSize: size * 0.38,
        background: `linear-gradient(180deg, ${from}, ${to})`,
      }}
      aria-hidden="true"
    >
      {initialsOf(name)}
    </span>
  );
}
