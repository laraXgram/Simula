/** List rows, switches and checkboxes, the building blocks of settings screens. */
import type { ReactNode } from "react";

import { Icon, type IconName } from "./Icon";

interface RowProps {
  icon?: IconName;
  /** Instead of an icon: an avatar or any leading element. */
  leading?: ReactNode;
  title: ReactNode;
  subtitle?: ReactNode;
  /** Right-hand detail (a count, a state). */
  detail?: ReactNode;
  onClick?: () => void;
  danger?: boolean;
  accent?: boolean;
  chevron?: boolean;
  trailing?: ReactNode;
}

/** A row of a settings list; a button when it does something. */
export function Row({
  icon,
  leading,
  title,
  subtitle,
  detail,
  onClick,
  danger,
  accent,
  chevron,
  trailing,
}: RowProps) {
  const className = ["row", danger ? "danger" : "", accent ? "accent" : ""].join(" ");
  const content = (
    <>
      {leading ??
        (icon && (
          <span className="row-icon">
            <Icon name={icon} />
          </span>
        ))}
      <span className="row-text">
        <span className="row-title" dir="auto">
          {title}
        </span>
        {subtitle && (
          <span className="row-subtitle" dir="auto">
            {subtitle}
          </span>
        )}
      </span>
      {detail !== undefined && <span className="row-detail">{detail}</span>}
      {trailing}
      {chevron && (
        <span className="row-chevron">
          <Icon name="chevron" size={20} />
        </span>
      )}
    </>
  );
  return onClick ? (
    <button type="button" className={className} onClick={onClick}>
      {content}
    </button>
  ) : (
    <div className={className}>{content}</div>
  );
}

/** An on/off switch. */
export function Switch({
  checked,
  onChange,
  label,
  disabled,
}: {
  checked: boolean;
  onChange: (checked: boolean) => void;
  label: string;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      aria-label={label}
      disabled={disabled}
      className={checked ? "switch on" : "switch"}
      onClick={() => onChange(!checked)}
    >
      <span className="switch-knob" />
    </button>
  );
}

/** A row with a switch: the whole row toggles it. */
export function SwitchRow({
  icon,
  title,
  subtitle,
  checked,
  onChange,
  disabled,
}: {
  icon?: IconName;
  title: string;
  subtitle?: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
  disabled?: boolean;
}) {
  return (
    <Row
      icon={icon}
      title={title}
      subtitle={subtitle}
      onClick={disabled ? undefined : () => onChange(!checked)}
      trailing={<Switch checked={checked} onChange={onChange} label={title} disabled={disabled} />}
    />
  );
}

/** A titled group of rows, with an optional note under it. */
export function Section({
  title,
  note,
  children,
}: {
  title?: string;
  note?: ReactNode;
  children: ReactNode;
}) {
  return (
    <section className="section">
      {title && <h3 className="section-title">{title}</h3>}
      <div className="section-rows">{children}</div>
      {note && <p className="section-note">{note}</p>}
    </section>
  );
}

/** A round check mark for picking people. */
export function Check({ checked }: { checked: boolean }) {
  return (
    <span className={checked ? "check on" : "check"} aria-hidden="true">
      {checked && <Icon name="check" size={16} />}
    </span>
  );
}
