/**
 * Dialogs instead of the browser's `confirm` / `prompt`: `useDialogs()` gives
 * `confirm` and `prompt` returning promises, and `Modal` frames any custom dialog.
 */
import {
  type CSSProperties,
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useId,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";

interface ConfirmOptions {
  title: string;
  text?: ReactNode;
  ok?: string;
  danger?: boolean;
}

interface PromptOptions {
  title: string;
  label: string;
  value?: string;
  ok?: string;
  multiline?: boolean;
  maxLength?: number;
}

interface ChooseOptions<T> {
  title: string;
  text?: ReactNode;
  choices: readonly { label: string; value: T; danger?: boolean }[];
}

interface Dialogs {
  confirm: (options: ConfirmOptions) => Promise<boolean>;
  prompt: (options: PromptOptions) => Promise<string | null>;
  /** One of several answers, or `null` when cancelled. */
  choose: <T>(options: ChooseOptions<T>) => Promise<T | null>;
}

const Context = createContext<Dialogs | null>(null);

export function useDialogs(): Dialogs {
  const dialogs = useContext(Context);
  if (!dialogs) throw new Error("useDialogs outside DialogsProvider");
  return dialogs;
}

type Open =
  | { kind: "confirm"; options: ConfirmOptions; resolve: (value: boolean) => void }
  | { kind: "prompt"; options: PromptOptions; resolve: (value: string | null) => void }
  | { kind: "choose"; options: ChooseOptions<unknown>; resolve: (value: unknown) => void };

export function DialogsProvider({ children }: { children: ReactNode }) {
  const [open, setOpen] = useState<Open | null>(null);
  const confirm = useCallback(
    (options: ConfirmOptions) =>
      new Promise<boolean>((resolve) => setOpen({ kind: "confirm", options, resolve })),
    [],
  );
  const prompt = useCallback(
    (options: PromptOptions) =>
      new Promise<string | null>((resolve) => setOpen({ kind: "prompt", options, resolve })),
    [],
  );
  const choose = useCallback(
    <T,>(options: ChooseOptions<T>) =>
      new Promise<T | null>((resolve) =>
        setOpen({
          kind: "choose",
          options: options as ChooseOptions<unknown>,
          resolve: resolve as (value: unknown) => void,
        }),
      ),
    [],
  );
  const value = useMemo(() => ({ confirm, prompt, choose }), [confirm, prompt, choose]);
  return (
    <Context.Provider value={value}>
      {children}
      {open?.kind === "confirm" && (
        <Modal
          title={open.options.title}
          onClose={() => {
            open.resolve(false);
            setOpen(null);
          }}
          actions={[
            {
              label: "Cancel",
              onClick: () => {
                open.resolve(false);
                setOpen(null);
              },
            },
            {
              label: open.options.ok ?? "OK",
              danger: open.options.danger,
              primary: true,
              onClick: () => {
                open.resolve(true);
                setOpen(null);
              },
            },
          ]}
        >
          {open.options.text && <p className="modal-text">{open.options.text}</p>}
        </Modal>
      )}
      {open?.kind === "choose" && (
        <Modal
          title={open.options.title}
          onClose={() => {
            open.resolve(null);
            setOpen(null);
          }}
          actions={[
            ...open.options.choices.map((choice) => ({
              label: choice.label,
              danger: choice.danger,
              primary: true,
              onClick: () => {
                open.resolve(choice.value);
                setOpen(null);
              },
            })),
            {
              label: "Cancel",
              onClick: () => {
                open.resolve(null);
                setOpen(null);
              },
            },
          ]}
        >
          {open.options.text && <p className="modal-text">{open.options.text}</p>}
        </Modal>
      )}
      {open?.kind === "prompt" && (
        <PromptDialog
          options={open.options}
          onDone={(value) => {
            open.resolve(value);
            setOpen(null);
          }}
        />
      )}
    </Context.Provider>
  );
}

function PromptDialog({
  options,
  onDone,
}: {
  options: PromptOptions;
  onDone: (value: string | null) => void;
}) {
  const [value, setValue] = useState(options.value ?? "");
  return (
    <Modal
      title={options.title}
      onClose={() => onDone(null)}
      onSubmit={() => onDone(value)}
      actions={[
        { label: "Cancel", onClick: () => onDone(null) },
        { label: options.ok ?? "Save", primary: true, submit: true },
      ]}
    >
      <Field
        label={options.label}
        value={value}
        onChange={setValue}
        multiline={options.multiline}
        maxLength={options.maxLength}
        autoFocus
      />
    </Modal>
  );
}

export interface ModalAction {
  label: string;
  onClick?: () => void;
  primary?: boolean;
  danger?: boolean;
  /** Submits the dialog's form. */
  submit?: boolean;
  disabled?: boolean;
}

interface ModalProps {
  title: string;
  children?: ReactNode;
  actions?: readonly ModalAction[];
  onClose: () => void;
  onSubmit?: () => void;
  wide?: boolean;
}

export function Modal({ title, children, actions = [], onClose, onSubmit, wide }: ModalProps) {
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => event.key === "Escape" && onClose();
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, [onClose]);
  return (
    // biome-ignore lint/a11y/noStaticElementInteractions: the backdrop closes on click; Escape closes too
    <div className="modal-backdrop" onMouseDown={onClose}>
      <form
        className={wide ? "modal wide" : "modal"}
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onMouseDown={(event) => event.stopPropagation()}
        onSubmit={(event) => {
          event.preventDefault();
          onSubmit?.();
        }}
      >
        <h2 className="modal-title">{title}</h2>
        <div className="modal-body">{children}</div>
        {actions.length > 0 && (
          <div className="modal-actions">
            {actions.map((action) => (
              <button
                key={action.label}
                type={action.submit ? "submit" : "button"}
                className={[
                  "text-button",
                  action.primary ? "primary" : "",
                  action.danger ? "danger" : "",
                ].join(" ")}
                disabled={action.disabled}
                onClick={action.onClick}
              >
                {action.label}
              </button>
            ))}
          </div>
        )}
      </form>
    </div>
  );
}

interface FieldProps {
  label: string;
  value: string;
  onChange: (value: string) => void;
  multiline?: boolean;
  maxLength?: number;
  autoFocus?: boolean;
  placeholder?: string;
  prefix?: string;
  hint?: ReactNode;
  error?: string;
}

/** A text field with a floating label, like Telegram's. */
export function Field({
  label,
  value,
  onChange,
  multiline,
  maxLength,
  autoFocus,
  placeholder,
  prefix,
  hint,
  error,
}: FieldProps) {
  const ref = useRef<HTMLInputElement & HTMLTextAreaElement>(null);
  const id = useId();
  // The text starts right after the prefix, whatever its width.
  const prefixRef = useRef<HTMLSpanElement>(null);
  const [prefixWidth, setPrefixWidth] = useState(0);
  useLayoutEffect(() => {
    setPrefixWidth(prefix ? (prefixRef.current?.offsetWidth ?? 0) : 0);
  }, [prefix]);
  useEffect(() => {
    if (autoFocus) ref.current?.focus();
  }, [autoFocus]);
  const common = {
    ref,
    id,
    value,
    maxLength,
    placeholder: placeholder ?? " ",
    dir: "auto" as const,
    onChange: (event: { target: { value: string } }) => onChange(event.target.value),
  };
  return (
    <div
      className={error ? "field invalid" : "field"}
      style={prefix ? ({ "--prefix-width": `${prefixWidth}px` } as CSSProperties) : undefined}
    >
      {prefix && (
        <span className="field-prefix" ref={prefixRef}>
          {prefix}
        </span>
      )}
      {multiline ? <textarea rows={3} {...common} /> : <input {...common} />}
      <label className="field-label" htmlFor={id}>
        {error || label}
      </label>
      {maxLength !== undefined && multiline && (
        <span className="field-count">{maxLength - value.length}</span>
      )}
      {hint && <span className="field-hint">{hint}</span>}
    </div>
  );
}
