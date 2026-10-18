/**
 * A message's link preview. Telegram fetches the page; Simula is offline and builds the
 * preview from the URL alone, so the card shows the site and address and says it was
 * built offline — the message would have a preview on Telegram.
 */
import type { tl } from "@simula/schema";

export function LinkPreview({ media }: { media: tl.messageMediaWebPage }) {
  const page = media.webpage;
  if (page._ !== "webPage") return null;
  return (
    <a className="link-preview" href={page.url} target="_blank" rel="noreferrer">
      <span className="link-preview-site">{page.site_name ?? page.display_url}</span>
      {page.title && <span className="link-preview-title">{page.title}</span>}
      {page.description && <span className="link-preview-description">{page.description}</span>}
      <span className="link-preview-url" dir="ltr">
        {page.display_url}
      </span>
      <span className="link-preview-note">Link preview · built offline by Simula</span>
    </a>
  );
}
