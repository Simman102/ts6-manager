/**
 * Unwrap TeamSpeak BBCode [URL]...[/URL] and [URL=...]...[/URL] tags into plain URLs.
 */
export function unwrapBbCodeUrls(input: string): string {
  return input
    .replace(
      /\[url\s*=\s*(?:"([^"]*)"|'([^']*)'|([^\]]*))\][\s\S]*?\[\/url\s*\]/gi,
      (_match, q1?: string, q2?: string, q3?: string) => (q1 ?? q2 ?? q3 ?? '').trim(),
    )
    .replace(
      /\[url\s*\]([\s\S]*?)\[\/url\s*\]/gi,
      (_match, inner: string) => inner.trim(),
    )
    .trim();
}
