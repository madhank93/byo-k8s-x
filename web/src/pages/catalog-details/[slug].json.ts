// Per-stage detail (the concept note, plus the reference solution's diff),
// prerendered as static JSON and fetched by the /catalog modal on open.
// Keeping it out of catalog.astro stops every stage's solution from loading
// with the table.
import type { APIRoute } from 'astro';
import { CATALOG, COURSES } from '../../data/catalog';

type MdModule = {
  frontmatter: Record<string, unknown>;
  // Astro 5 resolves compiled markdown asynchronously.
  compiledContent: () => Promise<string>;
};

// Astro compiles these to HTML at build time; key them by bare filename.
const DETAILS = Object.fromEntries(
  Object.entries(import.meta.glob('../../data/stage-details/*.md', { eager: true })).map(
    ([path, mod]) => [path.split('/').pop()!.replace(/\.md$/, ''), mod as MdModule]
  )
);

// Stages, plus one primer per course that ships one — the catalog's primer row
// and its ?stage=<course>-primer deep link fetch from here too.
export function getStaticPaths() {
  const stages = CATALOG.map((e) => ({ params: { slug: `${e.course}-${e.slug}` } }));
  const primers = Object.entries(COURSES)
    .filter(([, c]) => c.primer)
    .map(([course]) => ({ params: { slug: `${course}-primer` } }));
  return [...stages, ...primers];
}

export const GET: APIRoute = async ({ params }) => {
  const slug = params.slug!;
  const detail = DETAILS[slug];
  return new Response(
    JSON.stringify({ slug, detail: detail ? await detail.compiledContent() : null }),
    { headers: { 'Content-Type': 'application/json' } }
  );
};
