import { useEffect, useState } from 'preact/hooks';

export interface Route { path: string; segments: string[]; query: URLSearchParams; }
const read = (): Route => ({ path: location.pathname, segments: location.pathname.split('/').filter(Boolean), query: new URLSearchParams(location.search) });

export function navigate(path: string) { history.pushState({}, '', path); window.dispatchEvent(new PopStateEvent('popstate')); }
export function useRoute() {
  const [route, setRoute] = useState(read);
  useEffect(() => { const update = () => setRoute(read()); addEventListener('popstate', update); return () => removeEventListener('popstate', update); }, []);
  return route;
}

export function Link({ href, children, class: className }: { href: string; children: preact.ComponentChildren; class?: string }) {
  return <a href={href} class={className} onClick={(event) => { if (!event.ctrlKey && !event.metaKey) { event.preventDefault(); navigate(href); } }}>{children}</a>;
}
