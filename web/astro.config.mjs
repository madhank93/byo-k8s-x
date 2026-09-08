// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import sitemap from '@astrojs/sitemap';

// Served from the custom domain at the root.
const SITE = 'https://byok8s.madhan.app';
const BASE = '/';
const DESCRIPTION =
	'Build your own kubectl in Go, one stage at a time, against a real kind cluster — with a concept note and a verified reference solution for every stage.';

export default defineConfig({
	site: SITE,
	base: BASE,
	// Primers used to be pages under /learn/. They now open in the catalog's
	// modal, where the course is chosen; keep the published URLs working.
	redirects: {
		'/learn/kubectl/': '/catalog/?stage=kubectl-primer',
		'/learn/controller/': '/catalog/?stage=controller-primer',
	},
	integrations: [
		sitemap(),
		starlight({
			title: 'byok8s',
			description: DESCRIPTION,
			logo: { src: './src/assets/byok8s.svg' },
			customCss: ['./src/styles/hero.css'],
			favicon: '/favicon.svg',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/madhank93/byo-k8s-x' },
			],
			editLink: {
				baseUrl: 'https://github.com/madhank93/byo-k8s-x/edit/main/web/',
			},
			// SEO / social-share metadata applied to every page.
			head: [
				{ tag: 'meta', attrs: { property: 'og:type', content: 'website' } },
				{ tag: 'meta', attrs: { property: 'og:site_name', content: 'byok8s' } },
				{ tag: 'meta', attrs: { property: 'og:image', content: new URL(`${BASE}og.png`, SITE).href } },
				{ tag: 'meta', attrs: { property: 'og:image:width', content: '1200' } },
				{ tag: 'meta', attrs: { property: 'og:image:height', content: '630' } },
				{ tag: 'meta', attrs: { name: 'twitter:card', content: 'summary_large_image' } },
				{ tag: 'meta', attrs: { name: 'twitter:image', content: new URL(`${BASE}og.png`, SITE).href } },
				{ tag: 'link', attrs: { rel: 'apple-touch-icon', href: `${BASE}apple-touch-icon.png` } },
				{ tag: 'meta', attrs: { name: 'theme-color', content: '#326ce5' } },
				{
					tag: 'meta',
					attrs: {
						name: 'keywords',
						content:
							'kubectl, Kubernetes, client-go, build your own kubectl, Go, Golang, kind, discovery, RESTMapper, server-side apply, learn Kubernetes',
					},
				},
			],
			sidebar: [
				{ label: 'Getting started', slug: 'getting-started' },
				{ label: 'Catalog', link: '/catalog/' },
			],
		}),
	],
});
