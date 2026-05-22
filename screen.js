// cloud_loader — frontend script
//
// Wraps window.playSong so that if the requested song lives in Drive (not yet
// on disk), we download it first, show progress in an overlay, then let the
// original playSong continue. The server's WebSocket play handler runs
// against a real local file as if nothing happened.

(function () {
    'use strict';

    const PLUGIN_ID = 'cloud_loader';
    const API = `/api/${PLUGIN_ID}`;

    function el(tag, attrs, children) {
        const e = document.createElement(tag);
        if (attrs) for (const k in attrs) {
            if (k === 'style') Object.assign(e.style, attrs[k]);
            else if (k === 'class') e.className = attrs[k];
            else e.setAttribute(k, attrs[k]);
        }
        if (children) for (const c of [].concat(children)) {
            e.appendChild(typeof c === 'string' ? document.createTextNode(c) : c);
        }
        return e;
    }

    let overlay = null;
    let overlayLabel = null;
    let overlayBar = null;
    let overlayPct = null;
    let overlayCancel = null;
    let cancelled = false;

    function ensureOverlay() {
        if (overlay) return;
        overlay = el('div', {
            id: 'cloud-loader-overlay',
            style: {
                position: 'fixed', inset: '0', zIndex: '99999',
                background: 'rgba(0,0,0,0.75)', display: 'none',
                alignItems: 'center', justifyContent: 'center',
                backdropFilter: 'blur(4px)',
            },
        });
        const card = el('div', {
            style: {
                background: '#1a1d24', color: '#e5e7eb', padding: '28px 32px',
                borderRadius: '12px', minWidth: '360px', maxWidth: '480px',
                boxShadow: '0 10px 40px rgba(0,0,0,0.5)', textAlign: 'center',
                fontFamily: 'system-ui, -apple-system, sans-serif',
            },
        });
        const title = el('div', {
            style: { fontSize: '15px', fontWeight: '600', marginBottom: '8px',
                     color: '#9ca3af', letterSpacing: '0.05em', textTransform: 'uppercase' },
        }, 'Downloading from Google Drive');
        overlayLabel = el('div', {
            style: { fontSize: '14px', marginBottom: '16px', wordBreak: 'break-all',
                     color: '#d1d5db' },
        }, '');
        const barWrap = el('div', {
            style: { background: '#374151', borderRadius: '999px', height: '8px',
                     overflow: 'hidden', marginBottom: '12px' },
        });
        overlayBar = el('div', {
            style: { background: '#4080e0', height: '100%', width: '0%',
                     transition: 'width 200ms ease' },
        });
        barWrap.appendChild(overlayBar);
        overlayPct = el('div', {
            style: { fontSize: '13px', color: '#9ca3af', fontVariantNumeric: 'tabular-nums' },
        }, '0%');
        overlayCancel = el('button', {
            style: {
                marginTop: '20px', background: 'transparent', border: '1px solid #4b5563',
                color: '#d1d5db', padding: '6px 16px', borderRadius: '6px',
                cursor: 'pointer', fontSize: '13px',
            },
        }, 'Cancel');
        overlayCancel.addEventListener('click', () => { cancelled = true; hideOverlay(); });
        card.appendChild(title);
        card.appendChild(overlayLabel);
        card.appendChild(barWrap);
        card.appendChild(overlayPct);
        card.appendChild(overlayCancel);
        overlay.appendChild(card);
        document.body.appendChild(overlay);
    }

    function showOverlay(filename) {
        ensureOverlay();
        cancelled = false;
        overlayLabel.textContent = filename;
        overlayBar.style.width = '0%';
        overlayPct.textContent = '0%';
        overlay.style.display = 'flex';
    }

    function updateOverlay(progress) {
        if (!overlay) return;
        const pct = Math.round((progress || 0) * 100);
        overlayBar.style.width = pct + '%';
        overlayPct.textContent = pct + '%';
    }

    function hideOverlay() {
        if (overlay) overlay.style.display = 'none';
    }

    async function needsPrefetch(filename) {
        try {
            const r = await fetch(`${API}/needs_prefetch?filename=${encodeURIComponent(filename)}`);
            if (!r.ok) return false;
            const j = await r.json();
            return !!j.needs_prefetch;
        } catch (e) {
            return false;
        }
    }

    async function prefetch(filename) {
        showOverlay(filename);
        try {
            await fetch(`${API}/prefetch?filename=${encodeURIComponent(filename)}`, {
                method: 'POST',
            });
        } catch (e) {
            hideOverlay();
            throw e;
        }
        // Poll status every 400ms.
        while (true) {
            if (cancelled) {
                hideOverlay();
                throw new Error('cancelled');
            }
            await new Promise((res) => setTimeout(res, 400));
            let st;
            try {
                const r = await fetch(`${API}/prefetch/status?filename=${encodeURIComponent(filename)}`);
                st = await r.json();
            } catch (e) {
                continue;
            }
            updateOverlay(st.progress || 0);
            if (st.state === 'ready') {
                hideOverlay();
                return;
            }
            if (st.state === 'error') {
                hideOverlay();
                throw new Error(st.error || 'download failed');
            }
        }
    }

    function installWrapper() {
        const orig = window.playSong;
        if (typeof orig !== 'function') {
            // Try again next tick — slopsmith installs playSong fairly early
            // but our script may load before it.
            setTimeout(installWrapper, 50);
            return;
        }
        if (orig.__cloudLoaderWrapped) return;

        const wrapped = async function (filename, ...rest) {
            let needs = false;
            try {
                needs = await needsPrefetch(filename);
            } catch (e) {
                console.error('[cloud_loader] needs_prefetch check failed:', e);
            }
            if (needs) {
                try {
                    await prefetch(filename);
                } catch (e) {
                    console.error('[cloud_loader] prefetch failed:', e);
                    if (window.slopsmith && typeof window.slopsmith.emit === 'function') {
                        window.slopsmith.emit('cloud_loader:error', { filename, error: String(e) });
                    }
                    // BLOCK the play — calling the original would just feed a
                    // 0-byte stub to the server's PSARC unpacker and produce
                    // a confusing "no PSARC entries" error. Better: surface
                    // the real reason to the user and abort.
                    const reason = String(e).replace(/^Error:\s*/, '');
                    alert(
                        `Couldn't download "${filename}" from Google Drive.\n\n` +
                        `Reason: ${reason}\n\n` +
                        `Check Settings → Cloud Library to reconnect or retry.`
                    );
                    return;
                }
            }
            return orig.call(this, filename, ...rest);
        };
        wrapped.__cloudLoaderWrapped = true;
        window.playSong = wrapped;
        console.log('[cloud_loader] playSong wrapper installed');
    }

    // Mirror the play-time prefetch logic for the sloppak converter. The
    // converter's frontend (bundled inside the .app) fires a direct
    // fetch('/api/plugins/sloppak_converter/enqueue', {method:'POST', body: JSON})
    // — no window.* global to wrap. So we intercept window.fetch and prefetch
    // the file before the worker tries to open it. The converter only READS
    // the .psarc; prefetch sets uploaded_at, so the watcher won't re-upload
    // the materialized .psarc. The watcher does pick up the resulting
    // .sloppak in dlc/sloppak/ on its next cycle.
    const CONVERT_URL_RE = /\/api\/plugins\/sloppak_converter\/enqueue(\?|$)/;

    function installConvertHook() {
        const origFetch = window.fetch;
        if (!origFetch || origFetch.__cloudLoaderConvertWrapped) return;

        const wrapped = async function (input, init) {
            const url = typeof input === 'string' ? input : (input && input.url);
            const method = (init && init.method)
                || (input && typeof input !== 'string' && input.method)
                || 'GET';
            if (!url || !CONVERT_URL_RE.test(url) || method.toUpperCase() !== 'POST') {
                return origFetch.call(this, input, init);
            }

            // Extract the filename from the body without consuming it.
            let filename = null;
            let nextInit = init;
            try {
                if (init && init.body != null) {
                    let bodyText;
                    if (typeof init.body === 'string') {
                        bodyText = init.body;
                    } else {
                        // Blob / FormData / ReadableStream — read once, then
                        // hand a fresh string body to the original fetch.
                        bodyText = await new Response(init.body).text();
                        nextInit = Object.assign({}, init, { body: bodyText });
                    }
                    const parsed = JSON.parse(bodyText);
                    if (parsed && typeof parsed.filename === 'string') {
                        filename = parsed.filename;
                    }
                }
            } catch (e) {
                console.warn('[cloud_loader] convert hook: body parse failed, letting request through', e);
            }

            if (filename) {
                let needs = false;
                try {
                    needs = await needsPrefetch(filename);
                } catch (e) {
                    console.error('[cloud_loader] needs_prefetch check failed:', e);
                }
                if (needs) {
                    try {
                        await prefetch(filename);
                    } catch (e) {
                        console.error('[cloud_loader] prefetch-before-convert failed:', e);
                        const reason = String(e).replace(/^Error:\s*/, '');
                        alert(
                            `Couldn't download "${filename}" from Google Drive.\n\n` +
                            `Reason: ${reason}\n\n` +
                            `Conversion cancelled. Check Settings → Cloud Library.`
                        );
                        // Fake a server error response so the converter UI
                        // shows the failure inline instead of enqueueing a
                        // job that would fail with "Not a PSARC file".
                        return new Response(
                            JSON.stringify({ detail: 'cloud_loader prefetch failed: ' + reason }),
                            { status: 503, headers: { 'Content-Type': 'application/json' } }
                        );
                    }
                }
            }
            return origFetch.call(this, input, nextInit);
        };
        wrapped.__cloudLoaderConvertWrapped = true;
        window.fetch = wrapped;
        console.log('[cloud_loader] convert fetch hook installed');
    }

    installWrapper();
    installConvertHook();
})();
