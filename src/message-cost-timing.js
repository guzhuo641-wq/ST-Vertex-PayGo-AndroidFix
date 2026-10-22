/* Copyright (c) 2026 Mana Nekoha
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. https://mozilla.org/MPL/2.0/.
 *
 * Patched for SillyDroid / Android WebView compatibility:
 * On Android System WebView, constructing a new ReadableStream + new Response(body)
 * causes the entire stream to be buffered before any data reaches SillyTavern,
 * which breaks streaming (text appears all at once after generation completes).
 * The fix detects Android WebView and returns the original Response unmodified,
 * falling back to a lightweight observer that does not wrap the stream.
 */

function hasContent(value) {
    if (typeof value === 'string') return value.length > 0;
    if (Array.isArray(value)) return value.some(part => hasContent(part?.text));
    return false;
}

function hasGeneratedContent(data) {
    return data?.choices?.some(choice => {
        const delta = choice.delta ?? choice.message ?? {};
        return [delta.content, delta.reasoning, delta.reasoning_content, choice.text].some(hasContent)
            || delta.reasoning_details?.some(part => hasContent(part?.text));
    }) || data?.candidates?.some(candidate => candidate.content?.parts?.some(part => hasContent(part.text)));
}

/**
 * Detect Android System WebView environments where ReadableStream construction
 * from a fetch body causes full buffering instead of incremental streaming.
 * SillyDroid uses SYSTEM_WEBVIEW by default; its GeckoView engine is fine.
 */
function isAndroidWebView() {
    try {
        const ua = globalThis.navigator?.userAgent || '';
        // Android System WebView identifies itself with "wv" or "WebView" in the UA.
        // SillyDroid GeckoView reports as Firefox (contains "Gecko" without "wv").
        if (!/Android/i.test(ua)) return false;
        // GeckoView/Fennec reports as Mozilla/5.0 (Android ...) Gecko/... Firefox/...
        if (/Gecko\/\d/i.test(ua) && /Firefox\//i.test(ua)) return false;
        // System WebView variants: "wv" token, "Chrome/" without standalone "Firefox"
        if (/\bwv\b/i.test(ua) || /WebView/i.test(ua)) return true;
        // Chrome on Android (not standalone Chrome app) also has buffering issues
        if (/Chrome\//i.test(ua) && /Android/i.test(ua) && !/Firefox\//i.test(ua)) return true;
        return false;
    } catch {
        return false;
    }
}

/** Pass through the response without teeing/buffering it. Only inspect bounded
 * SSE frames until the first actual content arrives; role/usage frames don't count.
 * Timings are browser-observed wall times, not server-side model inference time.
 *
 * PATCH: On Android WebView, skip ReadableStream wrapping entirely and return
 * the original response. Timing data (firstTokenMs) will be unavailable but
 * streaming will work correctly. */
export function observeGenerationResponse(response, {
    stream, startedAt, now = () => performance.now(), onTiming = () => {}, signal,
} = {}) {
    if (!response?.body?.getReader || !response.ok) return response;

    // --- PATCH: Android WebView ReadableStream compatibility ---
    // On Android System WebView, new ReadableStream({ pull }) + new Response(body)
    // buffers the entire stream, breaking incremental streaming. Return the
    // original response so SillyTavern reads it directly without wrapping.
    if (isAndroidWebView()) {
        // Still report timing on abort/end using non-intrusive event listeners.
        const reportFinished = (interrupted = false) => {
            try {
                onTiming({ stream: Boolean(stream), durationMs: Math.max(0, now() - startedAt), firstTokenMs: null, interrupted });
            } catch (error) {
                console.warn('[Vertex PayGo] Could not record response timing.', error);
            }
        };
        if (signal) {
            signal.addEventListener('abort', () => reportFinished(true), { once: true });
            if (signal.aborted) reportFinished(true);
        }
        // Fire timing report when the response stream ends naturally.
        // This is best-effort: we can't detect firstToken without wrapping,
        // but we can at least report total duration.
        try {
            response.body.addEventListener?.('end', () => reportFinished(false));
        } catch { /* Not all streams support this event. */ }
        return response; // Return original response unmodified — streaming works!
    }
    // --- END PATCH ---

    const reader = response.body.getReader();
    let firstTokenMs = null;
    let finished = false;
    let buffer = '';
    let frame = '';
    let discardFrame = false;
    let decoder = stream ? new TextDecoder() : null;
    const maxFrame = 131072;
    const report = (interrupted = false) => {
        if (finished) return;
        finished = true;
        signal?.removeEventListener('abort', aborted);
        try { onTiming({ stream: Boolean(stream), durationMs: Math.max(0, now() - startedAt), firstTokenMs, interrupted }); }
        catch (error) { console.warn('[Vertex PayGo] Could not record response timing.', error); }
    };
    const aborted = () => report(true);
    signal?.addEventListener('abort', aborted, { once: true });
    if (signal?.aborted) aborted();

    function inspect(bytes) {
        if (!decoder) return;
        buffer += decoder.decode(bytes, { stream: true });
        let newline;
        while ((newline = buffer.indexOf('\n')) >= 0) {
            const line = buffer.slice(0, newline).replace(/\r$/, '');
            buffer = buffer.slice(newline + 1);
            if (!line) {
                if (!discardFrame && frame) {
                    try {
                        if (hasGeneratedContent(JSON.parse(frame))) {
                            firstTokenMs = Math.max(0, now() - startedAt);
                            decoder = null;
                            buffer = '';
                            frame = '';
                            return;
                        }
                    } catch { /* Metadata, malformed frames and [DONE] aren't content. */ }
                }
                frame = '';
                discardFrame = false;
            } else if (line.startsWith('data:') && !discardFrame) {
                frame += `${line.slice(5).trimStart()}\n`;
                if (frame.length > maxFrame) { frame = ''; discardFrame = true; }
            }
        }
        if (buffer.length > maxFrame) { buffer = ''; frame = ''; discardFrame = true; }
    }

    const body = new ReadableStream({
        async pull(controller) {
            try {
                const { done, value } = await reader.read();
                if (done) { report(); controller.close(); reader.releaseLock(); return; }
                inspect(value);
                controller.enqueue(value);
            } catch (error) {
                report(true);
                controller.error(error);
                reader.releaseLock();
            }
        },
        async cancel(reason) {
            report(true);
            try { await reader.cancel(reason); } finally { reader.releaseLock(); }
        },
    });
    const wrapped = new Response(body, { status: response.status, statusText: response.statusText, headers: response.headers });
    // Keep the observable fetch response metadata for host/extensions.
    for (const key of ['url', 'redirected', 'type']) Object.defineProperty(wrapped, key, { value: response[key] });
    return wrapped;
}
