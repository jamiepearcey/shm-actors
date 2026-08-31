/* shared: theme toggle, tooltip, svg helpers, envelope grid */
(function () {
  const btn = document.getElementById("themeToggle");
  const stored = (() => { try { return localStorage.getItem("theme"); } catch (_) { return null; } })();
  if (stored === "light" || stored === "dark") document.documentElement.dataset.theme = stored;
  function label() {
    const t = document.documentElement.dataset.theme;
    btn.textContent = t === "light" ? "◐" : t === "dark" ? "◑" : "◒";
    btn.title = "Theme: " + (t || "system");
  }
  if (btn) {
    label();
    btn.addEventListener("click", () => {
      const cur = document.documentElement.dataset.theme || "system";
      const next = cur === "system" ? "dark" : cur === "dark" ? "light" : "system";
      if (next === "system") { delete document.documentElement.dataset.theme; try { localStorage.removeItem("theme"); } catch (_) {} }
      else { document.documentElement.dataset.theme = next; try { localStorage.setItem("theme", next); } catch (_) {} }
      label();
    });
  }
})();

const tip = (() => {
  const el = document.createElement("div");
  el.className = "tip"; el.setAttribute("role", "status");
  document.body.appendChild(el);
  return el;
})();
function tipShow(html, x, y) {
  tip.innerHTML = html; tip.classList.add("show");
  const r = tip.getBoundingClientRect();
  tip.style.left = Math.min(x + 14, innerWidth - r.width - 8) + "px";
  tip.style.top = Math.max(8, y - r.height - 10) + "px";
}
function tipHide() { tip.classList.remove("show"); }
function svgEl(tag, attrs) {
  const e = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const k in attrs) e.setAttribute(k, attrs[k]);
  return e;
}
const cssVar = v => getComputedStyle(document.documentElement).getPropertyValue(v).trim();

/* 64-byte envelope explorer (index page) */
function buildEnvelope(gridId, legId, infoId) {
  const fields = [
    ["to", "u64", 0, 8, "destination ActorId"],
    ["from", "u64", 8, 8, "sender ActorId"],
    ["corr", "u64", 16, 8, "correlation id — 0 = tell, else ask/reply"],
    ["payload", "u64", 24, 8, "LocalRef bits: a packed reference into the shared pool"],
    ["schema_id", "u32", 32, 4, "the body's interned schema"],
    ["version", "u32", 36, 4, "cell version the payload was committed at"],
    ["kind", "u16", 40, 2, "MessageKind discriminant"],
    ["flags", "u16", 42, 2, "FLAG_* bits (inline vs by-ref payload)"],
    ["deadline", "u32", 44, 4, "coarse deadline, ms"],
    ["epoch", "u32", 48, 4, "fencing token of the owning memory node"],
    ["magic", "u32", 52, 4, "\"HOLN\" — validated on every read"],
    ["abi_version", "u16", 56, 2, "frozen ABI version"],
    ["body_len", "u16", 58, 2, "inline body length"],
    ["_reserved", "u32", 60, 4, "must be zero"]
  ];
  const grid = document.getElementById(gridId),
    leg = document.getElementById(legId),
    info = document.getElementById(infoId);
  if (!grid) return;
  const byteEls = [];
  for (let i = 0; i < 64; i++) {
    const d = document.createElement("div");
    const f = fields.findIndex(x => i >= x[2] && i < x[2] + x[3]);
    d.className = "byte " + (f % 2 ? "f1" : "f0");
    d.dataset.f = f; grid.appendChild(d); byteEls.push(d);
  }
  const defaultInfo = info.innerHTML;
  function show(fi) {
    byteEls.forEach(b => b.classList.toggle("hl", +b.dataset.f === fi));
    if (fi < 0) { info.innerHTML = defaultInfo; return; }
    const [n, t, o, len, d] = fields[fi];
    info.innerHTML = "<b>" + n + "</b>: " + t + " @ offset " + o + " (" + len + " B) — " + d;
  }
  fields.forEach((f, fi) => {
    const b = document.createElement("button");
    b.innerHTML = '<span class="sw ' + (fi % 2 ? "f1" : "f0") + '"></span>' + f[0];
    b.addEventListener("mouseenter", () => show(fi));
    b.addEventListener("focus", () => show(fi));
    b.addEventListener("mouseleave", () => show(-1));
    b.addEventListener("blur", () => show(-1));
    leg.appendChild(b);
  });
  grid.addEventListener("mousemove", e => {
    const t = e.target.closest(".byte"); if (t) show(+t.dataset.f);
  });
  grid.addEventListener("mouseleave", () => show(-1));
}

/* sticky-toc highlighting for pages with .toc */
function tocHighlight() {
  const links = [...document.querySelectorAll(".toc a")];
  if (!links.length) return;
  const map = new Map(links.map(a => [a.getAttribute("href").slice(1), a]));
  const obs = new IntersectionObserver(es => {
    es.forEach(e => {
      if (e.isIntersecting) {
        links.forEach(a => a.classList.remove("on"));
        const a = map.get(e.target.id); if (a) a.classList.add("on");
      }
    });
  }, { rootMargin: "-15% 0px -75% 0px" });
  document.querySelectorAll("section[id], h3[id]").forEach(s => obs.observe(s));
}
document.addEventListener("DOMContentLoaded", tocHighlight);
