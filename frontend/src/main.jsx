import React, { useEffect, useState } from "react";
import { createRoot } from "react-dom/client";
import "./style.css";

const money = (cents) =>
  new Intl.NumberFormat("en-CA", { style: "currency", currency: "CAD" }).format(
    cents / 100,
  );
async function api(path, options) {
  const response = await fetch(`/api/${path}`, options);
  const body = await response.json().catch(() => null);
  if (!response.ok)
    throw new Error(
      typeof body?.detail === "string"
        ? body.detail
        : "Service unavailable or invalid request. Check the backend terminals.",
    );
  return body;
}

function App() {
  const [products, setProducts] = useState([]);
  const [item, setItem] = useState("frozen yogurt");
  const [user, setUser] = useState("");
  const [quantity, setQuantity] = useState(1);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [accepted, setAccepted] = useState(null);
  const [services, setServices] = useState({});
  const [summary, setSummary] = useState(null);
  useEffect(() => {
    let active = true;
    let timer;
    async function refresh() {
      await Promise.all(
        ["payment", "notification", "analytics"].map(async (name) => {
          try {
            const [records, health] = await Promise.all([
              api(`${name}/records`),
              api(`${name}/health`),
            ]);
            if (active)
              setServices((previous) => ({
                ...previous,
                [name]: { records, health },
              }));
          } catch {
            if (active)
              setServices((previous) => ({
                ...previous,
                [name]: { error: "Offline · check this service" },
              }));
          }
        }),
      );
      try {
        const data = await api("analytics/summary");
        if (active) setSummary(data);
      } catch {
        if (active) setSummary(null);
      }
      if (active) timer = setTimeout(refresh, 2000);
    }
    api("orders/products")
      .then((data) => {
        if (active) setProducts(data);
      })
      .catch((e) => {
        if (active)
          setError(e.message + " Refresh the page once it is running.");
      });
    refresh();
    return () => {
      active = false;
      clearTimeout(timer);
    };
  }, []);

  async function submit(event) {
    event.preventDefault();
    setBusy(true);
    setError("");
    setAccepted(null);
    try {
      const result = await api("orders/orders", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          user: user.trim(),
          item,
          quantity: Number(quantity),
        }),
      });
      setAccepted(result);
    } catch (e) {
      setError(e.message);
    } finally {
      setBusy(false);
    }
  }
  const selected = products.find((product) => product.name === item);
  return (
    <main>
      <header>
        <a className="brand" href="/">
          ◈ StreamStore
        </a>
        <span className="badge">KAFKA LEARNING LAB</span>
      </header>
      <section className="intro">
        <p className="eyebrow">ONE ORDER. THREE CONSUMERS.</p>
        <h1>
          A little order.
          <br />
          <span>A whole chain of events.</span>
        </h1>
        <p>
          Place an order and watch three independent services pick it up from
          Kafka.
        </p>
      </section>
      <div className="layout">
        <section className="panel">
          <div className="section-title">
            <h2>Place an order</h2>
            <span>01 / PRODUCE</span>
          </div>
          <form onSubmit={submit}>
            <label>
              Your name
              <input
                required
                maxLength={80}
                value={user}
                onChange={(e) => setUser(e.target.value)}
                placeholder="e.g. Lara"
              />
            </label>
            <label>
              Pick something good
              <select
                value={item}
                onChange={(e) => setItem(e.target.value)}
                disabled={!products.length}
              >
                {products.map((p) => (
                  <option key={p.name} value={p.name}>
                    {p.name} · {money(p.price_cents)}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Quantity
              <input
                type="number"
                min="1"
                max="100"
                step="1"
                required
                value={quantity}
                onChange={(e) => setQuantity(e.target.value)}
              />
            </label>
            <div className="total">
              <span>Order total</span>
              <strong>
                {money((selected?.price_cents || 0) * Number(quantity))}
              </strong>
            </div>
            <button disabled={busy || !products.length || !user.trim()}>
              {busy ? "Publishing order…" : "Place order →"}
            </button>
            <p className="hint">Demo only. No real payments or emails.</p>
          </form>
          {error && (
            <p role="alert" className="error">
              {error}
            </p>
          )}
          {accepted && (
            <div role="status" className="receipt">
              <strong>Kafka accepted your order</strong>
              <p>Consumer results appear independently below.</p>
              <code>{accepted.order.order_id}</code>
              <p>
                Partition {accepted.kafka.partition} · Offset{" "}
                {accepted.kafka.offset}
              </p>
            </div>
          )}
        </section>
        <section className="activity">
          <div className="section-title">
            <h2>Behind the order</h2>
            <span>02 / CONSUME</span>
          </div>
          <div className="flow">
            Order API <span>→</span> orders topic <span>→</span> 3 consumer
            groups
          </div>
          <div className="stats">
            <div>
              <strong>{summary?.orders ?? "—"}</strong>
              <span>Orders counted</span>
            </div>
            <div>
              <strong>{summary?.items ?? "—"}</strong>
              <span>Items ordered</span>
            </div>
            <div>
              <strong>
                {summary ? money(summary.ordered_value_cents) : "—"}
              </strong>
              <span>Ordered value</span>
            </div>
          </div>
          {["payment", "notification", "analytics"].map((name, index) => {
            const service = services[name];
            const records = service?.records || [];
            const latest = accepted
              ? records.find((r) => r.order_id === accepted.order.order_id)
              : records[0];
            return (
              <article className="service" key={name}>
                <div className="service-head">
                  <div>
                    <span className="number">0{index + 1}</span>
                    <h3>{name}</h3>
                  </div>
                  <span className={service?.error ? "offline" : "status"}>
                    {service?.error
                      ? "Offline"
                      : service?.health?.assigned_partitions
                        ? "Listening"
                        : "Connecting"}
                  </span>
                </div>
                <p>
                  {service?.error ||
                    latest?.message ||
                    "Waiting for an order to arrive…"}
                </p>
                {latest && (
                  <div className="record">
                    <span>
                      {latest.quantity} × {latest.item} · {latest.user}
                    </span>
                    <code>{latest.order_id.slice(0, 8)}</code>
                  </div>
                )}
              </article>
            );
          })}
          <p className="hint">
            Updates every 2 seconds · Each service has its own Kafka offset
          </p>
        </section>
      </div>
      <footer>
        Built to make event-driven systems a little easier to see.
      </footer>
    </main>
  );
}
createRoot(document.getElementById("root")).render(<App />);
