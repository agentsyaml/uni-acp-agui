"use client";

import { CopilotChat } from "@copilotkit/react-core/v2";
import { useState } from "react";
import { z } from "zod";
import { useAcpFrontendTool } from "@/hooks/use-acp-frontend-tool";

/**
 * Generative-UI demo: the agent calls a frontend tool that **returns
 * structured data**, and we render an interactive card the user can click
 * through. The card persists in the right-hand panel even after the agent
 * finishes — proving the data round-tripped from the LLM, through the
 * browser, back into the LLM, and into the page state.
 */
export default function GenerativeToolPage() {
  const [orders, setOrders] = useState<
    Array<{ ts: string; sku: string; qty: number; total: number }>
  >([]);

  useAcpFrontendTool({
    name: "estimate_order",
    description:
      "Given a product SKU and quantity, return the estimated total price " +
      "(price = qty * unit_price). Renders as a card in the user's UI.",
    parameters: z.object({
      sku: z
        .string()
        .describe("product SKU; only A100, B200, or C300 are valid"),
      qty: z.number().int().positive().describe("number of units"),
    }),
    handler: ({ sku, qty }) => {
      // Pretend we have a price catalog living in the browser.
      const catalog: Record<string, number> = {
        A100: 19.99,
        B200: 49.5,
        C300: 199,
      };
      const unit = catalog[sku];
      if (typeof unit !== "number") {
        throw new Error(
          `unknown sku ${sku}; valid: ${Object.keys(catalog).join(", ")}`,
        );
      }
      const total = unit * qty;
      setOrders((prev) =>
        [
          {
            ts: new Date().toLocaleTimeString(),
            sku,
            qty,
            total,
          },
          ...prev,
        ].slice(0, 20),
      );
      return { sku, qty, unit, total };
    },
  });

  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <h1 className="text-3xl font-bold tracking-tight">
          Generative UI Tool
        </h1>
        <p className="text-muted text-sm leading-relaxed max-w-3xl">
          The agent calls <code>estimate_order</code>; the browser handler
          looks up the price, renders the card on the right, and returns the
          structured payload (
          <code>{`{ sku, qty, unit, total }`}</code>) to the agent. The LLM
          keeps referencing those numbers in its reply — that&rsquo;s the
          essence of Generative UI: the tool result is both UI and LLM
          context.
        </p>
        <p className="text-muted text-sm leading-relaxed">
          Try:{" "}
          <em>
            &ldquo;Estimate the cost of 3 units of SKU A100 and 2 of B200,
            then sum them.&rdquo;
          </em>
        </p>
      </header>

      <div className="grid lg:grid-cols-2 gap-4">
        <div className="demo-card h-[70vh] flex flex-col p-2">
          <CopilotChat className="flex-1" />
        </div>

        <div className="demo-card h-[70vh] flex flex-col">
          <h2 className="font-semibold mb-3 flex items-center justify-between">
            <span>Estimated orders</span>
            <span className="badge badge-accent">{orders.length}</span>
          </h2>
          <div className="flex-1 overflow-auto space-y-2 text-sm">
            {orders.length === 0 && (
              <div className="h-full flex items-center justify-center">
                <p className="text-muted text-center text-xs">
                  No estimates yet.<br />
                  Ask the agent to use <code>estimate_order</code>.
                </p>
              </div>
            )}
            {orders.map((o, i) => (
              <div
                key={i}
                className="rounded-lg border border-[var(--border)] bg-[var(--background)] p-3 flex items-center justify-between gap-3"
              >
                <div>
                  <div className="font-mono font-semibold text-base">
                    {o.sku}
                  </div>
                  <div className="text-xs text-muted">{o.ts}</div>
                </div>
                <div className="text-right">
                  <div className="font-mono text-xs text-muted">
                    qty &times; {o.qty}
                  </div>
                  <div className="font-mono text-base font-semibold text-[var(--success)]">
                    ${o.total.toFixed(2)}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
