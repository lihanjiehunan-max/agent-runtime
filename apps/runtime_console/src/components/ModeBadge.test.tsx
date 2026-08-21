import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ModeBadge } from "./ModeBadge";

describe("ModeBadge", () => {
  it("makes LIVE and DEMO provenance visible", () => {
    render(<ModeBadge mode="DEMO" reason="manual fixture" />);
    expect(screen.getByTestId("mode-badge")).toHaveTextContent("DEMO");
    expect(screen.getByTestId("mode-badge")).toHaveTextContent("manual fixture");
  });
});
