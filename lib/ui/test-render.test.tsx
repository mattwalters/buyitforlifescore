import { render } from "@testing-library/react";
import { describe, it, expect } from "vitest";

const TestComponent = () => <div>Hello World</div>;

describe("Test Render", () => {
  it("should render a simple component", () => {
    const { container } = render(<TestComponent />);
    expect(container.textContent).toBe("Hello World");
  });
});
