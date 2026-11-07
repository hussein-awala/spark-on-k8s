import { createRoot } from "react-dom/client";
import { SparkConsole } from "./index";
import type { ConsoleConfig } from "./types";

const element = document.getElementById("spark-console-root");
const bootstrap = document.getElementById("spark-console-config");
if (element && bootstrap) {
  const config = JSON.parse(bootstrap.textContent || "{}") as ConsoleConfig;
  createRoot(element).render(<SparkConsole config={config} />);
}
