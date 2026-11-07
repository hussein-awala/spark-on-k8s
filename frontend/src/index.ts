import "./styles.css";

export { SparkConsole } from "./SparkConsole";
export {
  createConsoleTransport,
  mergeApplications,
  consolePath,
} from "./client";
export type {
  ConsoleConfig,
  ConsoleApplication,
  ConsoleTransport,
  ApplicationPage,
  RecordedRun,
  RunPage,
} from "./types";
