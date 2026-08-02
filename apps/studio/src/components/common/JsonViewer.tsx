import { JsonView, darkStyles, defaultStyles } from "react-json-view-lite";
import "react-json-view-lite/dist/index.css";
import { useTheme } from "../../context/ThemeContext";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function JsonViewer({ data }: { data: any }) {
  const { theme } = useTheme();
  return (
    <JsonView
      data={data}
      style={theme === "dark" ? darkStyles : defaultStyles}
    />
  );
}
