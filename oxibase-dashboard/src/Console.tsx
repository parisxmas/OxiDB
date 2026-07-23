import { useState } from "react";
import { Projects } from "./Projects.tsx";
import { ProjectView } from "./ProjectView.tsx";

/** Top-level console: the project list, or one project's workspace. */
export function Console() {
  const [openRef, setOpenRef] = useState<string | null>(null);

  if (openRef) {
    return <ProjectView projectRef={openRef} onBack={() => setOpenRef(null)} />;
  }
  return <Projects onOpen={setOpenRef} />;
}
