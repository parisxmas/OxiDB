import { Projects } from "./Projects.tsx";
import { ProjectView } from "./ProjectView.tsx";
import { navigate, parseRoute, projectPath, usePath } from "./router.ts";

/** Top-level console: the project list, or one project's workspace.
 *
 *  Which one is in the URL, so a refresh keeps you where you were and a view can
 *  be linked to. */
export function Console() {
  const route = parseRoute(usePath());

  if (route.view === "project") {
    return (
      <ProjectView
        projectRef={route.ref}
        tab={route.tab}
        onTab={(t) => navigate(projectPath(route.ref, t))}
        onBack={() => navigate("/")}
      />
    );
  }
  return <Projects onOpen={(ref) => navigate(projectPath(ref))} />;
}
