import { NavLink } from "react-router-dom";
import type { ReactNode } from "react";
import {
  IconDashboard,
  IconCollections,
  IconQuery,
  IconSql,
  IconDesigner,
  IconOxiMem,
  IconIndexes,
  IconBlobs,
  IconAggregation,
  IconMonitor,
} from "./NavIcons";

const links: { to: string; label: string; icon: ReactNode }[] = [
  { to: "/dashboard", label: "Dashboard", icon: <IconDashboard /> },
  { to: "/collections", label: "Collections", icon: <IconCollections /> },
  { to: "/query", label: "Query", icon: <IconQuery /> },
  { to: "/sql", label: "SQL", icon: <IconSql /> },
  { to: "/designer", label: "Query Designer", icon: <IconDesigner /> },
  { to: "/oximem", label: "OxiMem", icon: <IconOxiMem /> },
  { to: "/indexes", label: "Indexes", icon: <IconIndexes /> },
  { to: "/blobs", label: "Blobs", icon: <IconBlobs /> },
  { to: "/aggregation", label: "Aggregation", icon: <IconAggregation /> },
  { to: "/monitor", label: "Monitor", icon: <IconMonitor /> },
];

export function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar-header">OxiDB</div>
      <nav className="sidebar-nav">
        {links.map((link) => (
          <NavLink
            key={link.to}
            to={link.to}
            className={({ isActive }) =>
              `sidebar-link${isActive ? " active" : ""}`
            }
          >
            <span className="sidebar-icon">{link.icon}</span>
            {link.label}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
