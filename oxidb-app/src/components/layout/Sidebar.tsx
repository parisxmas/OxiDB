import { NavLink } from "react-router-dom";

const links = [
  { to: "/dashboard", label: "Dashboard" },
  { to: "/collections", label: "Collections" },
  { to: "/query", label: "Query" },
  { to: "/indexes", label: "Indexes" },
  { to: "/blobs", label: "Blobs" },
  { to: "/aggregation", label: "Aggregation" },
  { to: "/monitor", label: "Monitor" },
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
            {link.label}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
