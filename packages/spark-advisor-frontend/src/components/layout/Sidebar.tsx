import { NavLink } from "react-router";

const navItems = [
  {
    to: "/",
    label: "Tasks",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
        <rect x="3" y="5" width="6" height="6" rx="1" />
        <path d="m3 17 2 2 4-4" />
        <path d="M13 6h8" />
        <path d="M13 12h8" />
        <path d="M13 18h8" />
      </svg>
    ),
  },
  {
    to: "/analyze",
    label: "Analyze",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
        <polygon points="6 3 20 12 6 21 6 3" />
      </svg>
    ),
  },
  {
    to: "/stats",
    label: "Statistics",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
        <path d="M3 3v16a2 2 0 0 0 2 2h16" />
        <path d="m19 9-5 5-4-4-3 3" />
      </svg>
    ),
  },
];

export function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <img src="/logo.png" alt="Spark Advisor" width={28} height={28} />
        <h1>spark-advisor</h1>
      </div>
      <nav className="sidebar-nav">
        {navItems.map((item) => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === "/"}
            className={({ isActive }) => (isActive ? "active" : "")}
          >
            {item.icon}
            {item.label}
          </NavLink>
        ))}
      </nav>
      <div className="sidebar-footer">Spark Advisor v0.1.9</div>
    </aside>
  );
}
