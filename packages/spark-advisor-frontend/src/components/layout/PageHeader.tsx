import { Link } from "react-router";

interface Breadcrumb {
  label: string;
  to?: string;
}

interface PageHeaderProps {
  title: string;
  breadcrumbs?: Breadcrumb[];
  actions?: React.ReactNode;
}

export function PageHeader({ title, breadcrumbs, actions }: PageHeaderProps) {
  return (
    <div className="page-header">
      <div>
        {breadcrumbs && (
          <div className="breadcrumb">
            {breadcrumbs.map((crumb, i) => (
              <span key={i}>
                {i > 0 && <span className="sep">/</span>}
                {crumb.to ? (
                  <Link to={crumb.to}>{crumb.label}</Link>
                ) : (
                  <span>{crumb.label}</span>
                )}
              </span>
            ))}
          </div>
        )}
        <h2>{title}</h2>
      </div>
      {actions && <div>{actions}</div>}
    </div>
  );
}
