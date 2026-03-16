interface PaginationProps {
  total: number;
  limit: number;
  offset: number;
  onPageChange: (offset: number) => void;
}

export function Pagination({ total, limit, offset, onPageChange }: PaginationProps) {
  const currentPage = Math.floor(offset / limit) + 1;
  const totalPages = Math.ceil(total / limit);

  if (totalPages <= 1) return null;

  const pages = getPageNumbers(currentPage, totalPages);

  return (
    <div className="pagination">
      <span className="pagination-info">
        {offset + 1}–{Math.min(offset + limit, total)} of {total}
      </span>
      <div className="pagination-controls">
        <button
          className="btn-page"
          disabled={currentPage === 1}
          onClick={() => onPageChange((currentPage - 2) * limit)}
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
            <path d="M15 18l-6-6 6-6" />
          </svg>
        </button>
        <div className="pagination-pages">
          {pages.map((page, i) =>
            page === "..." ? (
              <span key={`ellipsis-${i}`} className="pagination-ellipsis">...</span>
            ) : (
              <button
                key={page}
                className={`btn-page ${page === currentPage ? "active" : ""}`}
                onClick={() => onPageChange(((page as number) - 1) * limit)}
              >
                {page}
              </button>
            ),
          )}
        </div>
        <button
          className="btn-page"
          disabled={currentPage === totalPages}
          onClick={() => onPageChange(currentPage * limit)}
        >
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
            <path d="M9 18l6-6-6-6" />
          </svg>
        </button>
      </div>
    </div>
  );
}

function getPageNumbers(current: number, total: number): (number | "...")[] {
  if (total <= 7) {
    return Array.from({ length: total }, (_, i) => i + 1);
  }
  if (current <= 3) {
    return [1, 2, 3, 4, "...", total];
  }
  if (current >= total - 2) {
    return [1, "...", total - 3, total - 2, total - 1, total];
  }
  return [1, "...", current - 1, current, current + 1, "...", total];
}
