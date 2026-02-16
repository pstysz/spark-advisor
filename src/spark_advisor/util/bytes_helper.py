class BytesHelper:
    @staticmethod
    def to_human_bytes(n: int) -> str:
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if abs(n) < 1024:
                return f"{n:.1f} {unit}"
            n = int(n / 1024)
        return f"{n:.1f} PB"
