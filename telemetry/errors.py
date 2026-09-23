"""框架中立的业务异常类型。

放在 telemetry 包内，使分类层不依赖具体业务包（避免循环导入）；
业务代码可直接从 :mod:`app.errors` 导入（那里做了再导出）。
"""

from __future__ import annotations


class BusinessError(Exception):
    """业务异常：调用方可修复，默认映射为 HTTP 400。

    与未处理异常（框架级 5xx）在遥测口径中严格区分。
    """

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        if not 400 <= status_code < 500:
            raise ValueError("BusinessError 的 status_code 必须是 4xx")
        self.status_code = status_code
