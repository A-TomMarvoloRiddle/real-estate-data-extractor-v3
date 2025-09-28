# firecrawl_client.py
from __future__ import annotations
import os
import logging
from typing import Any, Dict, Optional, Tuple

# ملاحظة: مكتبة firecrawl تغيّر واجهتها بين إصدارات (FirecrawlApp.scrape_url) و(Firecrawl.scrape).
# هذا الراپر يحاول التوافق تلقائياً مع كِلا النمطين.
import firecrawl  # official SDK

from .settings import REQUEST_TIMEOUT_SEC, SLEEP_RANGE_SEC

log = logging.getLogger(__name__)


class Firecrawl:
    """
    Wrapper around Firecrawl SDK with auto-compat for different versions.
    - Reads API key from env (FIRECRAWL_API_KEY).
    - Provides scrape(url) that returns a dict containing 'html' / 'markdown' / 'raw_html' when possible.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
        sleep_range: Optional[Tuple[float, float]] = None,
    ):
        self.api_key = api_key or os.getenv("FIRECRAWL_API_KEY")
        if not self.api_key:
            raise RuntimeError("Missing FIRECRAWL_API_KEY (set in env or .env file)")

        self.timeout = int(timeout or REQUEST_TIMEOUT_SEC)
        self.sleep_range = sleep_range or SLEEP_RANGE_SEC

        # بعض الإصدارات تعرض FirecrawlApp، وأخرى Firecrawl فقط
        self.client = None
        if hasattr(firecrawl, "FirecrawlApp"):
            self.client = firecrawl.FirecrawlApp(api_key=self.api_key)
        elif hasattr(firecrawl, "Firecrawl"):
            self.client = firecrawl.Firecrawl(api_key=self.api_key)  # type: ignore
        else:
            raise RuntimeError("firecrawl SDK does not expose FirecrawlApp or Firecrawl class")

    def _normalize_result(self, res: Any) -> Dict[str, Any]:
        """
        حاول إرجاع dict فيها مفاتيح html/markdown/raw_html إن توفرت.
        """
        out: Dict[str, Any] = {}
        if res is None:
            return out

        # بعض الإصدارات ترجع dict جاهز
        if isinstance(res, dict):
            out = dict(res)
        # أو object فيه to_dict()
        elif hasattr(res, "to_dict"):
            try:
                out = dict(res.to_dict())  # type: ignore
            except Exception:
                out = {}

        # أسماء الحقول تختلف أحياناً؛ وحّدها قدر الإمكان
        # html/raw_html
        if "raw_html" not in out and "rawHtml" in out:
            out["raw_html"] = out.get("rawHtml")
        if "html" not in out and "content" in out and isinstance(out["content"], str):
            # fallback: بعض النسخ تضع الـ HTML في content
            out["html"] = out["content"]
        # markdown
        if "markdown" not in out and "md" in out:
            out["markdown"] = out.get("md")

        return out

    def scrape(self, url: str) -> Dict[str, Any]:
        """
        يحاول استدعاء الدالة الصحيحة حسب نسخة الـ SDK:
        - client.scrape_url(url=..., formats=[...], timeout=ms)
        - client.scrape(url=..., formats=[...], timeout=ms)
        - client.scrape(url) كـ fallback أخير
        ويرجّع dict موحّد المفاتيح قدر الإمكان.
        """
        last_err: Optional[Exception] = None
        # 1) scrape_url(...)
        try:
            if hasattr(self.client, "scrape_url"):
                res = self.client.scrape_url(  # type: ignore[attr-defined]
                    url=url,
                    formats=["html", "markdown", "raw_html"],
                    timeout=self.timeout * 1000,  # ms
                )
                return self._normalize_result(res)
        except Exception as e:
            last_err = e
            log.error(f"[firecrawl] scrape failed url={url} err={e}")

        # 2) scrape(url=..., formats=..., timeout=...)
        try:
            if hasattr(self.client, "scrape"):
                res = self.client.scrape(  # type: ignore[attr-defined]
                    url=url,
                    formats=["html", "markdown", "raw_html"],
                    timeout=self.timeout * 1000,
                )
                return self._normalize_result(res)
        except TypeError as e:
            # بعض الإصدارات لا تقبل هذي البراميترات
            last_err = e
        except Exception as e:
            last_err = e
            log.error(f"[firecrawl] scrape failed url={url} err={e}")

        # 3) scrape(url) فقط
        try:
            if hasattr(self.client, "scrape"):
                res = self.client.scrape(url)  # type: ignore[attr-defined]
                return self._normalize_result(res)
        except Exception as e:
            last_err = e
            log.error(f"[firecrawl] scrape failed url={url} err={e}")

        # لو فشل كل شيء، ارمِ آخر خطأ معروف
        if last_err:
            raise last_err
        raise RuntimeError("firecrawl scrape failed: no compatible method found")
