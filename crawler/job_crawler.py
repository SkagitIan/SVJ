from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urldefrag, urljoin, urlparse
from urllib.request import Request, urlopen

from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig
from openai import AsyncOpenAI
from playwright.async_api import async_playwright
from dotenv import load_dotenv

import db


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


CAREERS_TERMS = (
    "jobs",
    "job",
    "careers",
    "career",
    "employment",
    "hiring",
    "openings",
    "positions",
    "current openings",
    "join-us",
    "join us",
    "work-with-us",
    "work with us",
)

JOB_BOARD_HOSTS = (
    "workday",
    "greenhouse",
    "lever.co",
    "paylocity",
    "bamboohr",
    "icims",
    "adp",
    "indeed",
    "ultipro",
    "ukg",
    "recruiting2",
    "inforcloudsuite",
)

SOURCE_PRIORITIES = {
    "general_jobs": 100,
    "provider_jobs": 70,
    "internship": 35,
    "volunteer": 10,
    "culture": 5,
    "unrelated": 0,
    "unknown": 0,
}

BLOCKED_CANDIDATE_TERMS = (
    "privacy",
    "billing",
    "bill-pay",
    "financial-assistance",
    "insurance",
    "medicare",
    "analyst-reports",
    "industries/construction",
)

JOB_NOISE = (
    "view all",
    "search jobs",
    "job openings",
    "open positions",
    "current openings",
    "available positions",
    "see our current openings",
    "see current openings",
    "apply today",
    "learn more",
    "click here",
    "job alerts",
    "talent community",
    "privacy",
    "terms",
    "linkedin",
    "facebook",
    "instagram",
    "day in the life",
    "employee stories",
    "benefits",
    "equal opportunity",
)

GENERIC_JOB_TITLE_TERMS = (
    "opening",
    "opportunity",
    "apply",
    "learn more",
    "click here",
    "careers",
)

SALARY_RE = re.compile(
    r"(\$\s?\d{2,3}(?:,\d{3})?(?:\.\d{2})?\s*(?:-|to|/)\s*\$?\s?\d{2,3}(?:,\d{3})?(?:\.\d{2})?\s*(?:per\s+)?(?:hour|hr|year|yr|annually)?|\$\s?\d{2,3}(?:,\d{3})?(?:\.\d{2})?\s*(?:per\s+)?(?:hour|hr|year|yr|annually))",
    re.IGNORECASE,
)

LOCATION_RE = re.compile(
    r"\b([A-Z][A-Za-z .'-]+,\s*(?:WA|Washington|OR|Oregon|CA|California|ID|Idaho)|Remote|Hybrid)\b",
    re.IGNORECASE,
)

SKAGIT_LOCATION_TERMS = (
    "skagit county",
    "skagit valley",
    "anacortes",
    "burlington",
    "concrete",
    "hamilton",
    "la conner",
    "lyman",
    "mount vernon",
    "sedro-woolley",
    "sedro woolley",
    "bow",
    "edison",
    "marblemount",
    "rockport",
    "clear lake",
    "bay view",
    "fir island",
    "skagit",
)

NON_SKAGIT_LOCATION_RE = re.compile(
    r"\b[A-Z][A-Za-z .'-]+,\s*(?:AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WV|WI|WY|Oregon|California|Idaho)\b",
    re.IGNORECASE,
)


@dataclass
class JobPosting:
    job_title: str
    department: str | None
    salary_info: str | None
    location: str | None
    description: str | None
    application_url: str | None
    source_url: str
    source_type: str = "unknown"
    raw: dict[str, Any] | None = None


@dataclass
class DiscoveryCandidate:
    url: str
    text: str
    score: int
    source_type: str = "unknown"
    confidence: int = 0
    reason: str | None = None


@dataclass
class RenderedPage:
    url: str
    html: str


@dataclass
class CloudflareExtractionResult:
    mode: str
    jobs: list[JobPosting]
    source_url: str
    confidence: int
    evidence: list[str]
    browser_ms: int | None = None
    raw_result: dict[str, Any] | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_url(url: str, base_url: str | None = None) -> str | None:
    if not url:
        return None
    absolute = urljoin(base_url or url, url.strip())
    absolute, _fragment = urldefrag(absolute)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return absolute.rstrip("/")


def same_site(url: str, seed_url: str) -> bool:
    url_host = urlparse(url).netloc.lower().removeprefix("www.")
    seed_host = urlparse(seed_url).netloc.lower().removeprefix("www.")
    return url_host == seed_host or url_host.endswith("." + seed_host)


def is_site_root(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.path in {"", "/"} and not parsed.query


def clean_text(value: str | None) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def clean_optional_text(value: Any, limit: int = 1000) -> str | None:
    text = clean_text(str(value)) if value is not None else ""
    return text[:limit] if text else None


def normalize_title(value: str | None) -> str:
    normalized = clean_text(value).lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return clean_text(normalized)


def contains_skagit_location(value: str | None) -> bool:
    lowered = clean_text(value).lower()
    if not lowered:
        return False
    return any(term in lowered for term in SKAGIT_LOCATION_TERMS)


def has_explicit_non_skagit_location(value: str | None) -> bool:
    text = clean_text(value)
    if not text:
        return False
    if contains_skagit_location(text):
        return False
    return bool(NON_SKAGIT_LOCATION_RE.search(text))


def local_prefilter_job(job: JobPosting) -> tuple[bool, str | None]:
    location = clean_text(job.location or "")
    haystack = " ".join(
        part
        for part in (
            job.job_title,
            job.location,
            job.description,
            job.application_url,
            job.source_url,
        )
        if part
    )
    if has_explicit_non_skagit_location(location):
        return False, f"explicit non-Skagit location: {location}"
    if has_explicit_non_skagit_location(haystack) and not contains_skagit_location(haystack):
        return False, "explicit non-Skagit location in listing text"
    return True, None


def is_probable_job_title(text: str) -> bool:
    lowered = text.lower()
    if not text or len(text) < 4 or len(text) > 120:
        return False
    if any(noise in lowered for noise in JOB_NOISE):
        return False
    if lowered in CAREERS_TERMS:
        return False
    if lowered in {"job openings", "open positions", "current openings", "available positions"}:
        return False
    title_markers = (
        "manager",
        "assistant",
        "technician",
        "operator",
        "engineer",
        "specialist",
        "coordinator",
        "supervisor",
        "driver",
        "mechanic",
        "nurse",
        "cook",
        "server",
        "clerk",
        "associate",
        "machinist",
        "welder",
        "laborer",
        "worker",
        "representative",
        "developer",
        "analyst",
        "accountant",
        "administrator",
        "receptionist",
        "therapist",
        "aide",
    )
    return any(marker in lowered for marker in title_markers) or bool(
        re.search(r"\b(full|part)[-\s]?time\b", lowered)
    )


def is_generic_job_cta(text: str) -> bool:
    lowered = text.lower()
    return any(term in lowered for term in GENERIC_JOB_TITLE_TERMS) and not any(
        marker in lowered
        for marker in (
            "manager",
            "technician",
            "engineer",
            "specialist",
            "coordinator",
            "supervisor",
            "assistant",
            "nurse",
            "cook",
            "security",
            "accounting",
            "accountant",
            "clerk",
            "operator",
            "driver",
            "provider",
            "physician",
            "practitioner",
        )
    )


def page_markdown(result: Any) -> str:
    markdown = getattr(result, "markdown", "") or ""
    if isinstance(markdown, str):
        return markdown
    for attr in ("raw_markdown", "fit_markdown", "markdown"):
        value = getattr(markdown, attr, None)
        if isinstance(value, str):
            return value
    return str(markdown)


def page_html(result: Any) -> str:
    if isinstance(result, RenderedPage):
        return result.html
    return (
        getattr(result, "cleaned_html", None)
        or getattr(result, "html", None)
        or getattr(result, "raw_html", None)
        or ""
    )


def is_blocked_or_login_page(html: str) -> bool:
    lowered = clean_text(BeautifulSoup(html or "", "html.parser").get_text(" ")).lower()
    blocked_markers = (
        "attention required! | cloudflare",
        "cloudflare ray id",
        "checking if the site connection is secure",
        "blocked by anti-bot",
        "choose authentication",
    )
    return any(marker in lowered for marker in blocked_markers)


def candidate_score(url: str, text: str) -> int:
    haystack = f"{url} {text}".lower()
    score = 0
    for term in CAREERS_TERMS:
        term_pattern = re.escape(term).replace(r"\ ", r"[-\s]+")
        if re.search(rf"(?<![a-z]){term_pattern}(?![a-z])", haystack):
            score += 10
    if re.search(r"(?<![a-z])(apply|apply-today|apply today)(?![a-z])", haystack):
        score += 12
    if re.search(r"/(jobs|careers|employment|join|work)", url.lower()):
        score += 20
    if re.search(r"(job-openings|jobs|open-positions|available-positions)", url.lower()):
        score += 30
    if any(vendor in url.lower() for vendor in JOB_BOARD_HOSTS):
        score += 25
    if urlparse(url).query:
        score -= 2
    return score


def is_jobish_url(url: str) -> bool:
    lowered = url.lower()
    return any(host in lowered for host in JOB_BOARD_HOSTS) or bool(
        re.search(r"(job|opening|position|opportunit|requisition|posting|apply)", lowered)
    )


def is_allowed_discovered_url(url: str, seed_url: str, base_url: str) -> bool:
    parsed_host = urlparse(url).netloc.lower()
    base_host = urlparse(base_url).netloc.lower()
    return (
        same_site(url, seed_url)
        or parsed_host == base_host
        or any(host in parsed_host for host in JOB_BOARD_HOSTS)
    )


def collect_link_candidates(seed_url: str, html: str, base_url: str | None = None) -> list[DiscoveryCandidate]:
    base_url = base_url or seed_url
    soup = BeautifulSoup(html, "html.parser")
    scored: dict[str, DiscoveryCandidate] = {}
    for anchor in soup.find_all("a"):
        href = anchor.get("href")
        url = normalize_url(href, base_url)
        if not url:
            continue
        lowered_url = url.lower()
        if any(term in lowered_url for term in BLOCKED_CANDIDATE_TERMS):
            continue
        if not is_allowed_discovered_url(url, seed_url, base_url):
            continue
        text = clean_text(anchor.get_text(" "))
        score = candidate_score(url, text)
        if score > 0:
            current = scored.get(url)
            if not current or score > current.score:
                scored[url] = DiscoveryCandidate(url=url, text=text[:200], score=score)
    return sorted(scored.values(), key=lambda candidate: candidate.score, reverse=True)


async def ai_discover_candidate_urls(
    client: AsyncOpenAI,
    model: str,
    seed_url: str,
    current_url: str,
    html: str,
    max_links: int,
) -> list[DiscoveryCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text(" "))[:8000]
    links = collect_link_candidates(seed_url, html, current_url)
    if not links:
        return []
    link_payload = [asdict(candidate) for candidate in links[:max_links]]
    response = await client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "You are a precise careers-page discovery agent. Classify links into channels and rank what to crawl next. "
                    "The main goal is broad employment listings for regular staff jobs such as facilities, food service, "
                    "security, accounting, registration, medical assistants, nurses, technicians, and administration. "
                    "Provider recruitment pages for physicians, ARNPs, PAs, and specialists are useful but secondary. "
                    "Avoid volunteer, job shadow, culture, benefits, privacy, billing, news, and unrelated vendor pages. "
                    "Return only valid JSON."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "seed_url": seed_url,
                        "current_url": current_url,
                        "page_text": page_text,
                        "links": link_payload,
                        "output_schema": {
                            "candidates": [
                                {
                                    "url": "absolute URL from links",
                                    "source_type": "general_jobs | provider_jobs | internship | volunteer | culture | unrelated | unknown",
                                    "confidence": "integer 0-100",
                                    "reason": "short reason",
                                }
                            ]
                        },
                    },
                    ensure_ascii=False,
                ),
            },
        ],
        text={"format": {"type": "json_object"}},
    )
    raw_text = response.output_text
    data = json.loads(raw_text)
    candidates = data.get("candidates", [])
    if not isinstance(candidates, list):
        return []
    by_url = {candidate.url: candidate for candidate in links}
    ordered: list[str] = []
    selected: list[DiscoveryCandidate] = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        normalized = normalize_url(str(item.get("url") or ""), current_url)
        if not normalized or normalized not in by_url or normalized in ordered:
            continue
        source_type = str(item.get("source_type") or "unknown")
        if source_type not in SOURCE_PRIORITIES:
            source_type = "unknown"
        try:
            confidence = int(item.get("confidence") or 0)
        except (TypeError, ValueError):
            confidence = 0
        base = by_url[normalized]
        selected.append(
            DiscoveryCandidate(
                url=normalized,
                text=base.text,
                score=base.score,
                source_type=source_type,
                confidence=max(0, min(100, confidence)),
                reason=clean_text(str(item.get("reason") or "")) or None,
            )
        )
        ordered.append(normalized)
    selected.sort(
        key=lambda candidate: (
            SOURCE_PRIORITIES.get(candidate.source_type, 0),
            candidate.confidence,
            candidate.score,
        ),
        reverse=True,
    )
    return selected


async def ai_validate_job_source(
    client: AsyncOpenAI,
    model: str,
    seed_url: str,
    page_url: str,
    html: str,
    jobs: list[JobPosting],
) -> dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = clean_text(soup.get_text(" "))[:8000]
    links = [asdict(candidate) for candidate in collect_link_candidates(seed_url, html, page_url)[:30]]
    job_samples = [
        {
            "job_title": job.job_title,
            "location": job.location,
            "application_url": job.application_url,
        }
        for job in jobs[:15]
    ]
    response = await client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "You validate whether a page is an actual source of open job postings. "
                    "Classify the page as general_jobs for broad staff employment listings, provider_jobs for physician/APP/provider recruitment, "
                    "internship, volunteer, culture, unrelated, or unknown. Prefer general_jobs when the page points to or contains broad employment "
                    "roles like cooks, security, accounting, cleaning, registration, nurses, technicians, and admin. "
                    "Return only valid JSON."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "seed_url": seed_url,
                        "page_url": page_url,
                        "page_text": page_text,
                        "sample_extracted_jobs": job_samples,
                        "sample_links": links,
                        "output_schema": {
                            "has_jobs": "boolean",
                            "source_type": "general_jobs | provider_jobs | internship | volunteer | culture | unrelated | unknown",
                            "confidence": "integer 0-100",
                            "evidence": ["short evidence strings"],
                        },
                    },
                    ensure_ascii=False,
                ),
            },
        ],
        text={"format": {"type": "json_object"}},
    )
    data = json.loads(response.output_text)
    source_type = str(data.get("source_type") or "unknown")
    if source_type not in SOURCE_PRIORITIES:
        source_type = "unknown"
    try:
        confidence = int(data.get("confidence") or 0)
    except (TypeError, ValueError):
        confidence = 0
    return {
        "url": page_url,
        "has_jobs": bool(data.get("has_jobs")),
        "source_type": source_type,
        "confidence": max(0, min(100, confidence)),
        "evidence": data.get("evidence") if isinstance(data.get("evidence"), list) else [],
        "job_count": len(jobs),
    }


def extract_json_ld_jobs(html: str, source_url: str) -> list[JobPosting]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: list[JobPosting] = []
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            payload = json.loads(script.string or "")
        except json.JSONDecodeError:
            continue
        nodes = payload if isinstance(payload, list) else [payload]
        for node in nodes:
            if isinstance(node, dict) and node.get("@graph"):
                nodes.extend(node["@graph"])
            if not isinstance(node, dict):
                continue
            kind = node.get("@type")
            if kind == "JobPosting" or (isinstance(kind, list) and "JobPosting" in kind):
                salary = node.get("baseSalary")
                if isinstance(salary, dict):
                    salary = json.dumps(salary, separators=(",", ":"))
                apply_url = node.get("url") or source_url
                location = json_ld_location(node.get("jobLocation"))
                jobs.append(
                    JobPosting(
                        job_title=clean_text(node.get("title")),
                        department=clean_text(node.get("employmentType")) or None,
                        salary_info=clean_text(str(salary)) if salary else None,
                        location=location,
                        description=clean_text(BeautifulSoup(node.get("description") or "", "html.parser").get_text(" ")) or None,
                        application_url=normalize_url(apply_url, source_url),
                        source_url=source_url,
                        source_type="unknown",
                        raw=node,
                    )
                )
    return [job for job in jobs if job.job_title]


def json_ld_location(value: Any) -> str | None:
    if isinstance(value, list):
        locations = [json_ld_location(item) for item in value]
        return "; ".join(location for location in locations if location) or None
    if not isinstance(value, dict):
        return clean_text(str(value)) if value else None
    address = value.get("address") if isinstance(value.get("address"), dict) else value
    parts = [
        address.get("streetAddress"),
        address.get("addressLocality"),
        address.get("addressRegion"),
        address.get("postalCode"),
    ]
    return clean_text(", ".join(str(part) for part in parts if part)) or None


def nearest_salary(anchor: Any) -> str | None:
    container = anchor
    for _ in range(3):
        container = container.parent if container else None
        if not container:
            break
        match = SALARY_RE.search(clean_text(container.get_text(" ")))
        if match:
            return clean_text(match.group(1))
    return None


def nearest_location(anchor: Any) -> str | None:
    container = anchor
    for _ in range(4):
        container = container.parent if container else None
        if not container:
            break
        match = LOCATION_RE.search(clean_text(container.get_text(" ")))
        if match:
            return clean_text(match.group(1))
    return None


def extract_anchor_jobs(html: str, source_url: str) -> list[JobPosting]:
    soup = BeautifulSoup(html, "html.parser")
    jobs: list[JobPosting] = []
    for anchor in soup.find_all("a"):
        text = clean_text(anchor.get_text(" "))
        href = normalize_url(anchor.get("href"), source_url)
        haystack = f"{text} {href or ''}".lower()
        if not href:
            continue
        if is_generic_job_cta(text):
            continue
        if text.lower() in {"job openings", "open positions", "current openings", "available positions"}:
            continue
        if ((is_probable_job_title(text) and is_jobish_url(href)) or any(
            term in haystack for term in ("jobid", "job_id", "requisition", "opening", "position")
        )) and (is_jobish_url(source_url) or is_jobish_url(href)):
            jobs.append(
                JobPosting(
                    job_title=text[:120],
                    department=None,
                    salary_info=nearest_salary(anchor),
                    location=nearest_location(anchor),
                    description=None,
                    application_url=href,
                    source_url=source_url,
                    source_type="unknown",
                )
            )
    return jobs


def extract_jobs(html: str, source_url: str) -> list[JobPosting]:
    seen: set[tuple[str, str | None]] = set()
    jobs: list[JobPosting] = []
    for job in [*extract_json_ld_jobs(html, source_url), *extract_anchor_jobs(html, source_url)]:
        key = (job.job_title.lower(), job.application_url)
        if key in seen:
            continue
        seen.add(key)
        jobs.append(job)
    return jobs


def page_text_sample(html: str, limit: int = 10000) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for unwanted in soup.select("script, style, nav, header, footer, form"):
        unwanted.decompose()
    return clean_text(soup.get_text(" "))[:limit]


CLOUDFLARE_JOB_SCHEMA = {
    "type": "object",
    "properties": {
        "jobs": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "job_title": {"type": "string"},
                    "department": {"type": ["string", "null"]},
                    "salary_info": {"type": ["string", "null"]},
                    "location": {"type": ["string", "null"]},
                    "description": {"type": ["string", "null"]},
                    "application_url": {"type": ["string", "null"]},
                    "source_url": {"type": ["string", "null"]},
                },
                "required": ["job_title"],
            },
        },
        "evidence": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "integer"},
    },
    "required": ["jobs"],
}


def cloudflare_job_prompt(seed: dict[str, Any]) -> str:
    return (
        "Extract active job postings from this company's own careers page or a company-linked ATS widget. "
        "Return only real active paid roles, not navigation links, benefits pages, talent communities, volunteer pages, "
        "generic apply calls, LinkedIn, Indeed, Glassdoor, or unrelated job boards. "
        "Prefer jobs in or clearly relevant to Skagit Valley, Washington when location is present. "
        f"Company: {seed.get('business_name') or 'unknown'}. "
        f"Homepage: {seed.get('homepage_url') or seed.get('seed_url') or 'unknown'}. "
        "For each job include the title, apply URL, location, department, salary, and a short description when visible."
    )


def cloudflare_account_and_token() -> tuple[str, str]:
    load_dotenv(Path(__file__).with_name(".env"))
    account_id = os.environ.get("CLOUDFLARE_ACCOUNT_ID") or os.environ.get("CF_ACCOUNT_ID")
    token = os.environ.get("CLOUDFLARE_API_TOKEN") or os.environ.get("CF_API_TOKEN") or os.environ.get("CLOUDFLARE_API_KEY")
    if not account_id or not token:
        missing = []
        if not account_id:
            missing.append("CLOUDFLARE_ACCOUNT_ID")
        if not token:
            missing.append("CLOUDFLARE_API_TOKEN or CLOUDFLARE_API_KEY")
        raise RuntimeError(f"{' and '.join(missing)} required for Cloudflare Browser Run.")
    return account_id, token


def cloudflare_request(method: str, path: str, payload: dict[str, Any] | None = None, timeout: int = 90) -> tuple[dict[str, Any], int | None]:
    account_id, token = cloudflare_account_and_token()
    url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/browser-rendering/{path.lstrip('/')}"
    body = json.dumps(payload).encode("utf-8") if payload is not None else None
    request = Request(
        url,
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
            browser_ms = response.headers.get("X-Browser-Ms-Used")
    except HTTPError as exc:
        message = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Cloudflare Browser Run {path} failed with HTTP {exc.code}: {message[:800]}") from exc
    except URLError as exc:
        raise RuntimeError(f"Cloudflare Browser Run {path} failed: {exc.reason}") from exc
    data = json.loads(raw or "{}")
    if not data.get("success", False):
        raise RuntimeError(f"Cloudflare Browser Run {path} failed: {data.get('errors') or data}")
    try:
        return data, int(browser_ms) if browser_ms else None
    except ValueError:
        return data, None


def cloudflare_json_payload(seed: dict[str, Any], url: str) -> dict[str, Any]:
    return {
        "url": url,
        "prompt": cloudflare_job_prompt(seed),
        "response_format": {
            "type": "json_schema",
            "schema": CLOUDFLARE_JOB_SCHEMA,
        },
        "gotoOptions": {"waitUntil": "domcontentloaded", "timeout": 45000},
        "waitForTimeout": int(os.environ.get("CLOUDFLARE_DISCOVERY_SETTLE_MS", "5000")),
    }


def cloudflare_crawl_payload(seed: dict[str, Any], url: str) -> dict[str, Any]:
    return {
        "url": url,
        "limit": int(os.environ.get("CLOUDFLARE_DISCOVERY_CRAWL_LIMIT", "6")),
        "depth": int(os.environ.get("CLOUDFLARE_DISCOVERY_CRAWL_DEPTH", "2")),
        "formats": ["json"],
        "render": True,
        "source": "links",
        "crawlPurposes": ["search"],
        "jsonOptions": {
            "prompt": cloudflare_job_prompt(seed),
            "response_format": {
                "type": "json_schema",
                "schema": CLOUDFLARE_JOB_SCHEMA,
            },
        },
        "options": {
            "includeExternalLinks": True,
            "includeSubdomains": True,
            "includePatterns": ["**/career**", "**/job**", "**/employment**", "**/opening**", "**/position**"],
            "excludePatterns": ["**/privacy**", "**/terms**", "**/blog**", "**/news**"],
        },
    }


def find_cloudflare_job_payloads(value: Any) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    if isinstance(value, dict):
        jobs = value.get("jobs")
        if isinstance(jobs, list):
            payloads.append(value)
        for child in value.values():
            payloads.extend(find_cloudflare_job_payloads(child))
    elif isinstance(value, list):
        for item in value:
            payloads.extend(find_cloudflare_job_payloads(item))
    return payloads


def job_postings_from_cloudflare_payload(payload: dict[str, Any], default_url: str, source_type: str) -> list[JobPosting]:
    raw_jobs = payload.get("jobs")
    if not isinstance(raw_jobs, list):
        return []
    jobs: list[JobPosting] = []
    seen: set[tuple[str, str | None]] = set()
    for item in raw_jobs:
        if not isinstance(item, dict):
            continue
        title = clean_text(str(item.get("job_title") or item.get("title") or ""))
        if not title or is_generic_job_cta(title) or not is_probable_job_title(title):
            continue
        source_url = normalize_url(str(item.get("source_url") or ""), default_url) or default_url
        application_url = normalize_url(str(item.get("application_url") or item.get("apply_url") or ""), source_url) or source_url
        key = (normalize_title(title), application_url)
        if key in seen:
            continue
        seen.add(key)
        jobs.append(
            JobPosting(
                job_title=title[:120],
                department=clean_optional_text(item.get("department"), 240),
                salary_info=clean_optional_text(item.get("salary_info") or item.get("salary"), 240),
                location=clean_optional_text(item.get("location"), 240),
                description=clean_optional_text(item.get("description"), 12000),
                application_url=application_url,
                source_url=source_url,
                source_type=source_type,
                raw={"cloudflare_extracted": True, "cloudflare_job": item},
            )
        )
    return jobs


def normalize_cloudflare_result(mode: str, result: dict[str, Any], source_url: str, source_type: str, browser_ms: int | None) -> CloudflareExtractionResult:
    payloads = find_cloudflare_job_payloads(result.get("result"))
    jobs: list[JobPosting] = []
    evidence: list[str] = []
    confidence = 0
    for payload in payloads:
        jobs.extend(job_postings_from_cloudflare_payload(payload, source_url, source_type))
        if isinstance(payload.get("evidence"), list):
            evidence.extend(clean_text(str(item)) for item in payload["evidence"] if clean_text(str(item)))
        try:
            confidence = max(confidence, int(payload.get("confidence") or 0))
        except (TypeError, ValueError):
            pass
    unique_jobs: list[JobPosting] = []
    seen: set[tuple[str, str | None]] = set()
    for job in jobs:
        key = (normalize_title(job.job_title), job.application_url)
        if key in seen:
            continue
        seen.add(key)
        unique_jobs.append(job)
    if unique_jobs and not evidence:
        evidence.append(f"Cloudflare Browser Run {mode} extracted {len(unique_jobs)} active job posting(s).")
    if unique_jobs and confidence <= 0:
        confidence = 85
    return CloudflareExtractionResult(
        mode=mode,
        jobs=unique_jobs,
        source_url=source_url,
        confidence=confidence,
        evidence=evidence[:20],
        browser_ms=browser_ms,
        raw_result=result.get("result") if isinstance(result.get("result"), dict) else {"result": result.get("result")},
    )


def cloudflare_extract_jobs(seed: dict[str, Any]) -> CloudflareExtractionResult:
    source_url = normalize_url(seed.get("jobs_url") or seed.get("job_source_url") or seed.get("seed_url") or "")
    if not source_url:
        raise ValueError("Cloudflare extraction requires a jobs_url.")
    source_type = source_type_from_seed(seed)
    json_result, json_browser_ms = cloudflare_request("POST", "json", cloudflare_json_payload(seed, source_url))
    extracted = normalize_cloudflare_result("json", json_result, source_url, source_type, json_browser_ms)
    if extracted.jobs:
        return extracted

    crawl_result, crawl_browser_ms = cloudflare_request("POST", "crawl", cloudflare_crawl_payload(seed, source_url), timeout=60)
    job_id = crawl_result.get("result")
    if not isinstance(job_id, str) or not job_id:
        raise RuntimeError(f"Cloudflare crawl did not return a job id: {crawl_result}")
    max_attempts = int(os.environ.get("CLOUDFLARE_DISCOVERY_CRAWL_ATTEMPTS", "24"))
    delay_seconds = float(os.environ.get("CLOUDFLARE_DISCOVERY_CRAWL_DELAY", "2.5"))
    final_result: dict[str, Any] | None = None
    poll_browser_ms = 0
    for _ in range(max_attempts):
        time.sleep(delay_seconds)
        poll_result, browser_ms = cloudflare_request("GET", f"crawl/{job_id}?limit=20", timeout=60)
        if browser_ms:
            poll_browser_ms += browser_ms
        status = poll_result.get("result", {}).get("status") if isinstance(poll_result.get("result"), dict) else None
        if status and status != "running":
            final_result = poll_result
            break
    if final_result is None:
        raise RuntimeError("Cloudflare crawl did not complete before the local timeout.")
    status = final_result.get("result", {}).get("status") if isinstance(final_result.get("result"), dict) else None
    if status != "completed":
        raise RuntimeError(f"Cloudflare crawl ended with status {status}.")
    return normalize_cloudflare_result("crawl", final_result, source_url, source_type, (crawl_browser_ms or 0) + poll_browser_ms)


async def process_seed_with_cloudflare(seed: dict[str, Any]) -> dict[str, Any]:
    extracted = await asyncio.to_thread(cloudflare_extract_jobs, seed)
    timestamp = now_iso()
    jobs = [asdict(job) for job in extracted.jobs]
    seed.update(
        {
            "seed_url": normalize_url(seed.get("seed_url") or seed.get("homepage_url") or seed.get("jobs_url")),
            "homepage_url": normalize_url(seed.get("homepage_url") or seed.get("seed_url") or seed.get("jobs_url")),
            "jobs_url": extracted.source_url,
            "jobs": jobs,
            "job_sources": [
                {
                    "url": extracted.source_url,
                    "source_type": source_type_from_seed(seed),
                    "confidence": extracted.confidence,
                    "job_count": len(jobs),
                    "evidence": extracted.evidence,
                    "extraction_provider": "cloudflare",
                    "extraction_mode": extracted.mode,
                    "provider_config": {
                        "browser_ms": extracted.browser_ms,
                    },
                }
            ],
            "primary_source_type": source_type_from_seed(seed),
            "last_status": "ok",
            "last_checked_at": timestamp,
            "error": None,
            "extraction_provider": "cloudflare",
            "extraction_mode": extracted.mode,
            "provider_config": {
                "browser_ms": extracted.browser_ms,
                "raw_result_sample": extracted.raw_result,
            },
            "last_job_count": len(jobs),
            "no_jobs_verified": len(jobs) == 0,
            "no_jobs_verified_at": timestamp if len(jobs) == 0 else None,
            "no_jobs_note": "Cloudflare Browser Run found the careers page but extracted no active job listings." if len(jobs) == 0 else None,
            "debug": {
                "provider": "cloudflare",
                "mode": extracted.mode,
                "browser_ms": extracted.browser_ms,
                "verified_job_count": len(jobs),
            },
        }
    )
    return seed


async def ai_extract_jobs_from_page(
    client: AsyncOpenAI,
    model: str,
    seed_url: str,
    page_url: str,
    html: str,
    source_type: str,
    fallback_location: str | None,
) -> list[JobPosting]:
    text = page_text_sample(html, 12000)
    links = [
        {
            "url": candidate.url,
            "text": candidate.text,
        }
        for candidate in collect_link_candidates(seed_url, html, page_url)[:40]
    ]
    response = await client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "Extract active job postings from a company careers or job listing page. "
                    "Return jobs only when the page contains a real open role or a clear link to a real open role. "
                    "Do not extract volunteer pages, job shadows, benefits pages, culture pages, privacy pages, or generic navigation. "
                    "For provider recruitment pages, extract the provider role and mark source_type provider_jobs. "
                    "For broad staff employment listings, mark source_type general_jobs. Return only valid JSON."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "seed_url": seed_url,
                        "page_url": page_url,
                        "suggested_source_type": source_type,
                        "fallback_location": fallback_location,
                        "page_text": text,
                        "links": links,
                        "output_schema": {
                            "jobs": [
                                {
                                    "job_title": "string",
                                    "department": "string or null",
                                    "salary_info": "string or null",
                                    "location": "string or null",
                                    "description": "string or null",
                                    "application_url": "absolute URL or null",
                                    "source_type": "general_jobs | provider_jobs | internship | unknown",
                                }
                            ]
                        },
                    },
                    ensure_ascii=False,
                ),
            },
        ],
        text={"format": {"type": "json_object"}},
    )
    data = json.loads(response.output_text)
    raw_jobs = data.get("jobs", [])
    if not isinstance(raw_jobs, list):
        return []
    jobs: list[JobPosting] = []
    for item in raw_jobs:
        if not isinstance(item, dict):
            continue
        title = clean_text(str(item.get("job_title") or ""))
        if not title or is_generic_job_cta(title) or not is_probable_job_title(title):
            continue
        item_source_type = str(item.get("source_type") or source_type or "unknown")
        if item_source_type not in SOURCE_PRIORITIES:
            item_source_type = source_type or "unknown"
        jobs.append(
            JobPosting(
                job_title=title[:120],
                department=clean_text(str(item.get("department") or "")) or None,
                salary_info=clean_text(str(item.get("salary_info") or "")) or None,
                location=clean_text(str(item.get("location") or "")) or fallback_location,
                description=clean_text(str(item.get("description") or ""))[:12000] or None,
                application_url=normalize_url(str(item.get("application_url") or ""), page_url) or page_url,
                source_url=page_url,
                source_type=item_source_type,
                raw={"ai_extracted": True},
            )
        )
    return jobs


async def ai_verify_job_for_skagit(
    client: AsyncOpenAI,
    model: str,
    company_name_value: str,
    company_location: str | None,
    job: JobPosting,
) -> tuple[bool, dict[str, Any]]:
    response = await client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": (
                    "Verify job listings for a Skagit County, Washington jobs site. "
                    "Return only valid JSON. Approve only if this is a real open job listing and the work location is in Skagit County, WA "
                    "or in one of its cities/communities such as Mount Vernon, Burlington, Anacortes, Sedro-Woolley, Concrete, Hamilton, "
                    "La Conner, Lyman, Bow, Edison, Marblemount, Rockport, Clear Lake, or Bay View. "
                    "Reject remote-only jobs and jobs in other places such as Florence OR, Bellingham WA, Seattle WA, Everett WA, or Portland OR."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(
                    {
                        "company": company_name_value,
                        "company_location_fallback": company_location,
                        "job": {
                            "title": job.job_title,
                            "department": job.department,
                            "location": job.location,
                            "description": (job.description or "")[:6000],
                            "application_url": job.application_url,
                            "source_url": job.source_url,
                        },
                        "output_schema": {
                            "is_real_job": "boolean",
                            "is_in_skagit_county": "boolean",
                            "normalized_location": "string or null",
                            "reason": "short string",
                        },
                    },
                    ensure_ascii=False,
                ),
            },
        ],
        text={"format": {"type": "json_object"}},
    )
    data = json.loads(response.output_text)
    approved = bool(data.get("is_real_job")) and bool(data.get("is_in_skagit_county"))
    normalized_location = clean_text(str(data.get("normalized_location") or ""))
    if approved and normalized_location:
        job.location = normalized_location
    raw = job.raw or {}
    raw["verification"] = {
        "model": model,
        "approved": approved,
        "is_real_job": bool(data.get("is_real_job")),
        "is_in_skagit_county": bool(data.get("is_in_skagit_county")),
        "reason": clean_text(str(data.get("reason") or "")),
    }
    job.raw = raw
    return approved, raw["verification"]


async def verify_and_dedupe_jobs(
    client: AsyncOpenAI,
    model: str,
    seed: dict[str, Any],
    jobs: list[JobPosting],
) -> tuple[list[JobPosting], list[dict[str, Any]]]:
    verified: list[JobPosting] = []
    rejected: list[dict[str, Any]] = []
    seen_titles: set[str] = set()
    seed_url = normalize_url(seed.get("seed_url") or seed.get("homepage_url") or seed.get("url")) or ""
    company = company_name(seed, seed_url) if seed_url else clean_text(str(seed.get("business_name") or ""))
    fallback_location = fallback_company_location(seed)

    for job in jobs:
        title_key = normalize_title(job.job_title)
        if not title_key:
            continue
        if title_key in seen_titles:
            rejected.append({"job_title": job.job_title, "reason": "duplicate title for company"})
            continue
        prefilter_ok, prefilter_reason = local_prefilter_job(job)
        if not prefilter_ok:
            rejected.append({"job_title": job.job_title, "location": job.location, "reason": prefilter_reason})
            continue
        approved, verification = await ai_verify_job_for_skagit(client, model, company, fallback_location, job)
        if not approved:
            rejected.append(
                {
                    "job_title": job.job_title,
                    "location": job.location,
                    "reason": verification.get("reason") or "AI verification rejected listing",
                }
            )
            continue
        job.location = job.location or fallback_location
        seen_titles.add(title_key)
        verified.append(job)
    return verified, rejected


def extract_page_description(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for selector in (
        "[data-automation-id='jobPostingDescription']",
        "[data-testid='job-description']",
        ".job-description",
        ".jobDescription",
        "#job-description",
        "main",
        "article",
        "body",
    ):
        node = soup.select_one(selector)
        if not node:
            continue
        for unwanted in node.select("script, style, nav, header, footer, form"):
            unwanted.decompose()
        text = clean_text(node.get_text(" "))
        if len(text) > 120:
            return text[:12000]
    return None


def extract_page_location(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    for selector in (
        "[data-automation-id='locations']",
        "[data-testid='job-location']",
        ".job-location",
        ".location",
        "[class*='location']",
    ):
        node = soup.select_one(selector)
        if not node:
            continue
        text = clean_text(node.get_text(" "))
        match = LOCATION_RE.search(text)
        if match:
            return clean_text(match.group(1))
        if 2 <= len(text) <= 100:
            return text
    match = LOCATION_RE.search(clean_text(soup.get_text(" ")))
    return clean_text(match.group(1)) if match else None


async def enrich_job_details(
    crawler: AsyncWebCrawler,
    jobs: list[JobPosting],
    fallback_location: str | None,
    max_detail_pages: int,
) -> list[JobPosting]:
    enriched: list[JobPosting] = []
    detail_count = 0
    for job in jobs:
        if job.application_url and detail_count < max_detail_pages:
            try:
                result = await crawl_page(crawler, job.application_url)
                html = page_html(result)
                job.description = job.description or extract_page_description(html)
                job.location = job.location or extract_page_location(html)
                detail_count += 1
            except Exception as exc:
                raw = job.raw or {}
                raw["detail_error"] = str(exc)
                job.raw = raw
        job.location = job.location or fallback_location
        enriched.append(job)
    return enriched


async def crawl_page(crawler: AsyncWebCrawler, url: str) -> Any:
    configs = [
        CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            word_count_threshold=1,
            remove_overlay_elements=True,
            remove_consent_popups=True,
            wait_until="domcontentloaded",
            delay_before_return_html=0.3,
            page_timeout=45000,
        ),
        CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            word_count_threshold=1,
            remove_overlay_elements=True,
            remove_consent_popups=True,
            magic=True,
            simulate_user=True,
            override_navigator=True,
            wait_until="networkidle",
            delay_before_return_html=1.0,
            page_timeout=60000,
        ),
    ]
    last_error = "crawl failed"
    for config in configs:
        result = await crawler.arun(url=url, config=config)
        html = page_html(result)
        if is_blocked_or_login_page(html):
            last_error = "blocked or login page returned"
            continue
        if getattr(result, "success", False) or len(html) >= 250:
            return result
        last_error = getattr(result, "error_message", "crawl failed")
    raise RuntimeError(last_error)


def pagination_urls(html: str, current_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a"):
        text = clean_text(anchor.get_text(" ")).lower()
        rel = " ".join(anchor.get("rel") or []).lower()
        aria = clean_text(anchor.get("aria-label")).lower()
        href = normalize_url(anchor.get("href"), current_url)
        if not href or href in seen:
            continue
        candidate_text = " ".join([text, rel, aria, href.lower()])
        is_next = any(
            marker in candidate_text
            for marker in (
                "next",
                "more",
                "page=",
                "pagenumber",
                "offset=",
                "start=",
                "from=",
            )
        )
        is_page_number = bool(re.fullmatch(r"\d{1,3}", text))
        if is_next or is_page_number:
            seen.add(href)
            urls.append(href)
    return urls


def linked_job_source_urls(html: str, current_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    cta_terms = (
        "view all jobs",
        "see current openings",
        "see our current openings",
        "current openings",
        "job openings",
        "search jobs",
        "apply now",
    )
    for anchor in soup.find_all("a"):
        href = normalize_url(anchor.get("href"), current_url)
        if not href or href in seen or href == current_url:
            continue
        text = clean_text(anchor.get_text(" ")).lower()
        href_lower = href.lower()
        if any(host in href_lower for host in JOB_BOARD_HOSTS) or any(term in text for term in cta_terms):
            seen.add(href)
            urls.append(href)
    return urls


async def rendered_page_content(page: Any, url: str) -> RenderedPage:
    await page.goto(url, wait_until="domcontentloaded", timeout=60000)
    try:
        await page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    for _ in range(3):
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_timeout(750)
    html = await page.content()
    if is_blocked_or_login_page(html):
        raise RuntimeError("blocked or login page returned")
    return RenderedPage(url=page.url, html=html)


async def collect_rendered_listing_pages(start_url: str, max_pages: int) -> list[RenderedPage]:
    pages: list[RenderedPage] = []
    queued: list[str] = [start_url]
    seen: set[str] = set()
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            while queued and len(pages) < max_pages:
                url = queued.pop(0)
                if url in seen:
                    continue
                seen.add(url)
                rendered = await rendered_page_content(page, url)
                pages.append(rendered)
                for next_url in pagination_urls(rendered.html, rendered.url):
                    if next_url not in seen and next_url not in queued and len(seen) + len(queued) < max_pages:
                        queued.append(next_url)
                for source_url in linked_job_source_urls(rendered.html, rendered.url):
                    if source_url not in seen and source_url not in queued and len(seen) + len(queued) < max_pages:
                        queued.append(source_url)
        finally:
            await browser.close()
    return pages


async def enrich_job_details_with_playwright(
    jobs: list[JobPosting],
    max_detail_pages: int,
) -> list[JobPosting]:
    enriched: list[JobPosting] = []
    detail_count = 0
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            for job in jobs:
                detail_url = job.application_url
                if detail_url and detail_url != job.source_url and detail_count < max_detail_pages:
                    try:
                        rendered = await rendered_page_content(page, detail_url)
                        job.description = job.description or extract_page_description(rendered.html)
                        job.location = job.location or extract_page_location(rendered.html)
                        detail_count += 1
                    except Exception as exc:
                        raw = job.raw or {}
                        raw["detail_error"] = str(exc)
                        job.raw = raw
                enriched.append(job)
        finally:
            await browser.close()
    return enriched


def fallback_company_location(seed: dict[str, Any]) -> str | None:
    explicit = seed.get("location") or seed.get("company_location")
    if explicit:
        return clean_text(str(explicit))
    city = clean_text(str(seed.get("city") or ""))
    state = clean_text(str(seed.get("state") or seed.get("region") or ""))
    if city and state:
        return f"{city}, {state}"
    return city or state or None


def company_name(seed: dict[str, Any], seed_url: str) -> str:
    return clean_text(str(seed.get("business_name") or seed.get("company_name") or seed.get("name") or "")) or urlparse(seed_url).netloc


def source_type_from_seed(seed: dict[str, Any]) -> str:
    source_type = str(seed.get("source_type") or "general_jobs")
    return source_type if source_type in SOURCE_PRIORITIES else "general_jobs"


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with db.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS companies (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seed_url TEXT NOT NULL UNIQUE,
                business_name TEXT NOT NULL,
                city TEXT,
                state TEXT,
                location TEXT,
                industry TEXT,
                homepage_url TEXT,
                jobs_url TEXT,
                source_type TEXT NOT NULL DEFAULT 'general_jobs',
                extraction_provider TEXT,
                extraction_mode TEXT,
                provider_config TEXT,
                last_job_count INTEGER NOT NULL DEFAULT 0,
                last_checked_at TEXT,
                last_status TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        company_columns = {row[1] for row in conn.execute("PRAGMA table_info(companies)")}
        for column, definition in {
            "industry": "TEXT",
            "homepage_url": "TEXT",
            "source_type": "TEXT NOT NULL DEFAULT 'general_jobs'",
            "extraction_provider": "TEXT",
            "extraction_mode": "TEXT",
            "provider_config": "TEXT",
            "last_job_count": "INTEGER NOT NULL DEFAULT 0",
            "is_featured": "INTEGER NOT NULL DEFAULT 0",
            "summary": "TEXT",
            "hiring_summary": "TEXT",
            "job_categories": "TEXT NOT NULL DEFAULT '[]'",
            "common_job_titles": "TEXT NOT NULL DEFAULT '[]'",
            "search_keywords": "TEXT NOT NULL DEFAULT '[]'",
            "career_page_quality": "TEXT",
            "ai_confidence_score": "INTEGER NOT NULL DEFAULT 0",
            "needs_manual_review": "INTEGER NOT NULL DEFAULT 0",
            "no_jobs_verified": "INTEGER NOT NULL DEFAULT 0",
            "no_jobs_verified_at": "TEXT",
            "no_jobs_note": "TEXT",
            "ai_enriched_at": "TEXT",
        }.items():
            if column not in company_columns:
                conn.execute(f"ALTER TABLE companies ADD COLUMN {column} {definition}")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_company_enrichment_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                openai_batch_id TEXT,
                input_file_id TEXT,
                output_file_id TEXT,
                error_file_id TEXT,
                status TEXT NOT NULL DEFAULT 'local_created',
                model TEXT,
                total_count INTEGER NOT NULL DEFAULT 0,
                completed_count INTEGER NOT NULL DEFAULT 0,
                failed_count INTEGER NOT NULL DEFAULT 0,
                message TEXT,
                imported INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                submitted_at TEXT,
                checked_at TEXT,
                completed_at TEXT,
                imported_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_company_enrichment_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL REFERENCES ai_company_enrichment_batches(id),
                company_id INTEGER NOT NULL REFERENCES companies(id),
                custom_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                raw_response_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(batch_id, custom_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL REFERENCES companies(id),
                source_url TEXT NOT NULL,
                source_type TEXT NOT NULL,
                confidence INTEGER NOT NULL DEFAULT 0,
                active_job_count INTEGER NOT NULL DEFAULT 0,
                evidence_json TEXT,
                extraction_provider TEXT,
                extraction_mode TEXT,
                provider_config TEXT,
                last_checked_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(company_id, source_url)
            )
            """
        )
        job_source_columns = {row[1] for row in conn.execute("PRAGMA table_info(job_sources)")}
        for column, definition in {
            "extraction_provider": "TEXT",
            "extraction_mode": "TEXT",
            "provider_config": "TEXT",
        }.items():
            if column not in job_source_columns:
                conn.execute(f"ALTER TABLE job_sources ADD COLUMN {column} {definition}")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS job_postings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER NOT NULL REFERENCES companies(id),
                job_key TEXT NOT NULL UNIQUE,
                job_title TEXT NOT NULL,
                department TEXT,
                salary_info TEXT,
                location TEXT,
                description TEXT,
                application_url TEXT,
                source_url TEXT NOT NULL,
                source_type TEXT NOT NULL DEFAULT 'unknown',
                raw_json TEXT,
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                is_active INTEGER NOT NULL DEFAULT 1,
                is_new INTEGER NOT NULL DEFAULT 1
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS crawl_errors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_id INTEGER REFERENCES companies(id),
                company_name TEXT,
                listing_url TEXT,
                job_title TEXT,
                error_type TEXT NOT NULL,
                message TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS discovered_businesses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                business_name TEXT NOT NULL,
                industry TEXT NOT NULL,
                city TEXT,
                query_city TEXT,
                website_url TEXT NOT NULL,
                phone TEXT,
                full_address TEXT,
                google_place_id TEXT,
                google_id TEXT,
                rating TEXT,
                reviews INTEGER,
                raw_json TEXT,
                status TEXT NOT NULL DEFAULT 'discovered',
                job_source_url TEXT,
                source_type TEXT,
                platform TEXT,
                confidence INTEGER NOT NULL DEFAULT 0,
                evidence_json TEXT NOT NULL DEFAULT '[]',
                association_evidence TEXT,
                verification_status TEXT,
                verification_message TEXT,
                extraction_provider TEXT,
                extraction_mode TEXT,
                provider_config TEXT,
                verified_job_count INTEGER NOT NULL DEFAULT 0,
                imported_company_id INTEGER REFERENCES companies(id),
                discovered_at TEXT NOT NULL,
                continued_at TEXT,
                imported_at TEXT,
                updated_at TEXT NOT NULL,
                UNIQUE(website_url)
            )
            """
        )
        discovery_columns = {row[1] for row in conn.execute("PRAGMA table_info(discovered_businesses)")}
        for column, definition in {
            "query_city": "TEXT",
            "phone": "TEXT",
            "full_address": "TEXT",
            "google_place_id": "TEXT",
            "google_id": "TEXT",
            "rating": "TEXT",
            "reviews": "INTEGER",
            "raw_json": "TEXT",
            "source_type": "TEXT",
            "platform": "TEXT",
            "confidence": "INTEGER NOT NULL DEFAULT 0",
            "evidence_json": "TEXT NOT NULL DEFAULT '[]'",
            "association_evidence": "TEXT",
            "verification_status": "TEXT",
            "verification_message": "TEXT",
            "extraction_provider": "TEXT",
            "extraction_mode": "TEXT",
            "provider_config": "TEXT",
            "verified_job_count": "INTEGER NOT NULL DEFAULT 0",
            "imported_company_id": "INTEGER REFERENCES companies(id)",
            "continued_at": "TEXT",
            "imported_at": "TEXT",
            "updated_at": "TEXT",
        }.items():
            if column not in discovery_columns:
                conn.execute(f"ALTER TABLE discovered_businesses ADD COLUMN {column} {definition}")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        timestamp = now_iso()
        conn.execute(
            "INSERT OR IGNORE INTO app_settings (key, value, updated_at) VALUES ('job_refresh_days', '7', ?)",
            (timestamp,),
        )
        conn.execute(
            "INSERT OR IGNORE INTO app_settings (key, value, updated_at) VALUES ('job_refresh_day', 'sunday', ?)",
            (timestamp,),
        )
        columns = {row[1] for row in conn.execute("PRAGMA table_info(job_postings)")}
        if "source_type" not in columns:
            conn.execute("ALTER TABLE job_postings ADD COLUMN source_type TEXT NOT NULL DEFAULT 'unknown'")
        if "is_featured" not in columns:
            conn.execute("ALTER TABLE job_postings ADD COLUMN is_featured INTEGER NOT NULL DEFAULT 0")
        if "is_new" not in columns:
            conn.execute("ALTER TABLE job_postings ADD COLUMN is_new INTEGER NOT NULL DEFAULT 0")
        columns = {row[1] for row in conn.execute("PRAGMA table_info(job_postings)")}
        for column, definition in {
            "ai_summary": "TEXT",
            "ai_best_for": "TEXT",
            "ai_job_category": "TEXT",
            "ai_experience_level": "TEXT",
            "ai_worker_tags": "TEXT NOT NULL DEFAULT '[]'",
            "ai_physical_demands": "TEXT NOT NULL DEFAULT '[]'",
            "ai_estimated_pay_range": "TEXT",
            "ai_pay_range_type": "TEXT",
            "ai_confidence_score": "INTEGER NOT NULL DEFAULT 0",
            "ai_needs_manual_review": "INTEGER NOT NULL DEFAULT 0",
            "ai_enriched_at": "TEXT",
        }.items():
            if column not in columns:
                conn.execute(f"ALTER TABLE job_postings ADD COLUMN {column} {definition}")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_job_enrichment_batches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                openai_batch_id TEXT,
                input_file_id TEXT,
                output_file_id TEXT,
                error_file_id TEXT,
                status TEXT NOT NULL DEFAULT 'local_created',
                model TEXT,
                total_requests INTEGER NOT NULL DEFAULT 0,
                completed_requests INTEGER NOT NULL DEFAULT 0,
                failed_requests INTEGER NOT NULL DEFAULT 0,
                message TEXT,
                imported INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL,
                submitted_at TEXT,
                checked_at TEXT,
                completed_at TEXT,
                imported_at TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_job_enrichment_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                batch_id INTEGER NOT NULL REFERENCES ai_job_enrichment_batches(id),
                job_id INTEGER NOT NULL REFERENCES job_postings(id),
                custom_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                error TEXT,
                raw_response_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(batch_id, custom_id)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON job_postings(company_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_location ON job_postings(location)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_active ON job_postings(is_active)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_source_type ON job_postings(source_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_featured ON job_postings(is_featured)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_new ON job_postings(is_new)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_ai_enriched ON job_postings(ai_enriched_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_featured ON companies(is_featured)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_ai_enriched ON companies(ai_enriched_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sources_company ON job_sources(company_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sources_type ON job_sources(source_type)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_errors_company ON crawl_errors(company_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_crawl_errors_created ON crawl_errors(created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_discovered_status ON discovered_businesses(status, updated_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_discovered_industry ON discovered_businesses(industry)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_discovered_place ON discovered_businesses(google_place_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_discovered_google_id ON discovered_businesses(google_id)")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_discovered_place_unique ON discovered_businesses(google_place_id) WHERE google_place_id IS NOT NULL AND google_place_id != ''")
        conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_discovered_google_id_unique ON discovered_businesses(google_id) WHERE google_id IS NOT NULL AND google_id != ''")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_enrichment_batches_status ON ai_company_enrichment_batches(status, imported, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_enrichment_requests_batch ON ai_company_enrichment_requests(batch_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_job_enrichment_batches_status ON ai_job_enrichment_batches(status, imported, created_at)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_job_enrichment_requests_batch ON ai_job_enrichment_requests(batch_id)")


def refresh_days_from_db(db_path: Path, fallback: int = 7) -> int:
    init_db(db_path)
    with db.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        row = conn.execute("SELECT value FROM app_settings WHERE key = 'job_refresh_days'").fetchone()
    if not row:
        return fallback
    try:
        return max(1, int(row[0]))
    except (TypeError, ValueError):
        return fallback


def job_key(company_id: int, job: dict[str, Any]) -> str:
    stable = normalize_title(str(job.get("job_title") or ""))
    digest = hashlib.sha256(f"{company_id}|{stable}".encode("utf-8")).hexdigest()
    return digest


def source_score(source: dict[str, Any]) -> tuple[int, int, int]:
    url = str(source.get("url") or "").lower()
    ats_bonus = 20 if any(host in url for host in JOB_BOARD_HOSTS) else 0
    listing_bonus = 15 if int(source.get("job_count") or 0) >= 3 else 0
    return (
        SOURCE_PRIORITIES.get(str(source.get("source_type") or "unknown"), 0) + ats_bonus + listing_bonus,
        int(source.get("confidence") or 0),
        int(source.get("job_count") or 0),
    )


def manual_source_candidates(seed: dict[str, Any], seed_url: str) -> list[DiscoveryCandidate]:
    raw_sources = seed.get("job_sources") or seed.get("sources") or []
    candidates: list[DiscoveryCandidate] = []
    if isinstance(raw_sources, str):
        raw_sources = [{"url": raw_sources}]
    for item in raw_sources:
        if isinstance(item, str):
            source = {"url": item}
        elif isinstance(item, dict):
            source = item
        else:
            continue
        url = normalize_url(source.get("url") or source.get("source_url"), seed_url)
        if not url:
            continue
        source_type = str(source.get("source_type") or "general_jobs")
        if source_type not in SOURCE_PRIORITIES:
            source_type = "general_jobs"
        candidates.append(
            DiscoveryCandidate(
                url=url,
                text=clean_text(str(source.get("label") or source.get("text") or "manual source")),
                score=1000,
                source_type=source_type,
                confidence=int(source.get("confidence") or 100),
                reason=clean_text(str(source.get("reason") or "manual seed source")),
            )
        )
    return candidates


def seed_from_url(url: str) -> dict[str, Any]:
    normalized = normalize_url(url)
    if not normalized:
        raise ValueError(f"invalid seed URL: {url}")
    return {
        "seed_url": normalized,
        "job_sources": [
            {
                "url": normalized,
                "source_type": "general_jobs",
                "confidence": 100,
                "reason": "manual seed URL",
            }
        ],
    }


def upsert_seed_to_db(db_path: Path, seed: dict[str, Any]) -> None:
    init_db(db_path)
    timestamp = now_iso()
    seed_url = normalize_url(seed.get("seed_url") or seed.get("homepage_url") or seed.get("url"))
    if not seed_url:
        raise ValueError(f"missing seed_url in seed: {seed}")
    seed["seed_url"] = seed_url
    has_jobs_key = "jobs" in seed
    job_count = len(seed.get("jobs") or [])
    no_jobs_verified = 1 if seed.get("no_jobs_verified") or (has_jobs_key and seed.get("last_status") == "ok" and job_count == 0) else 0
    no_jobs_verified_at = seed.get("no_jobs_verified_at") or (timestamp if no_jobs_verified else None)
    no_jobs_note = seed.get("no_jobs_note") or ("No active jobs were verified on the configured job source." if no_jobs_verified else None)
    alternate_seed_url = seed_url.rstrip("/") + "/"
    with db.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute(
            """
            UPDATE companies
            SET seed_url = ?
            WHERE seed_url = ?
              AND NOT EXISTS (SELECT 1 FROM companies WHERE seed_url = ?)
            """,
            (seed_url, alternate_seed_url, seed_url),
        )
        cursor = conn.execute(
            """
            INSERT INTO companies (
                seed_url, business_name, city, state, location, industry, homepage_url, jobs_url, source_type,
                extraction_provider, extraction_mode, provider_config, last_job_count,
                last_checked_at, last_status, error, no_jobs_verified, no_jobs_verified_at, no_jobs_note, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(seed_url) DO UPDATE SET
                business_name=excluded.business_name,
                city=excluded.city,
                state=excluded.state,
                location=excluded.location,
                industry=excluded.industry,
                homepage_url=excluded.homepage_url,
                jobs_url=excluded.jobs_url,
                source_type=excluded.source_type,
                extraction_provider=excluded.extraction_provider,
                extraction_mode=excluded.extraction_mode,
                provider_config=excluded.provider_config,
                last_job_count=excluded.last_job_count,
                last_checked_at=excluded.last_checked_at,
                last_status=excluded.last_status,
                error=excluded.error,
                no_jobs_verified=excluded.no_jobs_verified,
                no_jobs_verified_at=excluded.no_jobs_verified_at,
                no_jobs_note=excluded.no_jobs_note,
                updated_at=excluded.updated_at
            RETURNING id
            """,
            (
                seed_url,
                company_name(seed, seed_url),
                seed.get("city"),
                seed.get("state") or seed.get("region"),
                fallback_company_location(seed),
                seed.get("industry"),
                seed.get("homepage_url"),
                seed.get("jobs_url"),
                seed.get("primary_source_type") or source_type_from_seed(seed),
                seed.get("extraction_provider"),
                seed.get("extraction_mode"),
                json.dumps(seed.get("provider_config"), sort_keys=True) if isinstance(seed.get("provider_config"), dict) else seed.get("provider_config"),
                int(seed.get("last_job_count") if seed.get("last_job_count") is not None else job_count),
                seed.get("last_checked_at"),
                seed.get("last_status"),
                seed.get("error"),
                no_jobs_verified,
                no_jobs_verified_at,
                no_jobs_note,
                timestamp,
                timestamp,
            ),
        )
        company_id = int(cursor.fetchone()[0])
        conn.execute("DELETE FROM crawl_errors WHERE company_id = ?", (company_id,))
        if seed.get("error"):
            conn.execute(
                """
                INSERT INTO crawl_errors (company_id, company_name, listing_url, job_title, error_type, message, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    company_id,
                    company_name(seed, seed_url),
                    seed.get("jobs_url") or seed_url,
                    None,
                    "company",
                    str(seed.get("error")),
                    timestamp,
                ),
            )
        for source in seed.get("job_sources", []):
            conn.execute(
                """
                INSERT INTO job_sources (
                    company_id, source_url, source_type, confidence,
                    active_job_count, evidence_json, extraction_provider, extraction_mode, provider_config,
                    last_checked_at, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(company_id, source_url) DO UPDATE SET
                    source_type=excluded.source_type,
                    confidence=excluded.confidence,
                    active_job_count=excluded.active_job_count,
                    evidence_json=excluded.evidence_json,
                    extraction_provider=excluded.extraction_provider,
                    extraction_mode=excluded.extraction_mode,
                    provider_config=excluded.provider_config,
                    last_checked_at=excluded.last_checked_at,
                    updated_at=excluded.updated_at
                """,
                (
                    company_id,
                    source.get("url"),
                    source.get("source_type") or "unknown",
                    int(source.get("confidence") or 0),
                    int(source.get("job_count") or 0),
                    json.dumps(source.get("evidence") or [], sort_keys=True),
                    source.get("extraction_provider") or seed.get("extraction_provider"),
                    source.get("extraction_mode") or seed.get("extraction_mode"),
                    json.dumps(source.get("provider_config"), sort_keys=True) if isinstance(source.get("provider_config"), dict) else source.get("provider_config"),
                    seed.get("last_checked_at") or timestamp,
                    timestamp,
                    timestamp,
                ),
            )
            if source.get("error"):
                conn.execute(
                    """
                    INSERT INTO crawl_errors (company_id, company_name, listing_url, job_title, error_type, message, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        company_id,
                        company_name(seed, seed_url),
                        source.get("url"),
                        None,
                        "source",
                        str(source.get("error")),
                        timestamp,
                    ),
                )
            for evidence in source.get("evidence") or []:
                evidence_text = str(evidence)
                if any(marker in evidence_text.lower() for marker in ("failed", "error", "blocked")):
                    conn.execute(
                        """
                        INSERT INTO crawl_errors (company_id, company_name, listing_url, job_title, error_type, message, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            company_id,
                            company_name(seed, seed_url),
                            source.get("url"),
                            None,
                            "source",
                            evidence_text,
                            timestamp,
                        ),
                    )
        debug = seed.get("debug") if isinstance(seed.get("debug"), dict) else {}
        conn.execute("UPDATE job_postings SET is_new = 0 WHERE company_id = ?", (company_id,))
        seen_keys: list[str] = []
        for job in seed.get("jobs", []):
            key = job_key(company_id, job)
            seen_keys.append(key)
            conn.execute(
                """
                INSERT INTO job_postings (
                    company_id, job_key, job_title, department, salary_info,
                    location, description, application_url, source_url, source_type, raw_json,
                    first_seen_at, last_seen_at, is_active, is_new
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
                ON CONFLICT(job_key) DO UPDATE SET
                    job_title=excluded.job_title,
                    department=excluded.department,
                    salary_info=excluded.salary_info,
                    location=excluded.location,
                    description=excluded.description,
                    application_url=excluded.application_url,
                    source_url=excluded.source_url,
                    source_type=excluded.source_type,
                    raw_json=excluded.raw_json,
                    last_seen_at=excluded.last_seen_at,
                    is_active=1,
                    is_new=0
                """,
                (
                    company_id,
                    key,
                    job.get("job_title"),
                    job.get("department"),
                    job.get("salary_info"),
                    job.get("location"),
                    job.get("description"),
                    job.get("application_url"),
                    job.get("source_url"),
                    job.get("source_type") or "unknown",
                    json.dumps(job.get("raw"), sort_keys=True) if job.get("raw") else None,
                    timestamp,
                    timestamp,
                ),
            )
        if seen_keys:
            placeholders = ",".join("?" for _ in seen_keys)
            conn.execute(
                f"DELETE FROM job_postings WHERE company_id=? AND job_key NOT IN ({placeholders})",
                [company_id, *seen_keys],
            )
        else:
            conn.execute("DELETE FROM job_postings WHERE company_id=?", (company_id,))


def db_crawl_record(db_path: Path, seed_url: str) -> dict[str, Any] | None:
    init_db(db_path)
    alternate_seed_url = seed_url.rstrip("/") + "/"
    with db.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        row = conn.execute(
            """
            SELECT
                companies.id,
                companies.last_checked_at,
                companies.last_status,
                companies.jobs_url,
                companies.no_jobs_verified,
                COUNT(job_postings.id) AS active_job_count,
                SUM(CASE WHEN job_postings.source_type = 'general_jobs' THEN 1 ELSE 0 END) AS active_general_job_count,
                (
                    SELECT COUNT(*)
                    FROM job_sources
                    WHERE job_sources.company_id = companies.id
                      AND job_sources.source_type = 'general_jobs'
                ) AS general_source_count
            FROM companies
            LEFT JOIN job_postings
                ON job_postings.company_id = companies.id
               AND job_postings.is_active = 1
            WHERE seed_url IN (?, ?)
            GROUP BY companies.id
            ORDER BY companies.updated_at DESC
            LIMIT 1
            """,
            (seed_url, alternate_seed_url),
        ).fetchone()
    if not row:
        return None
    return {
        "company_id": row[0],
        "last_checked_at": parse_iso_datetime(row[1]),
        "last_status": row[2],
        "jobs_url": row[3],
        "no_jobs_verified": bool(row[4]),
        "active_job_count": row[5],
        "active_general_job_count": row[6] or 0,
        "general_source_count": row[7] or 0,
    }


def should_crawl_seed(seed: dict[str, Any], db_path: Path, recrawl_days: int, force: bool) -> tuple[bool, str | None]:
    if force:
        return True, None
    seed_url = normalize_url(seed.get("seed_url") or seed.get("homepage_url") or seed.get("url"))
    if not seed_url:
        return True, None
    record = db_crawl_record(db_path, seed_url)
    if not record:
        return True, None
    if record["no_jobs_verified"]:
        return False, "verified no job listings on configured job source"
    if record["last_status"] != "ok" or not record["jobs_url"] or record["active_job_count"] == 0:
        return True, None
    if record["general_source_count"] > 0 and record["active_general_job_count"] == 0:
        return True, None
    last_checked = record["last_checked_at"]
    if not last_checked:
        return True, None
    next_allowed = last_checked + timedelta(days=recrawl_days)
    if datetime.now(timezone.utc) >= next_allowed:
        return True, None
    return False, f"last checked {last_checked.isoformat()} (next after {next_allowed.isoformat()})"


async def process_seed(
    seed: dict[str, Any],
    max_candidate_pages: int,
    max_ai_links: int,
    max_pages: int,
    max_detail_pages: int,
    ai_client: AsyncOpenAI,
    ai_model: str,
) -> dict[str, Any]:
    if str(seed.get("extraction_provider") or "").lower() == "cloudflare":
        return await process_seed_with_cloudflare(seed)

    seed_url = normalize_url(seed.get("seed_url") or seed.get("homepage_url") or seed.get("url"))
    if not seed_url:
        raise ValueError(f"missing seed_url in seed: {seed}")
    start_url = normalize_url(seed.get("jobs_url") or seed.get("job_source_url") or seed_url, seed_url)
    if not start_url:
        raise ValueError(f"missing jobs_url in seed: {seed}")

    rendered_pages = await collect_rendered_listing_pages(start_url, max_pages)
    collected_jobs: list[JobPosting] = []
    collected_keys: set[tuple[str, str | None]] = set()
    sources: list[dict[str, Any]] = []

    for rendered in rendered_pages:
        html = rendered.html
        jobs = extract_jobs(html, rendered.url)
        validation = await ai_validate_job_source(ai_client, ai_model, seed_url, rendered.url, html, jobs)
        validation["job_count"] = len(jobs)
        if validation["source_type"] not in {"unrelated", "culture", "volunteer"}:
            ai_jobs = await ai_extract_jobs_from_page(
                ai_client,
                ai_model,
                seed_url,
                rendered.url,
                html,
                validation["source_type"],
                None,
            )
            existing = {(job.job_title.lower(), job.application_url) for job in jobs}
            for ai_job in ai_jobs:
                key = (ai_job.job_title.lower(), ai_job.application_url)
                if key not in existing:
                    jobs.append(ai_job)
                    existing.add(key)
        validation["has_jobs"] = bool(jobs)
        validation["job_count"] = len(jobs)
        sources.append(validation)
        for job in jobs:
            job.source_type = validation["source_type"]
            key = (normalize_title(job.job_title), job.application_url)
            if key not in collected_keys and validation["source_type"] not in {"unrelated", "culture", "volunteer"}:
                collected_keys.add(key)
                collected_jobs.append(job)

    detailed_jobs = await enrich_job_details_with_playwright(collected_jobs, max_detail_pages)
    verified_jobs, rejected_jobs = await verify_and_dedupe_jobs(
        ai_client,
        os.environ.get("AI_VERIFICATION_MODEL") or ai_model or "gpt-5.4-nano",
        seed,
        detailed_jobs,
    )

    best_source = max(sources, key=source_score) if sources else {
        "url": start_url,
        "has_jobs": False,
        "source_type": source_type_from_seed(seed),
        "confidence": 0,
        "evidence": ["no pages rendered"],
        "job_count": 0,
    }

    seed.update(
        {
            "seed_url": seed_url,
            "jobs_url": start_url,
            "jobs": [asdict(job) for job in verified_jobs],
            "job_sources": sources,
            "primary_source_type": best_source["source_type"],
            "last_status": "ok",
            "last_checked_at": now_iso(),
            "error": None,
            "debug": {
                "pages_checked": len(rendered_pages),
                "rendered_page_bytes": sum(len(page.html) for page in rendered_pages),
                "candidate_job_count": len(collected_jobs),
                "verified_job_count": len(verified_jobs),
                "rejected_job_count": len(rejected_jobs),
                "rejected_jobs": rejected_jobs[:50],
            },
        }
    )
    return seed


def load_seeds(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        raise FileNotFoundError(
            f"Seed file not found: {path}. Create it from seeds.example.json or pass --url https://example.com."
        )
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("seed file must be a JSON list")
    seeds = []
    for item in raw:
        if isinstance(item, str):
            seeds.append(seed_from_url(item))
        elif isinstance(item, dict):
            seed = dict(item)
            seed_url = normalize_url(seed.get("seed_url") or seed.get("homepage_url") or seed.get("url"))
            if seed_url and not seed.get("job_sources") and not seed.get("sources"):
                seed["job_sources"] = [
                    {
                        "url": seed_url,
                        "source_type": "general_jobs",
                        "confidence": 100,
                        "reason": "manual seed URL",
                    }
                ]
            seeds.append(seed)
        else:
            raise ValueError(f"unsupported seed entry: {item}")
    return seeds


def normalize_company_seed(item: Any) -> dict[str, Any]:
    if isinstance(item, str):
        jobs_url = normalize_url(item)
        if not jobs_url:
            raise ValueError(f"invalid jobs URL: {item}")
        return {
            "seed_url": jobs_url,
            "homepage_url": jobs_url,
            "jobs_url": jobs_url,
            "source_type": "general_jobs",
            "job_sources": [{"url": jobs_url, "source_type": "general_jobs", "confidence": 100}],
        }
    if not isinstance(item, dict):
        raise ValueError(f"unsupported seed entry: {item}")
    seed = dict(item)
    jobs_url = normalize_url(seed.get("jobs_url") or seed.get("job_source_url") or seed.get("careers_url") or seed.get("seed_url") or seed.get("url"))
    homepage_url = normalize_url(seed.get("homepage_url") or seed.get("company_url") or seed.get("website") or seed.get("seed_url") or seed.get("url") or jobs_url)
    if not jobs_url:
        raise ValueError(f"missing jobs_url in seed: {item}")
    source_type = str(seed.get("source_type") or "general_jobs")
    if source_type not in SOURCE_PRIORITIES:
        source_type = "general_jobs"
    seed["jobs_url"] = jobs_url
    seed["homepage_url"] = homepage_url
    seed["seed_url"] = homepage_url or jobs_url
    seed["source_type"] = source_type
    seed["job_sources"] = [{"url": jobs_url, "source_type": source_type, "confidence": 100, "reason": "company table source"}]
    return seed


def existing_company_key_for_import(conn: db.connection, seed: dict[str, Any]) -> str | None:
    seed_url = seed["seed_url"]
    jobs_url = seed["jobs_url"]
    for row in conn.execute("SELECT seed_url FROM companies"):
        existing_seed_url = str(row[0])
        if existing_seed_url != seed_url and is_site_root(existing_seed_url) and (same_site(seed_url, existing_seed_url) or same_site(jobs_url, existing_seed_url)):
            return existing_seed_url

    exact = conn.execute("SELECT seed_url FROM companies WHERE seed_url = ?", (seed_url,)).fetchone()
    if exact:
        return str(exact[0])
    return None


def merge_company_rows(conn: db.connection, duplicate_seed_url: str, target_seed_url: str) -> None:
    if duplicate_seed_url == target_seed_url:
        return
    duplicate = conn.execute("SELECT id FROM companies WHERE seed_url = ?", (duplicate_seed_url,)).fetchone()
    target = conn.execute("SELECT id FROM companies WHERE seed_url = ?", (target_seed_url,)).fetchone()
    if not duplicate or not target:
        return
    duplicate_id = int(duplicate[0])
    target_id = int(target[0])

    source_columns = [
        str(row[1])
        for row in conn.execute("PRAGMA table_info(job_sources)")
        if str(row[1]) not in {"id", "company_id"}
    ]
    column_sql = ", ".join(source_columns)
    placeholders = ", ".join(["?"] * (len(source_columns) + 1))
    for row in conn.execute(f"SELECT {column_sql} FROM job_sources WHERE company_id = ?", (duplicate_id,)):
        conn.execute(
            f"INSERT OR IGNORE INTO job_sources (company_id, {column_sql}) VALUES ({placeholders})",
            (target_id, *row),
        )
    conn.execute("UPDATE job_postings SET company_id = ? WHERE company_id = ?", (target_id, duplicate_id))
    conn.execute("DELETE FROM job_sources WHERE company_id = ?", (duplicate_id,))
    conn.execute("DELETE FROM companies WHERE id = ?", (duplicate_id,))


def import_seeds_to_db(db_path: Path, seeds_path: Path) -> int:
    raw = json.loads(seeds_path.read_text(encoding="utf-8"))
    return import_seed_items_to_db(db_path, raw)


def import_seed_items_to_db(db_path: Path, raw: Any) -> int:
    init_db(db_path)
    if not isinstance(raw, list):
        raw = [raw]
    imported = 0
    timestamp = now_iso()
    with db.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        for item in raw:
            seed = normalize_company_seed(item)
            import_seed_url = seed["seed_url"]
            seed_url = existing_company_key_for_import(conn, seed) or import_seed_url
            conn.execute(
                """
                INSERT INTO companies (
                    seed_url, business_name, city, state, location, industry,
                    homepage_url, jobs_url, source_type, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(seed_url) DO UPDATE SET
                    business_name=COALESCE(companies.business_name, excluded.business_name),
                    city=COALESCE(companies.city, excluded.city),
                    state=COALESCE(companies.state, excluded.state),
                    location=COALESCE(companies.location, excluded.location),
                    industry=COALESCE(companies.industry, excluded.industry),
                    homepage_url=COALESCE(companies.homepage_url, excluded.homepage_url),
                    jobs_url=excluded.jobs_url,
                    source_type=excluded.source_type,
                    updated_at=excluded.updated_at
                """,
                (
                    seed_url,
                    company_name(seed, seed_url),
                    seed.get("city"),
                    seed.get("state") or seed.get("region"),
                    fallback_company_location(seed),
                    seed.get("industry"),
                    seed.get("homepage_url"),
                    seed.get("jobs_url"),
                    source_type_from_seed(seed),
                    timestamp,
                    timestamp,
                ),
            )
            merge_company_rows(conn, import_seed_url, seed_url)
            imported += 1
    return imported


def company_row_to_seed(row: db.Row) -> dict[str, Any]:
    jobs_url = row["jobs_url"]
    source_type = row["source_type"] or "general_jobs"
    extraction_provider = row["extraction_provider"] if "extraction_provider" in row.keys() else None
    extraction_mode = row["extraction_mode"] if "extraction_mode" in row.keys() else None
    provider_config = row["provider_config"] if "provider_config" in row.keys() else None
    return {
        "company_id": row["id"],
        "business_name": row["business_name"],
        "city": row["city"],
        "state": row["state"],
        "location": row["location"],
        "industry": row["industry"],
        "homepage_url": row["homepage_url"] or row["seed_url"],
        "seed_url": row["seed_url"],
        "jobs_url": jobs_url,
        "source_type": source_type,
        "extraction_provider": extraction_provider,
        "extraction_mode": extraction_mode,
        "provider_config": provider_config,
        "job_sources": [{"url": jobs_url, "source_type": source_type, "confidence": 100, "reason": "company table source"}],
    }


def due_company_seeds(db_path: Path, recrawl_days: int, limit: int | None, force: bool) -> list[dict[str, Any]]:
    init_db(db_path)
    cutoff = datetime.now(timezone.utc) - timedelta(days=recrawl_days)
    with db.connect(db_path, timeout=30) as conn:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.row_factory = db.Row
        query = """
            SELECT *
            FROM companies
            WHERE jobs_url IS NOT NULL
              AND jobs_url != ''
              AND no_jobs_verified = 0
              AND (
                ? = 1
                OR last_checked_at IS NULL
                OR last_status != 'ok'
                OR last_checked_at <= ?
              )
            ORDER BY COALESCE(last_checked_at, '') ASC, id ASC
        """
        params: list[Any] = [1 if force else 0, cutoff.isoformat()]
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        rows = conn.execute(query, params).fetchall()
    return [company_row_to_seed(row) for row in rows]


def load_state(path: Path, seeds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not path.exists():
        return seeds
    state = json.loads(path.read_text(encoding="utf-8"))
    existing = {
        normalize_url(item.get("seed_url") or item.get("homepage_url") or item.get("url")): item
        for item in state.get("seeds", [])
        if isinstance(item, dict)
    }
    merged = []
    for seed in seeds:
        seed_url = normalize_url(seed.get("seed_url") or seed.get("homepage_url") or seed.get("url"))
        merged.append({**existing.get(seed_url, {}), **seed})
    return merged


def save_state(path: Path, seeds: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"updated_at": now_iso(), "seeds": seeds}
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def select_seeds(
    seeds: list[dict[str, Any]],
    process_all: bool,
    db_path: Path,
    recrawl_days: int,
    force: bool,
) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], str]]]:
    selected: list[dict[str, Any]] = []
    skipped: list[tuple[dict[str, Any], str]] = []
    candidates = seeds if process_all else seeds[:1]
    if not process_all:
        for seed in seeds:
            should_crawl, reason = should_crawl_seed(seed, db_path, recrawl_days, force)
            if should_crawl:
                return [seed], skipped
            skipped.append((seed, reason or "recently checked"))
        return [], skipped

    for seed in candidates:
        should_crawl, reason = should_crawl_seed(seed, db_path, recrawl_days, force)
        if should_crawl:
            selected.append(seed)
        else:
            skipped.append((seed, reason or "recently checked"))
    return selected, skipped


def select_uncrawled_state_seed(seeds: list[dict[str, Any]], process_all: bool) -> list[dict[str, Any]]:
    if process_all:
        return seeds
    for seed in seeds:
        if not seed.get("jobs_url"):
            return [seed]
    return seeds[:1]


def build_ai_client_and_model(ai_model: str | None) -> tuple[AsyncOpenAI, str]:
    api_key = os.environ.get("OPENAI_API_KEY")
    model = ai_model or os.environ.get("AI_DISCOVERY_MODEL") or os.environ.get("AI_VERIFICATION_MODEL") or "gpt-5.4-nano"
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is required for AI-powered discovery.")
    return AsyncOpenAI(api_key=api_key), model


async def main() -> None:
    load_dotenv(Path(__file__).with_name(".env"))

    parser = argparse.ArgumentParser(description="Postgres-backed jobs crawler.")
    parser.add_argument("--db", type=Path, default=Path("postgres"), help="Deprecated; DATABASE_PUBLIC_URL is used.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    import_parser = subparsers.add_parser("import-seeds", help="Import company/job source records into Postgres.")
    import_parser.add_argument("--seeds", type=Path, default=Path("seeds.json"))

    crawl_parser = subparsers.add_parser("crawl", help="Crawl due companies from Postgres.")
    crawl_parser.add_argument("--recrawl-days", type=int)
    crawl_parser.add_argument("--force", action="store_true")
    crawl_parser.add_argument("--limit", type=int)
    crawl_parser.add_argument("--workers", type=int, default=1)
    crawl_parser.add_argument("--max-candidate-pages", type=int, default=12)
    crawl_parser.add_argument("--max-ai-links", type=int, default=60)
    crawl_parser.add_argument("--max-pages", type=int, default=12)
    crawl_parser.add_argument("--max-detail-pages", type=int, default=20)
    crawl_parser.add_argument("--ai-model")
    crawl_parser.add_argument("--print-json", action="store_true")

    args = parser.parse_args()
    init_db(args.db)

    if args.command == "import-seeds":
        try:
            count = import_seeds_to_db(args.db, args.seeds)
        except (FileNotFoundError, json.JSONDecodeError, ValueError) as exc:
            parser.error(str(exc))
        print(f"Imported {count} companies into {args.db}")
        return

    try:
        ai_client, ai_model = build_ai_client_and_model(args.ai_model)
    except RuntimeError as exc:
        parser.error(str(exc))

    recrawl_days = args.recrawl_days if args.recrawl_days is not None else refresh_days_from_db(args.db)
    targets = due_company_seeds(args.db, recrawl_days, args.limit, args.force)
    if not targets:
        print("No companies due for crawl.")
        return

    semaphore = asyncio.Semaphore(max(1, args.workers))
    db_lock = asyncio.Lock()

    async def run_company(seed: dict[str, Any]) -> None:
        async with semaphore:
            try:
                await process_seed(
                    seed,
                    args.max_candidate_pages,
                    args.max_ai_links,
                    args.max_pages,
                    args.max_detail_pages,
                    ai_client,
                    ai_model,
                )
            except Exception as exc:
                seed.update(
                    {
                        "jobs": [],
                        "job_sources": [],
                        "primary_source_type": "unknown",
                        "last_status": "error",
                        "last_checked_at": now_iso(),
                        "error": str(exc),
                    }
                )
            async with db_lock:
                upsert_seed_to_db(args.db, seed)

    await asyncio.gather(*(run_company(seed) for seed in targets))

    if args.print_json:
        print(json.dumps({"companies": targets}, indent=2, sort_keys=True))
    else:
        for seed in targets:
            print(f"{seed.get('business_name') or seed.get('seed_url')} -> {seed.get('jobs_url')} ({len(seed.get('jobs', []))} jobs, {seed.get('last_status')})")


if __name__ == "__main__":
    asyncio.run(main())
