import os
import time
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterable, List, Optional, Tuple

import requests
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable


API_VERSION = "5.199"
API_BASE_URL = "https://api.vk.com/method/"


class VkApiError(Exception):
    def __init__(self, error_code: int, error_msg: str, request_params: Optional[List[Dict[str, Any]]] = None):
        super().__init__(f"VK API error {error_code}: {error_msg}")
        self.error_code = error_code
        self.error_msg = error_msg
        self.request_params = request_params or []


def ensure_out_dir(directory: Path) -> None:
    directory.mkdir(parents=True, exist_ok=True)


logger = logging.getLogger("vk_parser")


def setup_logging(level: str = "INFO", log_file: Optional[Path] = None) -> None:
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

    handlers: List[logging.Handler] = []
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    handlers.append(stream_handler)

    if log_file is not None:
        ensure_out_dir(log_file.parent)
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    root = logging.getLogger()
    # Clear existing handlers to allow reconfiguration
    root.handlers = []
    root.setLevel(numeric_level)
    for h in handlers:
        root.addHandler(h)


class RateLimiter:
    def __init__(self, requests_per_second: float = 3.0):
        self.interval_s = 1.0 / max(requests_per_second, 0.1)
        self._lock = threading.Lock()
        self._last_ts = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_s = self.interval_s - (now - self._last_ts)
            if wait_s > 0:
                time.sleep(wait_s)
            self._last_ts = time.monotonic()


def get_http_session(max_pool_size: int = 10) -> requests.Session:
    sess = requests.Session()
    try:
        from requests.adapters import HTTPAdapter
        adapter = HTTPAdapter(pool_connections=max_pool_size, pool_maxsize=max_pool_size)
        sess.mount("http://", adapter)
        sess.mount("https://", adapter)
    except Exception:  # pragma: no cover - best effort
        pass
    return sess


def call_vk_api(method: str, token: str, params: Dict[str, Any], max_retries: int = 5, base_delay_s: float = 0.35,
                session: Optional[requests.Session] = None, rate_limiter: Optional[RateLimiter] = None) -> Dict[str, Any]:
    url = f"{API_BASE_URL}{method}"
    merged_params = {
        **params,
        "access_token": token,
        "v": API_VERSION,
    }

    last_error: Optional[VkApiError] = None
    for attempt in range(max_retries):
        safe_params = dict(merged_params)
        safe_params.pop("access_token", None)
        logger.debug("VK API request: method=%s attempt=%d params=%s", method, attempt + 1, safe_params)
        try:
            if rate_limiter is not None:
                rate_limiter.acquire()
            http = session or requests
            response = http.get(url, params=merged_params, timeout=30)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as req_err:
            last_error = VkApiError(-1, f"HTTP error: {req_err}")
            # Exponential backoff for transient network errors
            sleep_s = base_delay_s * (2 ** attempt)
            logger.warning("HTTP error on %s attempt %d: %s. Backing off for %.2fs", method, attempt + 1, req_err, sleep_s)
            time.sleep(sleep_s)
            continue

        if "error" in payload:
            err = payload["error"]
            error_code = err.get("error_code", -1)
            error_msg = err.get("error_msg", "Unknown error")
            last_error = VkApiError(error_code, error_msg, err.get("request_params"))

            # 6 Too many requests per second; 10 Internal server error; 1 Unknown; 14 Captcha needed (cannot solve here)
            if error_code in (1, 6, 9, 10):
                sleep_s = base_delay_s * (2 ** attempt)
                logger.warning("VK API error on %s attempt %d: code=%s msg=%s. Retry in %.2fs", method, attempt + 1, error_code, error_msg, sleep_s)
                time.sleep(sleep_s)
                continue
            # Private content / access denied or captcha -> do not retry
            if error_code in (14, 15, 18, 200, 201, 203):
                logger.error("Non-retryable VK API error on %s: code=%s msg=%s", method, error_code, error_msg)
                raise last_error

            # Default backoff
            sleep_s = base_delay_s * (2 ** attempt)
            logger.warning("VK API error on %s attempt %d: code=%s msg=%s. Backoff %.2fs", method, attempt + 1, error_code, error_msg, sleep_s)
            time.sleep(sleep_s)
            continue

        # Respect rate limit when no external limiter is provided
        if rate_limiter is None:
            time.sleep(base_delay_s)
        logger.debug("VK API response OK: method=%s", method)
        return payload["response"]

    # If we are here, all retries failed
    if last_error is not None:
        logger.error("Exhausted retries for %s. Last error: %s", method, last_error)
        raise last_error
    raise VkApiError(-1, "Unknown VK API failure")


def resolve_screen_name(token: str, screen_name: str, session: Optional[requests.Session] = None,
                        rate_limiter: Optional[RateLimiter] = None) -> Tuple[str, int]:
    logger.debug("Resolving screen name: %s", screen_name)
    resp = call_vk_api("utils.resolveScreenName", token, {"screen_name": screen_name}, session=session, rate_limiter=rate_limiter)
    if not resp:
        raise VkApiError(404, f"Cannot resolve screen name: {screen_name}")
    logger.debug("Resolved screen name '%s' -> type=%s id=%s", screen_name, resp.get("type"), resp.get("object_id"))
    return resp["type"], int(resp["object_id"])  # type: ignore[index]


def parse_owner_identifier(token: str, raw: str, session: Optional[requests.Session] = None,
                          rate_limiter: Optional[RateLimiter] = None) -> int:
    s = raw.strip()
    logger.debug("Parsing owner identifier: raw=%s", raw)
    # Numeric id or -id
    if s.lstrip("-").isdigit():
        logger.debug("Parsed numeric owner id: %s", s)
        return int(s)

    # Links like https://vk.com/public123, club123, id123, @group
    s = s.replace("https://", "").replace("http://", "")
    s = s.replace("vk.com/", "")
    s = s.lstrip("@")

    # Known prefixes
    for prefix in ("club", "public", "event", "id"):
        if s.startswith(prefix) and s[len(prefix):].isdigit():
            obj_id = int(s[len(prefix):])
            if prefix == "id":
                return obj_id  # user
            # groups use negative owner_id
            logger.debug("Parsed prefixed identifier: prefix=%s id=%d (group owner_id=%d)", prefix, obj_id, -obj_id)
            return -obj_id

    # Resolve arbitrary screen name
    obj_type, obj_id = resolve_screen_name(token, s, session=session, rate_limiter=rate_limiter)
    if obj_type == "user":
        logger.debug("Resolved user owner_id=%d", obj_id)
        return obj_id
    if obj_type in ("group", "page", "event"):
        logger.debug("Resolved group/page/event owner_id=%d", -obj_id)
        return -obj_id
    raise VkApiError(400, f"Unsupported VK object type: {obj_type} for {raw}")


def get_all_my_groups(token: str, session: Optional[requests.Session] = None,
                      rate_limiter: Optional[RateLimiter] = None) -> List[int]:
    group_ids: List[int] = []
    offset = 0
    count = 1000
    while True:
        resp = call_vk_api("groups.get", token, {"count": count, "offset": offset}, session=session, rate_limiter=rate_limiter)
        ids = resp.get("items", [])
        if not ids:
            break
        group_ids.extend(int(gid) for gid in ids)
        logger.debug("Fetched %d groups at offset %d", len(ids), offset)
        if len(ids) < count:
            break
        offset += count
    # Convert to owner_id values (negative for groups)
    logger.info("Total groups fetched: %d", len(group_ids))
    return [-gid for gid in group_ids]


def iterate_wall_posts(token: str, owner_id: int, max_posts: Optional[int] = None,
                       since_date: Optional[datetime] = None,
                       until_date: Optional[datetime] = None,
                       session: Optional[requests.Session] = None,
                       rate_limiter: Optional[RateLimiter] = None) -> Iterable[Dict[str, Any]]:
    fetched = 0
    offset = 0
    count = 100

    logger.info("Start fetching posts: owner_id=%d max_posts=%s since=%s until=%s", owner_id, max_posts, since_date, until_date)
    while True:
        resp = call_vk_api("wall.get", token, {
            "owner_id": owner_id,
            "count": count,
            "offset": offset,
        }, session=session, rate_limiter=rate_limiter)
        items: List[Dict[str, Any]] = resp.get("items", [])
        logger.debug("wall.get returned %d posts at offset %d", len(items), offset)
        if not items:
            break

        for post in items:
            post_date = datetime.fromtimestamp(post.get("date", 0))

            if since_date and post_date < since_date:
                # Since wall.get returns newest first, once we pass since_date we can stop
                logger.info("Reached since_date boundary: %s < %s. Stop.", post_date, since_date)
                return
            if until_date and post_date > until_date:
                # Skip too-new posts, continue to next item
                logger.debug("Skip post %s newer than until=%s", post.get("id"), until_date)
                continue

            yield post
            fetched += 1
            if fetched % 100 == 0:
                logger.info("Fetched posts count: %d (owner_id=%d)", fetched, owner_id)
            if max_posts is not None and fetched >= max_posts:
                logger.info("Reached max_posts=%d for owner_id=%d", max_posts, owner_id)
                return

        if len(items) < count:
            logger.debug("Less than requested posts returned (%d < %d). Stop.", len(items), count)
            break
        offset += count


def iterate_comment_replies(token: str, owner_id: int, post_id: int, parent_comment_id: int,
                            session: Optional[requests.Session] = None,
                            rate_limiter: Optional[RateLimiter] = None) -> Iterable[Dict[str, Any]]:
    offset = 0
    count = 100
    while True:
        resp = call_vk_api("wall.getComments", token, {
            "owner_id": owner_id,
            "post_id": post_id,
            "comment_id": parent_comment_id,
            "count": count,
            "offset": offset,
            "sort": "asc",
            "thread_items_count": 10,
            "extended": 0,
        }, session=session, rate_limiter=rate_limiter)
        items: List[Dict[str, Any]] = resp.get("items", [])
        logger.debug("Fetched %d replies for comment_id=%d offset=%d", len(items), parent_comment_id, offset)
        if not items:
            break
        for c in items:
            yield c
        if len(items) < count:
            break
        offset += count


def iterate_post_comments(token: str, owner_id: int, post_id: int,
                          session: Optional[requests.Session] = None,
                          rate_limiter: Optional[RateLimiter] = None) -> Iterable[Dict[str, Any]]:
    offset = 0
    count = 100
    logger.info("Start fetching comments: owner_id=%d post_id=%d", owner_id, post_id)
    while True:
        resp = call_vk_api("wall.getComments", token, {
            "owner_id": owner_id,
            "post_id": post_id,
            "count": count,
            "offset": offset,
            "sort": "asc",
            "thread_items_count": 10,
            "extended": 0,
        }, session=session, rate_limiter=rate_limiter)
        items: List[Dict[str, Any]] = resp.get("items", [])
        logger.debug("wall.getComments returned %d items at offset %d", len(items), offset)
        if not items:
            break

        for comment in items:
            yield comment
            thread = comment.get("thread") or {}
            # If there are more replies than returned in thread.items, fetch the rest
            if isinstance(thread, dict):
                total = int(thread.get("count", 0))
                returned = len(thread.get("items", []) or [])
                if total > returned:
                    parent_id = int(comment.get("id"))
                    logger.debug("Fetching extra replies for comment_id=%d: returned=%d total=%d", parent_id, returned, total)
                    for reply in iterate_comment_replies(token, owner_id, post_id, parent_id, session=session, rate_limiter=rate_limiter):
                        yield reply

        if len(items) < count:
            break
        offset += count


def fetch_all_comments_for_post(token: str, owner_id: int, post: Dict[str, Any],
                                session: Optional[requests.Session], rate_limiter: Optional[RateLimiter]) -> List[Dict[str, Any]]:
    post_id = int(post.get("id"))
    comments: List[Dict[str, Any]] = []
    total_fetched = 0
    filtered = 0
    
    for c in iterate_post_comments(token, owner_id, post_id, session=session, rate_limiter=rate_limiter):
        total_fetched += 1
        # Фильтруем только валидные комментарии
        if is_valid_comment(c):
            c_copy = dict(c)
            c_copy["_post_id"] = post_id
            comments.append(c_copy)
        else:
            filtered += 1
    
    if filtered > 0:
        logger.debug("Post %d: %d comments fetched, %d filtered, %d valid", 
                    post_id, total_fetched, filtered, len(comments))
    
    return comments


def write_jsonl(path: Path, records: Iterable[Dict[str, Any]]) -> int:
    ensure_out_dir(path.parent)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
            if n % 1000 == 0:
                logger.info("Wrote %d records to %s", n, path)
    logger.info("Finished writing %d records to %s", n, path)
    return n


def write_jsonl_with_callback(path: Path, records: Iterable[Dict[str, Any]], on_record: Optional[Callable[[Dict[str, Any]], None]]) -> int:
    ensure_out_dir(path.parent)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if on_record is not None:
                try:
                    on_record(rec)
                except Exception as e:
                    logger.error("on_record callback failed: %s", e)
            n += 1
            if n % 1000 == 0:
                logger.info("Wrote %d records to %s", n, path)
    logger.info("Finished writing %d records to %s", n, path)
    return n


# Kafka integration (optional)
class KafkaUnavailable(Exception):
    pass


def is_valid_comment(comment: Dict[str, Any]) -> bool:
    """Проверяет, является ли комментарий валидным для отправки в Kafka"""
    # Проверяем наличие текста
    text = comment.get("text") or comment.get("message")
    if not text:
        logger.debug("Comment %s filtered: no text/message field", comment.get("id", "unknown"))
        return False
    
    # Проверяем, что текст не пустой после очистки
    if not str(text).strip():
        logger.debug("Comment %s filtered: empty text after stripping", comment.get("id", "unknown"))
        return False
    
    # Проверяем наличие обязательных полей
    if not comment.get("id"):
        logger.debug("Comment filtered: no ID field")
        return False
    
    return True


def create_kafka_producer(bootstrap_servers: Optional[str]):
    if not bootstrap_servers:
        return None
    try:
        from kafka import KafkaProducer  # type: ignore
    except Exception as e:  # pragma: no cover
        logger.error("Kafka dependency not available: %s. Install 'kafka-python' package.", e)
        raise KafkaUnavailable(str(e))

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8") if v is not None else None,
        linger_ms=50,
        acks=1,
        compression_type="gzip",
        max_in_flight_requests_per_connection=5,
    )
    return producer


def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    if not date_str:
        return None
    return datetime.strptime(date_str, "%Y-%m-%d")


# --- Watch mode helpers ---
def append_jsonl(path: Path, records: Iterable[Dict[str, Any]]) -> int:
    ensure_out_dir(path.parent)
    n = 0
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            n += 1
            if n % 1000 == 0:
                logger.info("Appended %d records to %s", n, path)
    if n > 0:
        logger.info("Appended %d records to %s", n, path)
    return n


def append_jsonl_with_callback(path: Path, records: Iterable[Dict[str, Any]], on_record: Optional[Callable[[Dict[str, Any]], None]]) -> int:
    ensure_out_dir(path.parent)
    n = 0
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
            if on_record is not None:
                try:
                    on_record(rec)
                except Exception as e:
                    logger.error("on_record callback failed: %s", e)
            n += 1
            if n % 1000 == 0:
                logger.info("Appended %d records to %s", n, path)
    if n > 0:
        logger.info("Appended %d records to %s", n, path)
    return n


def load_state(state_path: Path) -> Dict[str, Any]:
    try:
        if state_path.exists():
            with state_path.open("r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.warning("Failed to load state from %s: %s", state_path, e)
    return {}


def save_state(state_path: Path, state: Dict[str, Any]) -> None:
    try:
        ensure_out_dir(state_path.parent)
        tmp_path = state_path.with_suffix(state_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False)
        tmp_path.replace(state_path)
    except Exception as e:
        logger.error("Failed to save state to %s: %s", state_path, e)


def resolve_owner_ids(groups: List[str], all_my_groups: bool, token: str, session: requests.Session, limiter: RateLimiter) -> List[int]:
    owner_ids: List[int] = []
    if all_my_groups:
        logger.info("Fetching all groups for the current user…")
        owner_ids.extend(get_all_my_groups(token, session=session, rate_limiter=limiter))
        logger.info("Found %d groups", len(owner_ids))
    if groups:
        for g in groups:
            try:
                oid = parse_owner_identifier(token, g, session=session, rate_limiter=limiter)
                owner_ids.append(oid)
            except VkApiError as e:
                logger.warning("Skip '%s': %s", g, e)
    seen = set()
    return [x for x in owner_ids if not (x in seen or seen.add(x))]


def watch(groups: List[str], all_my_groups: bool, token: str, out_dir: Path,
          include_comments: bool, poll_every_sec: int, recent_posts_for_comments: int,
          parallel_comments: int = 1, requests_per_second: float = 3.0, http_pool: int = 10,
          state_file: Optional[Path] = None) -> None:
    session = get_http_session(max_pool_size=http_pool)
    limiter = RateLimiter(requests_per_second=requests_per_second)

    owner_ids = resolve_owner_ids(groups, all_my_groups, token, session, limiter)
    if not owner_ids:
        logger.error("No owners to process. Provide --groups or --all-my-groups.")
        return

    base_dir = out_dir / "vk_watch"
    ensure_out_dir(base_dir)
    state_path = state_file or (base_dir / "watch_state.json")
    state: Dict[str, Any] = load_state(state_path)

    logger.info("Starting watch mode for %d owners. Poll every %ds", len(owner_ids), poll_every_sec)

    # Optional Kafka integration for comments
    on_record_cb: Optional[Callable[[Dict[str, Any]], None]] = None
    if include_comments and (args_kafka_topic := os.getenv("KAFKA_TOPIC") or None):
        kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        try:
            producer = create_kafka_producer(kafka_bootstrap)
            logger.info("Kafka producer created for topic '%s' on '%s'", args_kafka_topic, kafka_bootstrap)

            def on_record_send(rec: Dict[str, Any]) -> None:
                key = f"{rec.get('_owner_id')}_{rec.get('_post_id')}_{rec.get('id')}"
                _ = producer.send(args_kafka_topic, key=key, value=rec)
                return None

            on_record_cb = on_record_send
        except KafkaUnavailable:
            logger.error("Kafka unavailable; proceeding without producing")
            on_record_cb = None

    while True:
        loop_started = time.time()
        try:
            for owner_id in owner_ids:
                owner_key = str(owner_id)
                owner_state: Dict[str, Any] = state.get(owner_key) or {}
                last_seen_post_id: int = int(owner_state.get("last_post_id", 0) or 0)

                owner_str = f"group_{-owner_id}" if owner_id < 0 else f"user_{owner_id}"
                posts_path = base_dir / f"{owner_str}_posts.jsonl"
                comments_path = base_dir / f"{owner_str}_comments.jsonl"

                # Fetch latest posts page
                resp = call_vk_api("wall.get", token, {
                    "owner_id": owner_id,
                    "count": 100,
                    "offset": 0,
                }, session=session, rate_limiter=limiter)
                items: List[Dict[str, Any]] = resp.get("items", [])

                # Determine new posts by id
                new_posts = [p for p in items if int(p.get("id", 0)) > last_seen_post_id]
                if new_posts:
                    # Write oldest first by date
                    new_posts_sorted = sorted(new_posts, key=lambda p: int(p.get("date", 0)))
                    appended = append_jsonl(posts_path, new_posts_sorted)
                    logger.info("%s: appended %d new posts", owner_str, appended)
                    last_seen_post_id = max(int(p.get("id", 0)) for p in new_posts)
                    owner_state["last_post_id"] = last_seen_post_id
                else:
                    logger.debug("%s: no new posts", owner_str)

                # Comments watching
                if include_comments:
                    posts_to_check = items[:max(1, recent_posts_for_comments)]
                    posts_state: Dict[str, Any] = owner_state.get("posts") or {}

                    def gather_for_post(post: Dict[str, Any]) -> Tuple[int, List[Dict[str, Any]], int]:
                        pid = int(post.get("id"))
                        last_cid = int(posts_state.get(str(pid), 0) or 0)
                        new_comments: List[Dict[str, Any]] = []
                        max_seen = last_cid
                        total_fetched = 0
                        filtered = 0
                        
                        for c in iterate_post_comments(token, owner_id, pid, session=session, rate_limiter=limiter):
                            cid = int(c.get("id", 0))
                            if cid <= last_cid:
                                continue
                            total_fetched += 1
                            # Фильтруем только валидные комментарии
                            if is_valid_comment(c):
                                c_copy = dict(c)
                                c_copy["_owner_id"] = owner_id
                                c_copy["_post_id"] = pid
                                new_comments.append(c_copy)
                                if cid > max_seen:
                                    max_seen = cid
                            else:
                                filtered += 1
                        
                        if filtered > 0:
                            logger.debug("Post %d: %d new comments fetched, %d filtered, %d valid", 
                                        pid, total_fetched, filtered, len(new_comments))
                        
                        return pid, new_comments, max_seen

                    results: List[Tuple[int, List[Dict[str, Any]], int]] = []
                    if parallel_comments > 1 and len(posts_to_check) > 1:
                        with ThreadPoolExecutor(max_workers=max(1, parallel_comments)) as executor:
                            futures = [executor.submit(gather_for_post, p) for p in posts_to_check]
                            for fut in as_completed(futures):
                                try:
                                    results.append(fut.result())
                                except Exception as e:
                                    logger.error("%s: error checking comments: %s", owner_str, e)
                    else:
                        for p in posts_to_check:
                            try:
                                results.append(gather_for_post(p))
                            except Exception as e:
                                logger.error("%s: error checking comments: %s", owner_str, e)

                    total_new_comments = 0
                    for pid, new_comments, max_seen in results:
                        if new_comments:
                            new_comments_sorted = sorted(new_comments, key=lambda c: int(c.get("date", 0)))
                            total_new_comments += append_jsonl_with_callback(comments_path, new_comments_sorted, on_record_cb)
                            posts_state[str(pid)] = max_seen
                    if total_new_comments > 0:
                        logger.info("%s: appended %d new comments", owner_str, total_new_comments)
                    owner_state["posts"] = posts_state

                state[owner_key] = owner_state

            save_state(state_path, state)
        except Exception as e:
            logger.error("Watch loop error: %s", e)

        # Sleep until next poll
        elapsed = time.time() - loop_started
        sleep_s = max(0.0, float(poll_every_sec) - elapsed)
        if sleep_s > 0:
            time.sleep(sleep_s)


def run(groups: List[str], all_my_groups: bool, token: str, out_dir: Path,
        include_comments: bool, max_posts_per_group: Optional[int],
        since: Optional[str], until: Optional[str],
        parallel_comments: int = 1, requests_per_second: float = 3.0, http_pool: int = 10) -> None:
    since_dt = parse_date(since)
    until_dt = parse_date(until)

    session = get_http_session(max_pool_size=http_pool)
    limiter = RateLimiter(requests_per_second=requests_per_second)

    owner_ids = resolve_owner_ids(groups, all_my_groups, token, session, limiter)

    if not owner_ids:
        logger.error("No owners to process. Provide --groups or --all-my-groups.")
        return

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_dir = out_dir / f"vk_dump_{timestamp}"
    ensure_out_dir(base_dir)

    for owner_id in owner_ids:
        owner_str = f"group_{-owner_id}" if owner_id < 0 else f"user_{owner_id}"
        logger.info("Processing %s (owner_id=%d)…", owner_str, owner_id)
        t0 = time.time()

        posts_path = base_dir / f"{owner_str}_posts.jsonl"
        comments_path = base_dir / f"{owner_str}_comments.jsonl"

        # Collect posts
        posts_iter = list(iterate_wall_posts(token, owner_id, max_posts=max_posts_per_group,
                                             since_date=since_dt, until_date=until_dt,
                                             session=session, rate_limiter=limiter))
        num_posts = write_jsonl(posts_path, posts_iter)
        logger.info("Saved %d posts to %s", num_posts, posts_path)

        if include_comments and num_posts > 0:
            def sequential_comments() -> Iterable[Dict[str, Any]]:
                total_fetched = 0
                total_filtered = 0
                
                for p in posts_iter:
                    comments_count = int(((p.get("comments") or {}).get("count", 0)))
                    if comments_count <= 0:
                        logger.debug("Post %s has no comments. Skip.", p.get("id"))
                        continue
                    
                    post_fetched = 0
                    post_filtered = 0
                    
                    for c in iterate_post_comments(token, owner_id, int(p.get("id")), session=session, rate_limiter=limiter):
                        post_fetched += 1
                        total_fetched += 1
                        # Фильтруем только валидные комментарии
                        if is_valid_comment(c):
                            c_copy = dict(c)
                            c_copy["_owner_id"] = owner_id
                            c_copy["_post_id"] = int(p.get("id"))
                            yield c_copy
                        else:
                            post_filtered += 1
                            total_filtered += 1
                    
                    if post_filtered > 0:
                        logger.debug("Post %s: %d comments fetched, %d filtered", 
                                    p.get("id"), post_fetched, post_filtered)
                
                if total_filtered > 0:
                    logger.info("Sequential mode: %d total comments fetched, %d filtered", 
                               total_fetched, total_filtered)

            def parallel_comments_generator() -> Iterable[Dict[str, Any]]:
                total_fetched = 0
                total_filtered = 0
                
                with ThreadPoolExecutor(max_workers=max(1, parallel_comments)) as executor:
                    futures = []
                    for p in posts_iter:
                        comments_count = int(((p.get("comments") or {}).get("count", 0)))
                        if comments_count <= 0:
                            logger.debug("Post %s has no comments. Skip.", p.get("id"))
                            continue
                        futures.append(executor.submit(fetch_all_comments_for_post, token, owner_id, p, session, limiter))
                    
                    for fut in as_completed(futures):
                        try:
                            results = fut.result()
                            for item in results:
                                total_fetched += 1
                                # Дополнительная проверка на случай, если fetch_all_comments_for_post не отфильтровал
                                if is_valid_comment(item):
                                    yield item
                                else:
                                    total_filtered += 1
                        except Exception as e:
                            logger.error("Error fetching comments in parallel: %s", e)
                
                if total_filtered > 0:
                    logger.info("Parallel mode: %d total comments fetched, %d filtered", 
                               total_fetched, total_filtered)

            generator = sequential_comments if parallel_comments <= 1 else parallel_comments_generator
            # Optional Kafka integration via on_record callback
            on_record_cb: Optional[Callable[[Dict[str, Any]], None]] = None
            if args_kafka_topic := os.getenv("KAFKA_TOPIC") or None:
                kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
                try:
                    producer = create_kafka_producer(kafka_bootstrap)
                    logger.info("Kafka producer created for topic '%s' on '%s'", args_kafka_topic, kafka_bootstrap)

                    def on_record_send(rec: Dict[str, Any]) -> None:
                        key = f"{rec.get('_owner_id')}_{rec.get('_post_id')}_{rec.get('id')}"
                        future = producer.send(args_kafka_topic, key=key, value=rec)
                        # Optionally add callbacks here; we rely on batching
                        return None

                    on_record_cb = on_record_send
                except KafkaUnavailable:
                    logger.error("Kafka unavailable; proceeding without producing")
                    on_record_cb = None

            num_comments = write_jsonl_with_callback(comments_path, generator(), on_record_cb)
            logger.info("Saved %d comments to %s", num_comments, comments_path)

        logger.info("Finished %s in %.2fs", owner_str, time.time() - t0)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="VK parser: download posts and comments from groups and users"
    )
    parser.add_argument("--token", type=str, default=os.getenv("VK_TOKEN"),
                        help="VK access token. If omitted, takes from VK_TOKEN env var.")
    parser.add_argument("--groups", nargs="*", default=[],
                        help="List of group/user identifiers (e.g., public123, club456, id789, somegroup, -123456)")
    parser.add_argument("--all-my-groups", action="store_true",
                        help="Fetch all groups the current user belongs to")
    parser.add_argument("--out-dir", type=Path, default=Path("vk_data"),
                        help="Output directory (default: ./vk_data)")
    parser.add_argument("--no-comments", dest="include_comments", action="store_false",
                        help="Do not download comments")
    parser.add_argument("--max-posts-per-group", type=int, default=None,
                        help="Maximum number of recent posts to fetch per owner")
    parser.add_argument("--since", type=str, default=None,
                        help="Lower bound date for posts in YYYY-MM-DD (inclusive)")
    parser.add_argument("--until", type=str, default=None,
                        help="Upper bound date for posts in YYYY-MM-DD (inclusive)")
    parser.add_argument("--log-level", type=str, default=os.getenv("LOG_LEVEL", "INFO"),
                        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO")
    parser.add_argument("--log-file", type=Path, default=None,
                        help="Optional path to log file")
    parser.add_argument("--parallel-comments", type=int, default=int(os.getenv("PARALLEL_COMMENTS", "1")),
                        help="Number of threads to fetch comments in parallel per owner (default: 1)")
    parser.add_argument("--rps", type=float, default=float(os.getenv("VK_RPS", "3")),
                        help="Global requests-per-second limit (default: 3)")
    parser.add_argument("--http-pool", type=int, default=int(os.getenv("HTTP_POOL", "10")),
                        help="HTTP connection pool size (default: 10)")
    parser.add_argument("--watch", action="store_true",
                        help="Enable watch mode: poll periodically for new posts and comments")
    parser.add_argument("--poll-every", type=int, default=int(os.getenv("POLL_EVERY", "30")),
        				help="Polling interval in seconds for watch mode (default: 30)")
    parser.add_argument("--state-file", type=Path, default=None,
                        help="Optional path to persistent state JSON file for watch mode")
    parser.add_argument("--recent-posts-for-comments", type=int, default=int(os.getenv("RECENT_POSTS_FOR_COMMENTS", "50")),
                        help="How many latest posts per owner to scan for new comments in watch mode (default: 50)")

    args = parser.parse_args()

    if not args.token:
        raise SystemExit("Provide VK access token via --token or VK_TOKEN env var")

    setup_logging(level=args.log_level, log_file=args.log_file)

    if bool(args.watch):
        watch(
            groups=args.groups,
            all_my_groups=bool(args.all_my_groups),
            token=args.token,
            out_dir=Path(args.out_dir),
            include_comments=bool(args.include_comments),
            poll_every_sec=max(1, int(args.poll_every)),
            recent_posts_for_comments=max(1, int(args.recent_posts_for_comments)),
            parallel_comments=max(1, int(args.parallel_comments)),
            requests_per_second=max(0.1, float(args.rps)),
            http_pool=max(1, int(args.http_pool)),
            state_file=args.state_file,
        )
    else:
        run(
            groups=args.groups,
            all_my_groups=bool(args.all_my_groups),
            token=args.token,
            out_dir=Path(args.out_dir),
            include_comments=bool(args.include_comments),
            max_posts_per_group=args.max_posts_per_group,
            since=args.since,
            until=args.until,
            parallel_comments=max(1, int(args.parallel_comments)),
            requests_per_second=max(0.1, float(args.rps)),
            http_pool=max(1, int(args.http_pool)),
        )


if __name__ == "__main__":
    main()


