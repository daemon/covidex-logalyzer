from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from pathlib import Path
from typing import Optional, List, Iterable
import heapq
import json

from pydantic import BaseModel, Field


__all__ = ['QuerySession', 'Impression', 'Response', 'SessionLogParser']


class Impression(BaseModel):
    position: int
    type: str
    result_id: str
    query_id: UUID
    timestamp: datetime


class Response(BaseModel):
    id: str
    title: str
    doi: str
    source: str
    url: str
    score: float
    authors: List[str]
    paragraphs: List[str]
    abstract: str
    journal: str
    year: Optional[int]
    publish_time: str
    highlights: List[List[List[int]]]
    highlighted_abstract: bool


class QuerySession(BaseModel):
    query_id: UUID
    query: str
    request_ip: Optional[str]
    timestamp: datetime
    vertical: str
    response: List[Response]
    impressions: List[Impression]


@dataclass
class SessionLogParser:
    path: Path
    session_secs_limit: int = 60 * 30

    def read_sessions(self) -> Iterable[QuerySession]:
        def try_emit(curr_ts: datetime) -> Iterable[QuerySession]:
            while open_session_heap:
                earliest_ts, session = heapq.heappop(open_session_heap)
                td = curr_ts - earliest_ts
                if td.seconds > self.session_secs_limit:
                    del open_session_map[session.query_id]
                    yield session
                else:
                    heapq.heappush(open_session_heap, (earliest_ts, session))
                    return

        open_session_heap = []
        open_session_map = {}
        with open(str(self.path)) as f:
            for line in iter(f.readline, None):
                if not line:
                    return
                obj = json.loads(line)
                if obj['type'] == 'query':
                    del obj['type']
                    obj['response'] = [json.loads(x) for x in obj['response']]
                    obj['impressions'] = []
                    session = QuerySession(**obj)
                    heapq.heappush(open_session_heap, (session.timestamp, session))
                    open_session_map[session.query_id] = session
                    curr_ts = session.timestamp
                else:
                    obj = {k.strip(): v for k, v in obj.items()}
                    impression = Impression(**obj)
                    curr_ts = impression.timestamp
                    try:
                        open_session_map[impression.query_id].impressions.append(impression)
                    except KeyError:
                        pass
                for session in try_emit(curr_ts): yield session
            for session in open_session_heap:
                yield session
