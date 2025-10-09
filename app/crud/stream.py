from typing import List, Optional

from app.database.connection import SessionLocal
from app.models.stream import Stream, StreamState


class StreamDAO:
    """Data Access Object for Stream model: owns its own DB sessions per operation."""

    def add(
        self,
        stream_id: str,
        source_uri: str,
        output_url: str,
        state: StreamState = StreamState.error,
    ) -> Stream:
        db = SessionLocal()
        try:
            db_stream = Stream(
                stream_id=stream_id,
                source_uri=source_uri,
                output_url=output_url,
                state=state.value,
            )
            db.add(db_stream)
            db.commit()
            db.refresh(db_stream)
            return db_stream
        finally:
            db.close()

    def get(self, stream_id: str) -> Optional[Stream]:
        db = SessionLocal()
        try:
            return db.query(Stream).filter(Stream.stream_id == stream_id).first()
        finally:
            db.close()

    def list(self) -> List[Stream]:
        db = SessionLocal()
        try:
            return db.query(Stream).all()
        finally:
            db.close()

    def update_state(self, stream_id: str, state: StreamState) -> Optional[Stream]:
        db = SessionLocal()
        try:
            db_stream = db.query(Stream).filter(Stream.stream_id == stream_id).first()
            if db_stream:
                db_stream.state = state.value
                db.commit()
                db.refresh(db_stream)
            return db_stream
        finally:
            db.close()

    def remove(self, stream_id: str) -> bool:
        db = SessionLocal()
        try:
            db_stream = db.query(Stream).filter(Stream.stream_id == stream_id).first()
            if db_stream:
                db.delete(db_stream)
                db.commit()
                return True
            return False
        finally:
            db.close()

    def exists(self, stream_id: str) -> bool:
        db = SessionLocal()
        try:
            return (
                db.query(Stream).filter(Stream.stream_id == stream_id).first()
                is not None
            )
        finally:
            db.close()
