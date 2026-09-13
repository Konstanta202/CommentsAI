from sqlalchemy import Column, Integer, DateTime, Text, ForeignKey
from sqlalchemy.orm import relationship
from app.database.database import Base


class Comment(Base):
    __tablename__ = "comments"

    id = Column(Integer, primary_key=True, index=True)
    posts_id = Column(Integer, ForeignKey('posts.id'), nullable=False)
    user_id = Column(Integer, nullable=False)
    comment = Column(Text)
    created_at = Column(DateTime, nullable=False)

    post = relationship("Post", back_populates="comments")
