from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from . import Base

class RecordingModel(Base):
    __tablename__ = "recordings"
    id: int = Column(Integer, primary_key=True, index=True)
    name: str = Column(String, nullable=True)
    start_time: DateTime = Column(DateTime, nullable=False)
    end_time: DateTime = Column(DateTime, nullable=False)
    device_name: str = Column(String, nullable=True)

    def as_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "device_name": self.device_name,
        }