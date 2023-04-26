import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.sql import func
from . import Base, session

class RecordingModel(Base):
    __tablename__ = "recordings"
    name: str = Column(String, primary_key=True, index=True)
    start_time: DateTime = Column(DateTime, nullable=True)
    end_time: DateTime = Column(DateTime, nullable=True)
    device_name: str = Column(String, nullable=True)
    alias_name: str = Column(String, nullable=True)

    def as_dict(self):
        start = self.start_time
        end = self.end_time
        return {
            "name": self.name,
            "start_time": start.isoformat() if start else None,
            "end_time": end.isoformat() if end else None,
            "device_name": self.device_name,
        }



def create_recording(name=''):
    start_time = datetime.datetime.now()
    name = name or start_time.strftime('%Y-%m-%dT%H-%M-%S')
    session.add(RecordingModel(name=name, start_time=start_time))
    session.commit()
    return name

def end_recording(name):
    session.query(RecordingModel).filter(RecordingModel.name == name).update({'end_time': datetime.datetime.now()})
    session.commit()

def get_recording(name):
    return session.query(RecordingModel).get(name)

def get_last_recording():
    return session.query(RecordingModel).order_by(RecordingModel.start_time.desc()).first()

def free_up_name(name):
    new_name_q = RecordingModel.name + " " + func.to_char(RecordingModel.start_time, "%Y-%m-%dT%H-%M-%S")
    named = session.query([RecordingModel.name, new_name_q]).filter(RecordingModel.name == name).all()
    session.query(RecordingModel).filter(RecordingModel.name == name).update({
        'name': new_name_q,
        'alias_name': RecordingModel.name,
    })
    session.commit()
    return named

def rename_recording(old_name, new_name):
    session.query(RecordingModel).filter(RecordingModel.name == old_name).update({'name': new_name})
    session.commit()

def delete_recording(name):
    session.query(RecordingModel).filter(RecordingModel.name == name).delete()
    session.commit()

def delete_all_recordings(*, confirm=False):
    if not confirm:
        raise RuntimeError("you must pass confirm=True")
    session.query(RecordingModel).delete()
    session.commit()
